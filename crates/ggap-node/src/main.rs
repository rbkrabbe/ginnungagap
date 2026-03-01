use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use openraft::BasicNode;
use serde::Deserialize;

use ggap_consensus::{
    build_raft_config, GgapLogStorage, GgapNetworkFactory, GgapRaft, GgapStateMachine,
    OpenRaftCluster, OpenRaftNode,
};
use ggap_storage::{
    fjall::{FjallStateMachine, FjallStore},
    ttl::TtlGcTask,
};
use ggap_server::{serve_client, serve_cluster};

#[derive(clap::Parser, Debug)]
#[command(name = "ggap-node", about = "Ginnungagap KV node")]
struct Cli {
    #[arg(long)]
    node_id: u64,
    #[arg(long, default_value = "0.0.0.0:17000")]
    client_addr: String,
    #[arg(long, default_value = "0.0.0.0:17001")]
    cluster_addr: String,
    /// Peer specs: "id=addr" format, repeatable
    #[arg(long = "peer")]
    peers: Vec<String>,
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    #[arg(long, default_value = "/var/lib/ginnungagap")]
    data_dir: std::path::PathBuf,
}

#[derive(Debug, Deserialize)]
struct StorageConfig {
    data_dir: String,
    max_key_bytes: usize,
    max_value_bytes: usize,
    max_history_versions: usize,
    ttl_gc_interval_secs: u64,
}

#[derive(Debug, Deserialize)]
struct RaftConfig {
    heartbeat_interval_ms: u64,
    election_timeout_min_ms: u64,
    election_timeout_max_ms: u64,
    snapshot_threshold: u64,
}

#[derive(Debug, Deserialize)]
struct ConsistencyConfig {
    default_read_mode: String,
    default_write_quorum: String,
    lease_enabled: bool,
    lease_duration_ms: u64,
}

#[derive(Debug, Deserialize)]
struct ServerConfig {
    watch_broadcast_capacity: usize,
    request_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
struct ObservabilityConfig {
    log_level: String,
    log_format: String,
    metrics_addr: String,
    tracing_otlp_endpoint: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    storage: StorageConfig,
    raft: RaftConfig,
    consistency: ConsistencyConfig,
    server: ServerConfig,
    observability: ObservabilityConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;
    let cli = Cli::parse();

    let mut figment = Figment::new()
        .merge(Toml::string(include_str!("../../../config/default.toml")));

    if let Some(ref config_path) = cli.config {
        figment = figment.merge(Toml::file_exact(config_path));
    }

    let config: Config = figment
        .merge(Env::prefixed("GINNUNGAGAP_").split("__"))
        .extract()
        .context("failed to load configuration")?;

    let log_format = config.observability.log_format.as_str();
    match log_format {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(&config.observability.log_level)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .pretty()
                .with_env_filter(&config.observability.log_level)
                .init();
        }
    }

    tracing::info!(
        node_id = cli.node_id,
        client_addr = %cli.client_addr,
        cluster_addr = %cli.cluster_addr,
        "node starting"
    );

    let client_addr: SocketAddr = cli
        .client_addr
        .parse()
        .with_context(|| format!("invalid client_addr: {}", cli.client_addr))?;
    let cluster_addr: SocketAddr = cli
        .cluster_addr
        .parse()
        .with_context(|| format!("invalid cluster_addr: {}", cli.cluster_addr))?;

    // Use data_dir from CLI if provided (non-default), else fall back to config.
    let data_dir = if cli.data_dir == std::path::Path::new("/var/lib/ginnungagap") {
        std::path::PathBuf::from(&config.storage.data_dir)
    } else {
        cli.data_dir.clone()
    };
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("cannot create data dir: {}", data_dir.display()))?;

    // 1. Open storage.
    let store = FjallStore::open(&data_dir)
        .with_context(|| format!("failed to open FjallStore at {}", data_dir.display()))?;

    // 2. Create FSM and log store.
    let fsm = Arc::new(FjallStateMachine::new(store.clone()));
    let log_store = GgapLogStorage::new(store.clone(), 0);
    let sm = GgapStateMachine::new(fsm.clone(), store.clone(), 0);

    // 3. Build Raft config from file config.
    let raft_cfg = build_raft_config(
        config.raft.heartbeat_interval_ms,
        config.raft.election_timeout_min_ms,
        config.raft.election_timeout_max_ms,
    );

    // 4. Create the Raft instance.
    let net = GgapNetworkFactory;
    let raft = Arc::new(
        GgapRaft::new(cli.node_id, raft_cfg, net, log_store, sm)
            .await
            .context("failed to create Raft instance")?,
    );

    // 5. Initialize as single-node cluster on first boot.
    if !raft.is_initialized().await.context("raft.is_initialized failed")? {
        let mut members = BTreeMap::new();
        members.insert(
            cli.node_id,
            BasicNode { addr: cluster_addr.to_string() },
        );
        raft.initialize(members)
            .await
            .map_err(|e| anyhow::anyhow!("raft.initialize failed: {}", e))?;
    }

    // 6. Build the node and cluster handles.
    let node = Arc::new(OpenRaftNode::new(raft.clone(), fsm.clone(), 0, cli.node_id));
    let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));

    // 7. Spawn TTL GC task, wired through Raft.
    let (ttl_tx, mut ttl_rx) = tokio::sync::mpsc::channel(256);
    tokio::spawn(TtlGcTask::new(fsm.clone(), 0, ttl_tx).run());
    let raft2 = raft.clone();
    tokio::spawn(async move {
        while let Some(cmd) = ttl_rx.recv().await {
            let _ = raft2.client_write(cmd).await;
        }
    });

    // 8. Serve.
    tokio::try_join!(
        serve_client(client_addr, node, cli.node_id),
        serve_cluster(cluster_addr, cluster),
    )?;

    Ok(())
}
