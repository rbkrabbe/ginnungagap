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
    OpenRaftCluster, OpenRaftNode, ShardRouter, SplitCoordinator, SplitCoordinatorConfig,
};
use ggap_server::{serve_client, serve_cluster, KvServiceConfig};
use tokio_util::sync::CancellationToken;

use ggap_storage::{
    fjall::{FjallStateMachine, FjallStore},
    ttl::TtlGcTask,
    ShardMap,
};

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
#[allow(dead_code)]
struct StorageConfig {
    data_dir: String,
    max_key_bytes: usize,
    max_value_bytes: usize,
    max_history_versions: usize,
    ttl_gc_interval_secs: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct RaftConfig {
    heartbeat_interval_ms: u64,
    election_timeout_min_ms: u64,
    election_timeout_max_ms: u64,
    snapshot_threshold: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ConsistencyConfig {
    default_read_mode: String,
    default_write_quorum: String,
    lease_enabled: bool,
    lease_duration_ms: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ServerConfig {
    watch_broadcast_capacity: usize,
    request_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ObservabilityConfig {
    log_level: String,
    log_format: String,
    metrics_addr: String,
    tracing_otlp_endpoint: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
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

    let mut figment =
        Figment::new().merge(Toml::string(include_str!("../../../config/default.toml")));

    if let Some(ref config_path) = cli.config {
        figment = figment.merge(Toml::file_exact(config_path));
    }

    let config: Config = figment
        .merge(Env::prefixed("GINNUNGAGAP_").split("__"))
        .extract()
        .context("failed to load configuration")?;

    validate_config(&config)?;

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

    // 2. Load or initialize ShardMap.
    let shard_map = Arc::new(ShardMap::load(store.clone()).context("failed to load shard map")?);
    shard_map
        .initialize_default()
        .await
        .context("failed to initialize default shard")?;

    // 3. Create FSM (shared across all shards).
    let fsm = Arc::new(FjallStateMachine::new(store.clone()));

    // 4. Create ShardRouter.
    let router = Arc::new(ShardRouter::new(shard_map.clone()));

    // 5. Start a Raft group for each shard in the ShardMap.
    let shutdown = CancellationToken::new();
    let shards = shard_map.all_shards().await;
    let raft_cfg = build_raft_config(
        config.raft.heartbeat_interval_ms,
        config.raft.election_timeout_min_ms,
        config.raft.election_timeout_max_ms,
    );

    for shard_info in &shards {
        let shard_id = shard_info.shard_id;
        let log_store = GgapLogStorage::new(store.clone(), shard_id);
        let sm = GgapStateMachine::new(fsm.clone(), shard_id);
        let net = GgapNetworkFactory::new(shard_id);

        let raft = Arc::new(
            GgapRaft::new(cli.node_id, raft_cfg.clone(), net, log_store, sm)
                .await
                .with_context(|| format!("failed to create Raft for shard {shard_id}"))?,
        );

        // Initialize as single-node cluster on first boot (only for shard 0 initially).
        if !raft
            .is_initialized()
            .await
            .with_context(|| format!("raft.is_initialized failed for shard {shard_id}"))?
        {
            let mut members = BTreeMap::new();
            members.insert(
                cli.node_id,
                BasicNode {
                    addr: cluster_addr.to_string(),
                },
            );
            raft.initialize(members)
                .await
                .map_err(|e| anyhow::anyhow!("raft.initialize failed for shard {shard_id}: {e}"))?;
        }

        let node = Arc::new(OpenRaftNode::new(
            raft.clone(),
            fsm.clone(),
            shard_id,
            cli.node_id,
        ));
        let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));

        router.add_shard(shard_id, node, cluster).await;

        // Spawn TTL GC task for this shard.
        let (ttl_tx, mut ttl_rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(TtlGcTask::new(fsm.clone(), shard_id, ttl_tx, shutdown.child_token()).run());
        let raft2 = raft.clone();
        tokio::spawn(async move {
            while let Some(cmd) = ttl_rx.recv().await {
                if let Err(e) = raft2.client_write(cmd).await {
                    tracing::warn!(shard_id, error = %e, "TTL GC delete failed via Raft");
                }
            }
        });

        tracing::info!(shard_id, "started Raft group");
    }

    // 6. Create the SplitCoordinator.
    let split_coordinator = Arc::new(SplitCoordinator::new(SplitCoordinatorConfig {
        router: router.clone(),
        shard_map: shard_map.clone(),
        store: store.clone(),
        fsm: fsm.clone(),
        node_id: cli.node_id,
        cluster_addr: cluster_addr.to_string(),
        heartbeat_ms: config.raft.heartbeat_interval_ms,
        election_min_ms: config.raft.election_timeout_min_ms,
        election_max_ms: config.raft.election_timeout_max_ms,
    }));

    // 7. Serve with graceful shutdown on SIGINT / SIGTERM.
    let kv_config = KvServiceConfig {
        max_key_bytes: config.storage.max_key_bytes,
        max_value_bytes: config.storage.max_value_bytes,
    };

    let shutdown_trigger = shutdown.clone();
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("register SIGTERM");
            tokio::select! {
                _ = ctrl_c => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
        }
        tracing::info!("shutdown signal received, draining...");
        shutdown_trigger.cancel();
    });

    tokio::try_join!(
        serve_client(client_addr, router.clone(), cli.node_id, kv_config),
        serve_cluster(cluster_addr, router, split_coordinator, shard_map),
    )?;

    Ok(())
}

fn validate_config(config: &Config) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.storage.max_key_bytes > 0,
        "max_key_bytes must be > 0"
    );
    anyhow::ensure!(
        config.storage.max_value_bytes > 0,
        "max_value_bytes must be > 0"
    );
    anyhow::ensure!(
        config.raft.heartbeat_interval_ms < config.raft.election_timeout_min_ms,
        "heartbeat_interval_ms ({}) must be < election_timeout_min_ms ({})",
        config.raft.heartbeat_interval_ms,
        config.raft.election_timeout_min_ms
    );
    anyhow::ensure!(
        config.raft.election_timeout_min_ms < config.raft.election_timeout_max_ms,
        "election_timeout_min_ms ({}) must be < election_timeout_max_ms ({})",
        config.raft.election_timeout_min_ms,
        config.raft.election_timeout_max_ms
    );
    if config.consistency.lease_enabled {
        anyhow::ensure!(
            config.consistency.lease_duration_ms < config.raft.election_timeout_min_ms,
            "lease_duration_ms ({}) must be < election_timeout_min_ms ({}) when leases are enabled",
            config.consistency.lease_duration_ms,
            config.raft.election_timeout_min_ms
        );
    }
    Ok(())
}
