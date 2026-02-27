use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::Deserialize;

use ggap_consensus::StubRaftNode;
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

    let stub = Arc::new(StubRaftNode::new());

    tokio::try_join!(
        serve_client(client_addr, stub, cli.node_id),
        serve_cluster(cluster_addr),
    )?;

    Ok(())
}
