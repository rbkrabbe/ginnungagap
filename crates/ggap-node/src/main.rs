use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::Deserialize;

use ggap_consensus::{
    build_raft_config, run_split_handler, GgapLogStorage, GgapNetworkFactory, GgapNode, GgapRaft,
    GgapStateMachine, GossipTask, OpenRaftCluster, OpenRaftNode, RaftMetricsTask, ShardRegistry,
    ShardRouter, SplitCoordinator, SplitCoordinatorConfig,
};
use ggap_server::{serve_client, serve_cluster, ClusterServiceConfig, KvServiceConfig};
use tokio_util::sync::CancellationToken;

use ggap_storage::{
    fjall::{FjallLogStorage, FjallStateMachine, FjallStore},
    keys::meta_key,
    ttl::TtlGcTask,
    BootCounter, DirectoryStore, ShardMap,
};
use ggap_types::{DomainWatchEvent, NodeAddrs};

mod observability;

#[derive(clap::Parser, Debug)]
#[command(name = "ggap-node", about = "Ginnungagap KV node")]
struct Cli {
    #[arg(long)]
    node_id: u64,
    #[arg(long, default_value = "0.0.0.0:17000")]
    client_addr: String,
    /// Address to bind the client gRPC listener to. Defaults to `--client-addr`
    /// when omitted. Set this to `0.0.0.0:<port>` when `--client-addr` is a DNS
    /// hostname that should only be used for advertisement to other nodes.
    #[arg(long)]
    client_listen_addr: Option<String>,
    #[arg(long, default_value = "0.0.0.0:17001")]
    cluster_addr: String,
    /// Address to bind the cluster gRPC listener to. Defaults to `--cluster-addr`
    /// when omitted. Set this to `0.0.0.0:<port>` when `--cluster-addr` is a DNS
    /// hostname that should only be used for Raft peer advertisement.
    #[arg(long)]
    cluster_listen_addr: Option<String>,
    /// Initialize shard 0 as a fresh single-voter Raft cluster on first boot.
    /// Exactly one node in a fresh deployment should run with this flag; other
    /// nodes start uninitialized and wait for `AdminService.AddLearner` +
    /// `AdminService.ChangeMembership` from the seed.
    #[arg(long)]
    seed: bool,
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    #[arg(long, default_value = "/var/lib/ginnungagap")]
    data_dir: std::path::PathBuf,
    /// Prometheus scrape endpoint. Overrides `[observability].metrics_addr`.
    /// An empty string disables the endpoint entirely.
    #[arg(long)]
    metrics_addr: Option<String>,
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
    #[serde(default)]
    cors_allowed_origins: Vec<String>,
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

    let trace_guard =
        observability::init_tracing(&config.observability, cli.node_id).context("init tracing")?;

    tracing::info!(
        node_id = cli.node_id,
        client_addr = %cli.client_addr,
        cluster_addr = %cli.cluster_addr,
        "node starting"
    );

    // Initialize Prometheus metrics. Empty string disables; CLI overrides config.
    let shutdown = CancellationToken::new();
    let raw_metrics_addr = cli
        .metrics_addr
        .clone()
        .unwrap_or_else(|| config.observability.metrics_addr.clone());
    let metrics_addr: Option<SocketAddr> = if raw_metrics_addr.trim().is_empty() {
        None
    } else {
        Some(
            raw_metrics_addr
                .parse()
                .with_context(|| format!("invalid metrics_addr: {raw_metrics_addr}"))?,
        )
    };
    // `_metrics_handle` retains the PrometheusHandle for the lifetime of the
    // process. Nothing consumes it yet; the Overview screen and a metrics-read
    // transport will use it in a follow-up.
    let (_metrics_handle, _metrics_task) =
        observability::init_metrics_recorder(metrics_addr, shutdown.clone())?;

    let client_addr: SocketAddr = match &cli.client_listen_addr {
        Some(listen) => listen
            .parse()
            .with_context(|| format!("invalid client_listen_addr: {listen}"))?,
        None => tokio::net::lookup_host(&cli.client_addr)
            .await
            .with_context(|| format!("cannot resolve client_addr: {}", cli.client_addr))?
            .next()
            .with_context(|| format!("no addresses for client_addr: {}", cli.client_addr))?,
    };
    let cluster_addr: SocketAddr = match &cli.cluster_listen_addr {
        Some(listen) => listen
            .parse()
            .with_context(|| format!("invalid cluster_listen_addr: {listen}"))?,
        None => tokio::net::lookup_host(&cli.cluster_addr)
            .await
            .with_context(|| format!("cannot resolve cluster_addr: {}", cli.cluster_addr))?
            .next()
            .with_context(|| format!("no addresses for cluster_addr: {}", cli.cluster_addr))?,
    };

    // `--client-addr` is what other nodes dial to forward client requests here;
    // `--client-listen-addr` is where the listener binds. Needed before the
    // registry is built below, because it is this node's own directory
    // descriptor that carries both addresses to its peers.
    let self_client_addr = cli.client_addr.clone();
    let self_addrs = NodeAddrs::new(cli.cluster_addr.clone(), self_client_addr.clone());

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

    // 2b. Take this boot's incarnation. It ranks the descriptor this node
    //     publishes for itself, so a restart at a new address outranks every
    //     copy of the old one still in flight — including the copies peers hold
    //     of this node's own entry. Wiping the data dir restarts the count, so
    //     an address change made across a wipe needs a fresh node id.
    //
    //     This is the one piece of startup state whose loss is fatal: a node
    //     that publishes below the rank its peers hold cannot win back
    //     authorship of its own address and re-emits the stale one, so failing
    //     here beats starting unrankable. A rank that cannot be *persisted* is
    //     fatal for the same reason one step out: the next boot would reissue
    //     it, and an equal rank keeps the entry already held, so an address
    //     changed across those two boots would never install.
    let self_incarnation = BootCounter::new(store.clone(), cli.node_id)
        .advance()
        .context("cannot establish this node's incarnation")?;
    tracing::info!(
        node_id = cli.node_id,
        incarnation = self_incarnation,
        "publishing this node's addresses at this incarnation"
    );

    // 3. Create watch broadcast channel and FSM (shared across all shards).
    let (watch_tx, _watch_rx) =
        tokio::sync::broadcast::channel::<DomainWatchEvent>(config.server.watch_broadcast_capacity);

    // Create split-event channel: FjallStateMachine sends SplitApplied events when
    // a KvCommand::Split is applied; run_split_handler receives them and bootstraps
    // the new shard's Raft group on this node.
    let (split_tx, split_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut fsm_builder = FjallStateMachine::new(store.clone()).with_watch(watch_tx.clone());
    fsm_builder.set_split_sender(split_tx);
    fsm_builder.set_shard_map(shard_map.clone());
    let fsm = Arc::new(fsm_builder);

    // 4. Create ShardRouter.
    let router = Arc::new(ShardRouter::new(shard_map.clone()));

    // 4b. Cluster-wide shard registry. It has to exist before any Raft group,
    //     because `GgapNetwork` resolves every outbound RPC's target address
    //     through its directory. No bootstrap seeds: a node joins by being added
    //     to a shard's membership, which is exactly what tells it about the
    //     cluster.
    //
    //     The directory is cached in the `node` keyspace and read back here, so
    //     a node that restarts and is elected before any peer has gossiped to it
    //     resolves its peers straight away instead of failing sends until it is
    //     dialled. It is a cache of gossip: a missing or corrupt record starts
    //     the node with an empty directory rather than failing the boot.
    //
    //     The registry keeps the store and does the writing — the gossip task
    //     asks it to after each round, and `retire` writes a tombstone through
    //     immediately, which is the one case that cannot wait for a round.
    let directory_store = DirectoryStore::new(store.clone());
    let persisted_directory = directory_store.load();
    let registry =
        Arc::new(ShardRegistry::new(cli.node_id, []).with_directory_store(directory_store));
    tracing::info!(
        node_id = cli.node_id,
        entries = persisted_directory.len(),
        "restored the persisted directory"
    );
    // A node whose own entry is a tombstone was retired. Nothing it publishes
    // can outrank that, so it has no useful work left: it is in no membership,
    // resolvable by nobody, and dialled by nobody. It comes up *quiesced*
    // rather than refusing to start — refusing would be a non-zero exit, and
    // under a supervisor that only permits `restartPolicy: Always` (a
    // StatefulSet pod) that is a crash loop for as long as the pod exists,
    // which scaling down cannot remove without taking healthy nodes with it.
    //
    // Quiesced is the same state `RemoveNode` leaves a running node in, by the
    // same token: gossip keeps serving so the tombstone keeps spreading, and
    // every other service refuses. Reaching it here as well is what makes a
    // reschedule — drain, OOM, node failure — harmless rather than a loop.
    let retired = CancellationToken::new();
    if persisted_directory
        .iter()
        .any(|(id, entry)| *id == cli.node_id && entry.is_removed())
    {
        tracing::warn!(
            node_id = cli.node_id,
            "this node was retired from the cluster; starting quiesced — gossip only, \
             no Raft, no KV, no admin. A retired id is never reused: to put this host \
             back in service, start it under a fresh --node-id."
        );
        retired.cancel();
    }
    // Registered either way, so the series exists on a healthy node and an
    // operator can tell a quiesced pod from a serving one by scraping it. The
    // `raft_*` gauges cannot answer that: retirement stops the task that
    // publishes them, so they freeze at their last values and a retired node
    // scrapes identically to an idle healthy one.
    metrics::gauge!("ggap_node_retired").set(if retired.is_cancelled() { 1.0 } else { 0.0 });
    registry.merge_directory(persisted_directory).await;

    // Everything a retired node has no use for hangs off this: each shard's Raft
    // group, its TTL GC task and its metrics task. A child of `shutdown`, so a
    // real shutdown stops them too — while cancelling it on retirement leaves
    // the gossip task, which hangs off `shutdown` directly, still running.
    let shard_tasks = shutdown.child_token();

    // 5. Start a Raft group for each shard in the ShardMap.
    let shards = shard_map.all_shards().await;
    // Kept so retirement can stop them; a quiesced node holds no Raft group.
    let mut rafts: Vec<(u64, Arc<GgapRaft>)> = Vec::new();
    let raft_cfg = build_raft_config(
        config.raft.heartbeat_interval_ms,
        config.raft.election_timeout_min_ms,
        config.raft.election_timeout_max_ms,
        config.raft.snapshot_threshold,
    );

    // A node that was already retired when it started skips this entirely: it
    // is in no membership, so every group would campaign against nobody and
    // hold storage open for nothing, and the gate on the Raft service refuses
    // the peers that have not yet heard. A node retired *while* running took
    // this path already — the retire handler below stops what it started.
    for shard_info in shards.iter().filter(|_| !retired.is_cancelled()) {
        let shard_id = shard_info.shard_id;
        let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), shard_id);
        let sm = GgapStateMachine::new(fsm.clone(), shard_id);
        let net = GgapNetworkFactory::new(shard_id, registry.clone());

        let raft = Arc::new(
            GgapRaft::new(cli.node_id, raft_cfg.clone(), net, log_store, sm)
                .await
                .with_context(|| format!("failed to create Raft for shard {shard_id}"))?,
        );

        // Initialize Raft membership on first boot. Three cases:
        //   1. `bootstrap_members` meta key present — split-created shard. Always
        //      initialize from the persisted membership (written atomically with
        //      the split data movement; a restart must not re-init as single-node).
        //   2. No `bootstrap_members` and `--seed` was passed — fresh seed node.
        //      Initialize shard 0 as a single-voter cluster; other nodes join
        //      later via AdminService.AddLearner/ChangeMembership.
        //   3. No `bootstrap_members` and no `--seed` — non-seed fresh node.
        //      Leave Raft uninitialized; it will pick up membership when the
        //      seed sends it AppendEntries after AddLearner.
        if !raft
            .is_initialized()
            .await
            .with_context(|| format!("raft.is_initialized failed for shard {shard_id}"))?
        {
            let bootstrap_key = meta_key(shard_id, "bootstrap_members");
            let members: Option<BTreeMap<u64, GgapNode>> = match store.meta.get(&bootstrap_key) {
                Ok(Some(bytes)) => {
                    let (ids, _): (BTreeSet<u64>, _) =
                        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                            .with_context(|| {
                                format!("failed to decode bootstrap_members for shard {shard_id}")
                            })?;
                    // Ids alone. Each resolves through the directory restored
                    // above, or through the first peer to gossip to this node.
                    Some(ids.into_iter().map(|id| (id, GgapNode {})).collect())
                }
                Ok(None) if cli.seed => Some(BTreeMap::from([(cli.node_id, GgapNode {})])),
                Ok(None) => None,
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to read bootstrap_members for shard {shard_id}: {e}"
                    ));
                }
            };
            match members {
                Some(members) => {
                    raft.initialize(members).await.map_err(|e| {
                        anyhow::anyhow!("raft.initialize failed for shard {shard_id}: {e}")
                    })?;
                }
                None => {
                    tracing::info!(
                        shard_id,
                        node_id = cli.node_id,
                        "non-seed fresh node: Raft uninitialized, waiting for AddLearner"
                    );
                }
            }
        }

        let node = Arc::new(OpenRaftNode::new(
            raft.clone(),
            fsm.clone(),
            shard_id,
            cli.node_id,
            registry.clone(),
            tokio::time::Duration::from_millis(config.consistency.lease_duration_ms),
        ));
        let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));

        router.add_shard(shard_id, node, cluster).await;

        // Spawn TTL GC task for this shard.
        let (ttl_tx, mut ttl_rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(
            TtlGcTask::new(fsm.clone(), shard_id, ttl_tx, shard_tasks.child_token())
                .with_watch(watch_tx.clone())
                .run(),
        );
        let raft2 = raft.clone();
        tokio::spawn(async move {
            while let Some(cmd) = ttl_rx.recv().await {
                if let Err(e) = raft2.client_write(cmd).await {
                    tracing::warn!(shard_id, error = %e, "TTL GC delete failed via Raft");
                }
            }
        });

        tokio::spawn(RaftMetricsTask::new(raft.clone(), shard_id, shard_tasks.child_token()).run());
        rafts.push((shard_id, raft.clone()));

        tracing::info!(shard_id, "started Raft group");
    }

    // 6. Spawn the background split handler.
    //    Receives SplitApplied events from FjallStateMachine::apply() and
    //    bootstraps the new shard's Raft group + registers it in the router.
    tokio::spawn(run_split_handler(
        split_rx,
        store.clone(),
        fsm.clone(),
        router.clone(),
        cli.node_id,
        raft_cfg.clone(),
        registry.clone(),
    ));

    // 7. Create the SplitCoordinator.
    let split_coordinator = Arc::new(SplitCoordinator::new(SplitCoordinatorConfig {
        router: router.clone(),
        shard_map: shard_map.clone(),
        registry: registry.clone(),
    }));

    // 7b. Gossip task. This node publishes its own addresses into the directory
    //     each tick and carries the whole view to peers, so every node learns
    //     every other node's address from its author and any node can report
    //     consensus state for shards it does not host locally.
    tokio::spawn(
        GossipTask::new(
            router.clone(),
            registry.clone(),
            cli.node_id,
            self_addrs.clone(),
            self_incarnation,
            shutdown.child_token(),
        )
        .run(),
    );

    // 8. Serve with graceful shutdown on SIGINT / SIGTERM.
    let kv_config = KvServiceConfig {
        max_key_bytes: config.storage.max_key_bytes,
        max_value_bytes: config.storage.max_value_bytes,
        watch_tx: Some(watch_tx),
        watch_output_buffer: 128,
        retired: Some(retired.clone()),
    };

    // `retired` is cancelled either above, when the persisted directory already
    // held this node's tombstone, or by AdminService.RemoveNode once the node
    // has tombstoned itself and handed that over. Both reach the same state and
    // the process stays up: the gate on KV, Admin and Raft refuses everything,
    // while gossip keeps running so the tombstone reaches peers that were
    // unreachable when it was written.
    //
    // It does not exit. Under a supervisor an exit is a restart, and the restart
    // would read the tombstone and quiesce anyway — so exiting buys nothing and
    // costs a pod flap. An operator removes the host when convenient; until
    // then it answers UNAVAILABLE rather than refusing connections, which is
    // easier to diagnose than a closed port.
    tokio::spawn({
        let retired = retired.clone();
        let shard_tasks = shard_tasks.clone();
        let router = router.clone();
        async move {
            retired.cancelled().await;

            // Stop serving a cluster this node is no longer part of. The gate
            // already refuses the RPCs; this stops the work nobody asked for —
            // TTL GC proposing deletes into a membership it left, the metrics
            // tasks, and the Raft groups themselves. Dropping the shards from
            // the router also stops `refresh_local` publishing `ShardEntry`
            // for them: its version rises every tick and wins at peers on
            // version alone, so a retired node left publishing would keep
            // overwriting the live view of a shard it no longer hosts.
            metrics::gauge!("ggap_node_retired").set(1.0);
            shard_tasks.cancel();
            for (shard_id, raft) in rafts {
                router.remove_shard(shard_id).await;
                if let Err(e) = raft.shutdown().await {
                    tracing::warn!(shard_id, error = %e, "raft shutdown on retirement");
                }
            }

            tracing::warn!(
                "retired from the cluster; serving gossip only, so the tombstone \
                 reaches peers that have not heard. This node will not rejoin: a \
                 retired id is never reused."
            );
        }
    });

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
        serve_client(
            client_addr,
            router.clone(),
            cli.node_id,
            kv_config,
            config.server.cors_allowed_origins.clone(),
        ),
        serve_cluster(
            cluster_addr,
            router,
            split_coordinator,
            shard_map,
            registry,
            ClusterServiceConfig {
                cors_origins: config.server.cors_allowed_origins,
                retired: Some(retired),
            },
        ),
    )?;

    trace_guard.shutdown();
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
