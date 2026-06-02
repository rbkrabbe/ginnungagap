//! Background gossip task: periodic push-pull anti-entropy that keeps each
//! node's [`ShardRegistry`] converged on a cluster-wide view of shard placement
//! and Raft status.
//!
//! Mirrors the [`crate::metrics_task::RaftMetricsTask`] shape (Arc state +
//! `CancellationToken` + `pub async fn run(self)` with a `tokio::select!`
//! cancellation guard). All time uses `tokio::time` so the deterministic
//! simulation harness can pause/advance it.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use ggap_proto::v1::{
    gossip_service_client::GossipServiceClient, GossipNode, GossipShardEntry, GossipState,
};

use crate::registry::{ShardEntry, ShardRegistry};
use crate::router::ShardRouter;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_FANOUT: usize = 3;
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(2);

pub struct GossipTask {
    router: Arc<ShardRouter>,
    registry: Arc<ShardRegistry>,
    self_node_id: u64,
    self_cluster_addr: String,
    cancel: CancellationToken,
    interval: Duration,
    fanout: usize,
    rpc_timeout: Duration,
    /// Monotonic heartbeat counter bumped each time we publish local status.
    version: u64,
    /// Rotating cursor over the peer list (avoids a `rand` prod dependency).
    peer_cursor: usize,
}

impl GossipTask {
    pub fn new(
        router: Arc<ShardRouter>,
        registry: Arc<ShardRegistry>,
        self_node_id: u64,
        self_cluster_addr: String,
        cancel: CancellationToken,
    ) -> Self {
        GossipTask {
            router,
            registry,
            self_node_id,
            self_cluster_addr,
            cancel,
            interval: DEFAULT_INTERVAL,
            fanout: DEFAULT_FANOUT,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            version: 0,
            peer_cursor: 0,
        }
    }

    pub fn with_interval(mut self, d: Duration) -> Self {
        self.interval = d;
        self
    }

    pub fn with_fanout(mut self, n: usize) -> Self {
        self.fanout = n;
        self
    }

    pub fn with_rpc_timeout(mut self, d: Duration) -> Self {
        self.rpc_timeout = d;
        self
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    tracing::info!(node_id = self.self_node_id, "gossip task shutting down");
                    return;
                }
                _ = tokio::time::sleep(self.interval) => {}
            }
            self.refresh_local().await;
            self.exchange_round().await;
        }
    }

    /// Publish status for every locally-hosted shard into the registry and feed
    /// the node directory from each shard's Raft membership (transitive peer
    /// discovery).
    async fn refresh_local(&mut self) {
        let shard_ids = self.router.local_shard_ids().await;
        if shard_ids.is_empty() {
            return;
        }
        self.version += 1;

        // Range/state metadata comes from the shard map (same source the admin
        // service uses for the authoritative shard list).
        let shard_meta: HashMap<u64, (String, String, String)> = self
            .router
            .shard_map()
            .all_shards()
            .await
            .into_iter()
            .map(|s| {
                (
                    s.shard_id,
                    (s.range.start, s.range.end, format!("{:?}", s.state)),
                )
            })
            .collect();

        // Seed the directory with self.
        self.registry
            .merge_directory([(self.self_node_id, self.self_cluster_addr.clone())])
            .await;

        for shard_id in shard_ids {
            let Some(node) = self.router.get_node(shard_id).await else {
                continue;
            };
            let status = node.cluster_status();

            // Each membership pair carries the peer's cluster gRPC address.
            self.registry
                .merge_directory(
                    status
                        .voters
                        .iter()
                        .chain(status.learners.iter())
                        .map(|(id, addr)| (*id, addr.clone())),
                )
                .await;

            let (range_start, range_end, state) = shard_meta
                .get(&shard_id)
                .cloned()
                .unwrap_or_else(|| (String::new(), String::new(), String::new()));

            self.registry
                .upsert_local(ShardEntry {
                    shard_id,
                    range_start,
                    range_end,
                    state,
                    leader_id: status.leader_id,
                    voters: status.voters.iter().map(|(id, _)| *id).collect(),
                    learners: status.learners.iter().map(|(id, _)| *id).collect(),
                    term: status.term,
                    last_applied: status.last_applied,
                    version: self.version,
                    origin_node_id: self.self_node_id,
                    last_updated: Instant::now(),
                })
                .await;
        }
    }

    /// Contact up to `fanout` peers (round-robin), push our view and merge
    /// theirs. Unreachable peers are skipped — their entries simply age.
    async fn exchange_round(&mut self) {
        let peers = self.registry.peers_excluding_self().await;
        if peers.is_empty() {
            return;
        }

        let take = self.fanout.min(peers.len());
        for _ in 0..take {
            let (peer_id, addr) = peers[self.peer_cursor % peers.len()].clone();
            self.peer_cursor = self.peer_cursor.wrapping_add(1);

            if addr == self.self_cluster_addr {
                continue;
            }
            if let Err(e) = self.exchange_with(&addr).await {
                tracing::debug!(node_id = self.self_node_id, peer_id, %addr, error = %e, "gossip exchange failed");
            }
        }
    }

    async fn exchange_with(&self, addr: &str) -> Result<(), String> {
        let (dir, shards) = self.registry.snapshot_for_gossip().await;
        let req = GossipState {
            sender_node_id: self.self_node_id,
            directory: dir
                .into_iter()
                .map(|(node_id, cluster_addr)| GossipNode {
                    node_id,
                    cluster_addr,
                })
                .collect(),
            shards: shards.into_iter().map(entry_to_proto).collect(),
        };

        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| e.to_string())?;
        let channel = tokio::time::timeout(self.rpc_timeout, endpoint.connect())
            .await
            .map_err(|_| "connect timed out".to_string())?
            .map_err(|e| e.to_string())?;
        let mut client = GossipServiceClient::new(channel);

        let resp = tokio::time::timeout(self.rpc_timeout, client.exchange(req))
            .await
            .map_err(|_| "exchange timed out".to_string())?
            .map_err(|e| e.to_string())?
            .into_inner();

        merge_gossip_state(&self.registry, resp).await;
        Ok(())
    }
}

/// Merge a received [`GossipState`] (directory + shard entries) into a registry.
/// Shared by the gossip task (response side) and the gossip service (request
/// side).
pub async fn merge_gossip_state(registry: &ShardRegistry, state: GossipState) {
    registry
        .merge_directory(
            state
                .directory
                .into_iter()
                .map(|n| (n.node_id, n.cluster_addr)),
        )
        .await;
    for e in state.shards {
        registry.merge_entry(entry_from_proto(e)).await;
    }
}

pub fn entry_to_proto(e: ShardEntry) -> GossipShardEntry {
    GossipShardEntry {
        shard_id: e.shard_id,
        range_start: e.range_start,
        range_end: e.range_end,
        state: e.state,
        leader_id: e.leader_id,
        voters: e.voters,
        learners: e.learners,
        term: e.term,
        last_applied: e.last_applied,
        version: e.version,
        origin_node_id: e.origin_node_id,
    }
}

pub fn entry_from_proto(e: GossipShardEntry) -> ShardEntry {
    ShardEntry {
        shard_id: e.shard_id,
        range_start: e.range_start,
        range_end: e.range_end,
        state: e.state,
        leader_id: e.leader_id,
        voters: e.voters,
        learners: e.learners,
        term: e.term,
        last_applied: e.last_applied,
        version: e.version,
        origin_node_id: e.origin_node_id,
        // Overwritten by merge_entry to "now"; placeholder here.
        last_updated: Instant::now(),
    }
}
