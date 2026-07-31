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

use ggap_types::NodeAddrs;

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
    /// This node's advertised client address (`--client-addr`). An empty string
    /// is gossiped as "unknown" and merged away, never dialled.
    self_client_addr: String,
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
        self_client_addr: String,
        cancel: CancellationToken,
    ) -> Self {
        GossipTask {
            router,
            registry,
            self_node_id,
            self_cluster_addr,
            self_client_addr,
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

        // Seed the directory with self. This is the only place either address is
        // originated: every other node learns both by gossiping with us.
        self.registry
            .merge_directory([(
                self.self_node_id,
                NodeAddrs::new(
                    self.self_cluster_addr.clone(),
                    self.self_client_addr.clone(),
                ),
            )])
            .await;

        for shard_id in shard_ids {
            let Some(node) = self.router.get_node(shard_id).await else {
                continue;
            };
            let status = node.cluster_status();

            // Membership carries both addresses. `merge_directory` merges
            // field-wise, so a split-created shard's cluster-only
            // `bootstrap_members` (tk-10b7) cannot blank a client address.
            self.registry
                .merge_directory(
                    status
                        .voters
                        .iter()
                        .chain(status.learners.iter())
                        .map(|(id, addrs)| (*id, addrs.clone())),
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
            directory: dir.into_iter().map(node_to_proto).collect(),
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
        .merge_directory(state.directory.into_iter().map(node_from_proto))
        .await;
    for e in state.shards {
        registry.merge_entry(entry_from_proto(e)).await;
    }
}

pub fn node_to_proto((node_id, addrs): (u64, NodeAddrs)) -> GossipNode {
    GossipNode {
        node_id,
        cluster_addr: addrs.cluster_addr,
        client_addr: addrs.client_addr,
    }
}

/// A peer that predates `client_addr` sends the field as an empty string (prost
/// decodes an absent proto3 scalar to its default), which `merge_directory`
/// treats as "unknown" and skips.
pub fn node_from_proto(n: GossipNode) -> (u64, NodeAddrs) {
    (
        n.node_id,
        NodeAddrs {
            cluster_addr: n.cluster_addr,
            client_addr: n.client_addr,
        },
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_round_trip_preserves_both_addrs() {
        let addrs = NodeAddrs::new("host:17001", "host:17000");
        let (id, back) = node_from_proto(node_to_proto((7, addrs.clone())));
        assert_eq!(id, 7);
        assert_eq!(back, addrs);
    }

    /// An old peer sends no client_addr; prost decodes the absent field to "".
    #[test]
    fn proto_round_trip_tolerates_absent_client_addr() {
        let (id, back) = node_from_proto(GossipNode {
            node_id: 7,
            cluster_addr: "host:17001".into(),
            client_addr: String::new(),
        });
        assert_eq!(id, 7);
        assert_eq!(back, NodeAddrs::cluster_only("host:17001"));
    }
}
