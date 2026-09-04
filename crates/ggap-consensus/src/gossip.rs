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

use ggap_storage::DirectoryStore;
use ggap_types::{DirectoryEntry, NodeAddrs, NodeDescriptor};

use crate::registry::{ShardEntry, ShardRegistry};
use crate::router::ShardRouter;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_FANOUT: usize = 3;
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(2);

pub struct GossipTask {
    router: Arc<ShardRouter>,
    registry: Arc<ShardRegistry>,
    self_node_id: u64,
    /// This node's own addresses, published into the directory each tick.
    self_addrs: NodeAddrs,
    /// Orders this node's publications. Boot-scoped and >= 1, so a descriptor
    /// written on this node's behalf at incarnation 0 is superseded on the
    /// first tick.
    incarnation: u64,
    cancel: CancellationToken,
    interval: Duration,
    fanout: usize,
    rpc_timeout: Duration,
    /// Where the directory is cached for the next boot. `None` disables
    /// persistence — a registry built by hand in a test needs none.
    directory_store: Option<DirectoryStore>,
    /// What was last written, so an unchanged directory costs no write.
    persisted_directory: Vec<(u64, DirectoryEntry)>,
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
        self_addrs: NodeAddrs,
        incarnation: u64,
        cancel: CancellationToken,
    ) -> Self {
        GossipTask {
            router,
            registry,
            self_node_id,
            self_addrs,
            incarnation,
            cancel,
            interval: DEFAULT_INTERVAL,
            fanout: DEFAULT_FANOUT,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            directory_store: None,
            persisted_directory: Vec::new(),
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

    /// Cache the directory to disk after every round, so the next boot resolves
    /// peers without waiting to be dialled. The caller is expected to have
    /// seeded the registry from the same store before starting the task.
    pub fn with_directory_store(mut self, store: DirectoryStore) -> Self {
        self.directory_store = Some(store);
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
            self.persist_directory().await;
        }
    }

    /// Publish this node's own descriptor, then status for every locally-hosted
    /// shard.
    ///
    /// Self-publication is not gated on hosting a shard: a node is the sole
    /// author of its own addresses, so it must say where it is even while it
    /// hosts nothing — after a drain, or before placement. Every other node's
    /// address arrives the same way, as its own descriptor carried here by
    /// gossip; membership holds ids and cannot supply one.
    async fn refresh_local(&mut self) {
        self.registry
            .merge_directory([(
                self.self_node_id,
                NodeDescriptor::new(self.self_addrs.clone(), self.incarnation),
            )])
            .await;

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

        for shard_id in shard_ids {
            let Some(node) = self.router.get_node(shard_id).await else {
                continue;
            };
            let status = node.cluster_status();

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
                    voters: status.voters,
                    learners: status.learners,
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

            if addr == self.self_addrs.cluster_addr {
                continue;
            }
            if let Err(e) = self.exchange_with(&addr).await {
                tracing::debug!(node_id = self.self_node_id, peer_id, %addr, error = %e, "gossip exchange failed");
            }
        }
    }

    /// Write the directory out if it changed since the last round. Failing to
    /// write is not fatal: the cache only buys immediacy on the next boot, and
    /// a node that starts without one is re-seeded by the first peer to dial
    /// it. Persisting after the exchange captures what peers just told us as
    /// well as what we published.
    async fn persist_directory(&mut self) {
        let Some(store) = &self.directory_store else {
            return;
        };
        let dir = self.registry.directory_snapshot().await;
        if dir == self.persisted_directory {
            return;
        }
        match store.save(&dir) {
            Ok(()) => self.persisted_directory = dir,
            Err(e) => {
                tracing::warn!(node_id = self.self_node_id, error = %e, "cannot persist the directory");
            }
        }
    }

    async fn exchange_with(&self, addr: &str) -> Result<(), String> {
        exchange_once(&self.registry, self.self_node_id, addr, self.rpc_timeout).await
    }
}

/// One push-pull exchange with the peer at `addr`: send our whole view, merge
/// what comes back.
pub async fn exchange_once(
    registry: &ShardRegistry,
    self_node_id: u64,
    addr: &str,
    rpc_timeout: Duration,
) -> Result<(), String> {
    let req = gossip_request(registry, self_node_id, None).await;
    send_exchange(registry, addr, req, rpc_timeout).await
}

/// This node's view as a `GossipState`, optionally with one id tombstoned in
/// the copy that goes out. The override exists for a node retiring itself,
/// which must not write the tombstone locally until a peer has taken it.
async fn gossip_request(
    registry: &ShardRegistry,
    self_node_id: u64,
    tombstone: Option<u64>,
) -> GossipState {
    let (mut dir, shards) = registry.snapshot_for_gossip().await;
    if let Some(node_id) = tombstone {
        dir.retain(|(id, _)| *id != node_id);
        dir.push((node_id, DirectoryEntry::Removed));
        dir.sort_by_key(|(id, _)| *id);
    }
    GossipState {
        sender_node_id: self_node_id,
        directory: dir.into_iter().map(node_to_proto).collect(),
        shards: shards.into_iter().map(entry_to_proto).collect(),
    }
}

async fn send_exchange(
    registry: &ShardRegistry,
    addr: &str,
    req: GossipState,
    rpc_timeout: Duration,
) -> Result<(), String> {
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?;
    let channel = tokio::time::timeout(rpc_timeout, endpoint.connect())
        .await
        .map_err(|_| "connect timed out".to_string())?
        .map_err(|e| e.to_string())?;
    let mut client = GossipServiceClient::new(channel);

    let resp = tokio::time::timeout(rpc_timeout, client.exchange(req))
        .await
        .map_err(|_| "exchange timed out".to_string())?
        .map_err(|e| e.to_string())?
        .into_inner();

    merge_gossip_state(registry, resp).await;
    Ok(())
}

/// Hand a tombstone for `removed_id` to every known peer, returning how many
/// accepted it.
///
/// The gossip task's own rounds are round-robin over a fanout and happen on a
/// timer; this is for the one caller that can neither wait for the next tick nor
/// afford to miss it — a node retiring itself, which is about to stop. A
/// tombstone that reaches no peer dies with the node that wrote it.
///
/// The tombstone is added to the copy that goes out, not to the local
/// directory: the caller decides what to do when nothing gets through, and a
/// removal recorded locally can never be taken back.
pub async fn broadcast_removal(
    registry: &ShardRegistry,
    self_node_id: u64,
    removed_id: u64,
    rpc_timeout: Duration,
) -> usize {
    let req = gossip_request(registry, self_node_id, Some(removed_id)).await;
    let mut delivered = 0;
    for (peer_id, addr) in registry.peers_excluding_self().await {
        match send_exchange(registry, &addr, req.clone(), rpc_timeout).await {
            Ok(()) => delivered += 1,
            Err(e) => {
                tracing::debug!(node_id = self_node_id, peer_id, %addr, error = %e, "tombstone delivery failed")
            }
        }
    }
    delivered
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

pub fn node_to_proto((node_id, entry): (u64, DirectoryEntry)) -> GossipNode {
    match entry {
        DirectoryEntry::Live(desc) => GossipNode {
            node_id,
            cluster_addr: desc.addrs.cluster_addr,
            client_addr: desc.addrs.client_addr,
            incarnation: desc.incarnation,
            removed: false,
        },
        // A tombstone says only that the node is gone; its addresses are no
        // longer anyone's business, and carrying them would invite a receiver
        // to use them.
        DirectoryEntry::Removed => GossipNode {
            node_id,
            cluster_addr: String::new(),
            client_addr: String::new(),
            incarnation: 0,
            removed: true,
        },
    }
}

/// A member that advertises no client address sends the field as an empty
/// string, which is also what prost decodes an absent proto3 scalar to. Both
/// mean the same thing here: nothing can forward a client request to that node.
pub fn node_from_proto(n: GossipNode) -> (u64, DirectoryEntry) {
    if n.removed {
        return (n.node_id, DirectoryEntry::Removed);
    }
    (
        n.node_id,
        DirectoryEntry::Live(NodeDescriptor {
            addrs: NodeAddrs {
                cluster_addr: n.cluster_addr,
                client_addr: n.client_addr,
            },
            incarnation: n.incarnation,
        }),
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
    fn proto_round_trip_preserves_both_addrs_and_incarnation() {
        let desc = NodeDescriptor::new(NodeAddrs::new("host:17001", "host:17000"), 5);
        let (id, back) = node_from_proto(node_to_proto((7, desc.clone().into())));
        assert_eq!(id, 7);
        assert_eq!(back, DirectoryEntry::Live(desc));
    }

    /// A tombstone has to survive the wire, or a removal stops at the node that
    /// made it.
    #[test]
    fn proto_round_trip_preserves_a_tombstone() {
        let wire = node_to_proto((7, DirectoryEntry::Removed));
        assert!(wire.removed);
        assert_eq!(wire.cluster_addr, "", "a tombstone carries no address");
        assert_eq!(node_from_proto(wire), (7, DirectoryEntry::Removed));
    }

    /// A member advertising no client address round-trips as cluster-only.
    #[test]
    fn proto_round_trip_tolerates_absent_client_addr() {
        let (id, back) = node_from_proto(GossipNode {
            node_id: 7,
            cluster_addr: "host:17001".into(),
            client_addr: String::new(),
            incarnation: 2,
            removed: false,
        });
        assert_eq!(id, 7);
        assert_eq!(
            back,
            DirectoryEntry::Live(NodeDescriptor::new(
                NodeAddrs::cluster_only("host:17001"),
                2
            ))
        );
    }

    /// A peer running the pre-incarnation wire format sends no field at all;
    /// prost decodes that to 0, which is exactly the "written on its behalf"
    /// rank — so an old peer's copies never outbid a self-published descriptor.
    #[test]
    fn an_absent_incarnation_decodes_as_a_hint() {
        let (_, back) = node_from_proto(GossipNode {
            node_id: 7,
            cluster_addr: "host:17001".into(),
            client_addr: "host:17000".into(),
            incarnation: 0,
            removed: false,
        });
        assert_eq!(
            back,
            DirectoryEntry::Live(NodeDescriptor::hint(NodeAddrs::new(
                "host:17001",
                "host:17000"
            )))
        );
    }
}
