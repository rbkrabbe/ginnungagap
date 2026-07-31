//! In-memory, eventually-consistent view of cluster-wide shard placement and
//! per-shard Raft status, populated by the gossip task ([`crate::gossip`]).
//!
//! The registry is a rebuildable cache — nothing here is persisted. It lets any
//! node answer `ListShards` / `ClusterStatus` for shards it does not host
//! locally, degrading gracefully (entries simply age) when a peer is
//! unreachable. It holds no gRPC types; the proto <-> [`ShardEntry`] conversion
//! lives in [`crate::gossip`].

use std::collections::HashMap;

use tokio::sync::RwLock;
use tokio::time::Instant;

use ggap_types::{NodeAddrs, ShardId};

/// One shard's placement + lightweight Raft status as last known to this node.
///
/// The hosting set is exactly the Raft membership (`voters` ∪ `learners`); there
/// is no separate "hosting node ids" — that would be redundant.
#[derive(Clone, Debug)]
pub struct ShardEntry {
    pub shard_id: ShardId,
    pub range_start: String,
    pub range_end: String,
    pub state: String,
    pub leader_id: Option<u64>,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub term: u64,
    pub last_applied: u64,
    /// Monotonic heartbeat counter at the origin node.
    pub version: u64,
    /// The node that produced this record.
    pub origin_node_id: u64,
    /// When *this* node last refreshed the entry — monotonic, pausable for the
    /// deterministic simulation harness (`tokio::time`, never `std::time`).
    pub last_updated: Instant,
}

impl ShardEntry {
    /// Age of the snapshot from this node's perspective, in milliseconds.
    pub fn age_ms(&self) -> u64 {
        self.last_updated.elapsed().as_millis() as u64
    }
}

/// The eventually-consistent global view shared between the gossip task, the
/// gossip service, and the admin service.
pub struct ShardRegistry {
    self_node_id: u64,
    /// `node_id -> addresses`, learned transitively via gossip + bootstrap
    /// seeds.
    directory: RwLock<HashMap<u64, NodeAddrs>>,
    /// Best-known entry per shard.
    shards: RwLock<HashMap<ShardId, ShardEntry>>,
}

impl ShardRegistry {
    /// Create a registry seeded with an initial `node_id -> addresses` directory
    /// (typically just `self`).
    pub fn new(self_node_id: u64, seeds: impl IntoIterator<Item = (u64, NodeAddrs)>) -> Self {
        ShardRegistry {
            self_node_id,
            directory: RwLock::new(seeds.into_iter().collect()),
            shards: RwLock::new(HashMap::new()),
        }
    }

    pub fn self_node_id(&self) -> u64 {
        self.self_node_id
    }

    /// Record a shard this node hosts locally. The freshly-built local entry
    /// always wins (its `last_updated` is now), so local data is never shadowed
    /// by older gossip about the same shard.
    pub async fn upsert_local(&self, entry: ShardEntry) {
        self.shards.write().await.insert(entry.shard_id, entry);
    }

    /// Merge a gossiped entry using a deterministic, eventual-consistency-safe
    /// conflict rule: higher term, then higher version, then leader-origin
    /// authority, then higher last_applied, then higher origin id. On accept,
    /// `last_updated` is set to now (freshness = when *we* learned it).
    pub async fn merge_entry(&self, mut incoming: ShardEntry) {
        incoming.last_updated = Instant::now();
        let mut shards = self.shards.write().await;
        match shards.get(&incoming.shard_id) {
            Some(existing) if !Self::incoming_wins(existing, &incoming) => {}
            _ => {
                shards.insert(incoming.shard_id, incoming);
            }
        }
    }

    /// True if `incoming` should replace `existing`.
    fn incoming_wins(existing: &ShardEntry, incoming: &ShardEntry) -> bool {
        let existing_leader_origin = existing.leader_id == Some(existing.origin_node_id);
        let incoming_leader_origin = incoming.leader_id == Some(incoming.origin_node_id);
        (
            incoming.term,
            incoming.version,
            incoming_leader_origin,
            incoming.last_applied,
            incoming.origin_node_id,
        ) > (
            existing.term,
            existing.version,
            existing_leader_origin,
            existing.last_applied,
            existing.origin_node_id,
        )
    }

    /// Merge a batch of directory entries, **field by field**.
    ///
    /// An empty field in `entries` is ignored rather than written, so a source
    /// that knows only one of the two addresses cannot erase the other. This is
    /// load-bearing, not defensive: the gossip task refreshes the directory from
    /// Raft membership once per tick (see [`crate::gossip`]), and membership
    /// carries `cluster_addr` alone. A whole-value merge would blank every
    /// `client_addr` in the cluster roughly once a second.
    pub async fn merge_directory(&self, entries: impl IntoIterator<Item = (u64, NodeAddrs)>) {
        let mut dir = self.directory.write().await;
        for (node_id, addrs) in entries {
            if addrs.cluster_addr.is_empty() && addrs.client_addr.is_empty() {
                continue;
            }
            let slot = dir.entry(node_id).or_default();
            if !addrs.cluster_addr.is_empty() {
                slot.cluster_addr = addrs.cluster_addr;
            }
            if !addrs.client_addr.is_empty() {
                slot.client_addr = addrs.client_addr;
            }
        }
    }

    /// Cluster address for a node id, if known.
    pub async fn directory_addr(&self, node_id: u64) -> Option<String> {
        self.directory
            .read()
            .await
            .get(&node_id)
            .map(|a| a.cluster_addr.clone())
            .filter(|a| !a.is_empty())
    }

    /// Client-facing address for a node id, if known.
    ///
    /// `None` covers both an unknown node and a known node whose client address
    /// has never been gossiped — a peer running a version that predates the
    /// field, or an entry so far only learned from Raft membership. Callers must
    /// treat it as "cannot reach this node's client API yet", never dialling the
    /// empty string.
    pub async fn client_addr(&self, node_id: u64) -> Option<String> {
        self.directory
            .read()
            .await
            .get(&node_id)
            .map(|a| a.client_addr.clone())
            .filter(|a| !a.is_empty())
    }

    /// Gossip peers: every known node except self, as `(node_id, cluster_addr)`.
    /// Gossip dials the cluster port, so the client address is irrelevant here.
    pub async fn peers_excluding_self(&self) -> Vec<(u64, String)> {
        let mut peers: Vec<(u64, String)> = self
            .directory
            .read()
            .await
            .iter()
            .filter(|(id, _)| **id != self.self_node_id)
            .filter(|(_, addrs)| !addrs.cluster_addr.is_empty())
            .map(|(id, addrs)| (*id, addrs.cluster_addr.clone()))
            .collect();
        // Deterministic order (the gossip task rotates over this).
        peers.sort_by_key(|(id, _)| *id);
        peers
    }

    /// The full view to push to a peer (or return from `Exchange`). Sorted for
    /// deterministic output.
    pub async fn snapshot_for_gossip(&self) -> (Vec<(u64, NodeAddrs)>, Vec<ShardEntry>) {
        let mut dir: Vec<(u64, NodeAddrs)> = self
            .directory
            .read()
            .await
            .iter()
            .map(|(id, addrs)| (*id, addrs.clone()))
            .collect();
        dir.sort_by_key(|(id, _)| *id);

        let mut shards: Vec<ShardEntry> = self.shards.read().await.values().cloned().collect();
        shards.sort_by_key(|e| e.shard_id);
        (dir, shards)
    }

    /// Best-known entry for a shard, if any.
    pub async fn lookup(&self, shard_id: ShardId) -> Option<ShardEntry> {
        self.shards.read().await.get(&shard_id).cloned()
    }

    /// All known shard entries, sorted by shard id.
    pub async fn all(&self) -> Vec<ShardEntry> {
        let mut shards: Vec<ShardEntry> = self.shards.read().await.values().cloned().collect();
        shards.sort_by_key(|e| e.shard_id);
        shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(shard_id: ShardId, term: u64, version: u64, origin: u64) -> ShardEntry {
        ShardEntry {
            shard_id,
            range_start: String::new(),
            range_end: String::new(),
            state: "Active".into(),
            leader_id: Some(origin),
            voters: vec![1, 2, 3],
            learners: vec![],
            term,
            last_applied: 10,
            version,
            origin_node_id: origin,
            last_updated: Instant::now(),
        }
    }

    #[tokio::test]
    async fn higher_term_wins() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_entry(entry(0, 5, 1, 2)).await;
        reg.merge_entry(entry(0, 4, 99, 3)).await; // higher version but lower term
        assert_eq!(reg.lookup(0).await.unwrap().term, 5);
    }

    #[tokio::test]
    async fn higher_version_breaks_term_tie() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_entry(entry(0, 5, 1, 2)).await;
        reg.merge_entry(entry(0, 5, 2, 2)).await;
        assert_eq!(reg.lookup(0).await.unwrap().version, 2);
    }

    #[tokio::test]
    async fn local_upsert_present_in_snapshot() {
        let reg = ShardRegistry::new(1, []);
        reg.upsert_local(entry(7, 1, 1, 1)).await;
        let (_, shards) = reg.snapshot_for_gossip().await;
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, 7);
    }

    #[tokio::test]
    async fn peers_exclude_self() {
        let reg = ShardRegistry::new(
            1,
            [
                (1, NodeAddrs::cluster_only("a")),
                (2, NodeAddrs::cluster_only("b")),
                (3, NodeAddrs::cluster_only("c")),
            ],
        );
        let peers = reg.peers_excluding_self().await;
        assert_eq!(peers, vec![(2, "b".into()), (3, "c".into())]);
    }

    #[tokio::test]
    async fn merge_directory_ignores_empty_addr() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, NodeAddrs::default()), (3, NodeAddrs::cluster_only("c"))])
            .await;
        assert_eq!(reg.directory_addr(2).await, None);
        assert_eq!(reg.directory_addr(3).await, Some("c".into()));
    }

    /// The rule the Raft-membership refresh depends on: a merge carrying only a
    /// cluster addr must not blank a client addr learned from gossip. Without
    /// this, `refresh_local` would erase every client addr once per tick.
    #[tokio::test]
    async fn merge_directory_preserves_client_addr_across_cluster_only_merge() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, NodeAddrs::new("host:17001", "host:17000"))])
            .await;

        // Simulate many rounds of the membership-derived refresh.
        for _ in 0..5 {
            reg.merge_directory([(2, NodeAddrs::cluster_only("host:17001"))])
                .await;
        }

        assert_eq!(reg.client_addr(2).await, Some("host:17000".into()));
        assert_eq!(reg.directory_addr(2).await, Some("host:17001".into()));
    }

    /// The mirror case: a client-only merge must not blank a known cluster addr.
    #[tokio::test]
    async fn merge_directory_preserves_cluster_addr_across_client_only_merge() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, NodeAddrs::new("host:17001", "host:17000"))])
            .await;
        reg.merge_directory([(2, NodeAddrs::new("", "other:17000"))])
            .await;

        assert_eq!(reg.directory_addr(2).await, Some("host:17001".into()));
        assert_eq!(reg.client_addr(2).await, Some("other:17000".into()));
    }

    /// A peer predating the field gossips entries with an empty client_addr.
    /// Merging one must neither panic nor clear anything.
    #[tokio::test]
    async fn client_addr_absent_for_unknown_and_for_cluster_only_node() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, NodeAddrs::cluster_only("host:17001"))])
            .await;

        assert_eq!(reg.client_addr(99).await, None, "unknown node");
        assert_eq!(reg.client_addr(2).await, None, "known, no client addr");
        assert_eq!(reg.directory_addr(2).await, Some("host:17001".into()));
    }

    /// A directory entry with no usable cluster addr must not become a gossip
    /// target — dialling "" would fail every round.
    #[tokio::test]
    async fn peers_exclude_nodes_with_no_cluster_addr() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, NodeAddrs::new("", "host:17000"))])
            .await;

        assert!(reg.peers_excluding_self().await.is_empty());
        assert_eq!(reg.client_addr(2).await, Some("host:17000".into()));
    }

    #[tokio::test]
    async fn snapshot_carries_both_addrs() {
        let reg = ShardRegistry::new(1, [(1, NodeAddrs::new("c1:17001", "c1:17000"))]);
        let (dir, _) = reg.snapshot_for_gossip().await;
        assert_eq!(dir, vec![(1, NodeAddrs::new("c1:17001", "c1:17000"))]);
    }
}
