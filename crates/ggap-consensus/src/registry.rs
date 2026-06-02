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

use ggap_types::ShardId;

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
    /// `node_id -> cluster_addr`, learned transitively via gossip + bootstrap
    /// seeds. The address is the cluster gRPC endpoint (shared by RaftService,
    /// AdminService, and GossipService).
    directory: RwLock<HashMap<u64, String>>,
    /// Best-known entry per shard.
    shards: RwLock<HashMap<ShardId, ShardEntry>>,
}

impl ShardRegistry {
    /// Create a registry seeded with an initial `node_id -> cluster_addr`
    /// directory (typically just `self`).
    pub fn new(self_node_id: u64, seeds: impl IntoIterator<Item = (u64, String)>) -> Self {
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

    /// Merge a batch of `node_id -> cluster_addr` directory entries.
    pub async fn merge_directory(&self, entries: impl IntoIterator<Item = (u64, String)>) {
        let mut dir = self.directory.write().await;
        for (node_id, addr) in entries {
            if addr.is_empty() {
                continue;
            }
            dir.insert(node_id, addr);
        }
    }

    /// Cluster address for a node id, if known.
    pub async fn directory_addr(&self, node_id: u64) -> Option<String> {
        self.directory.read().await.get(&node_id).cloned()
    }

    /// Gossip peers: every known node except self, as `(node_id, addr)`.
    pub async fn peers_excluding_self(&self) -> Vec<(u64, String)> {
        let mut peers: Vec<(u64, String)> = self
            .directory
            .read()
            .await
            .iter()
            .filter(|(id, _)| **id != self.self_node_id)
            .map(|(id, addr)| (*id, addr.clone()))
            .collect();
        // Deterministic order (the gossip task rotates over this).
        peers.sort_by_key(|(id, _)| *id);
        peers
    }

    /// The full view to push to a peer (or return from `Exchange`). Sorted for
    /// deterministic output.
    pub async fn snapshot_for_gossip(&self) -> (Vec<(u64, String)>, Vec<ShardEntry>) {
        let mut dir: Vec<(u64, String)> = self
            .directory
            .read()
            .await
            .iter()
            .map(|(id, addr)| (*id, addr.clone()))
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
        let reg = ShardRegistry::new(1, [(1, "a".into()), (2, "b".into()), (3, "c".into())]);
        let peers = reg.peers_excluding_self().await;
        assert_eq!(peers, vec![(2, "b".into()), (3, "c".into())]);
    }

    #[tokio::test]
    async fn merge_directory_ignores_empty_addr() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, String::new()), (3, "c".into())])
            .await;
        assert_eq!(reg.directory_addr(2).await, None);
        assert_eq!(reg.directory_addr(3).await, Some("c".into()));
    }
}
