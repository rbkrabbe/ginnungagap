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
    /// Cluster addresses to gossip with before the directory has been learned.
    /// Deliberately *not* directory entries: a seed is a dial hint, not a
    /// statement about a node's addresses, and gossiping one out would push a
    /// half-known node over a fully-known one.
    seed_peers: Vec<(u64, String)>,
    /// `node_id -> addresses`, derived from Raft membership on the nodes that
    /// host the shard and copied elsewhere by gossip.
    directory: RwLock<HashMap<u64, NodeAddrs>>,
    /// Best-known entry per shard.
    shards: RwLock<HashMap<ShardId, ShardEntry>>,
}

impl ShardRegistry {
    /// Create a registry with bootstrap gossip peers as `(node_id,
    /// cluster_addr)`. A node added to a shard by `AddLearner` needs none: it
    /// reaches the cluster through the Raft membership it is given.
    pub fn new(self_node_id: u64, seed_peers: impl IntoIterator<Item = (u64, String)>) -> Self {
        ShardRegistry {
            self_node_id,
            seed_peers: seed_peers.into_iter().collect(),
            directory: RwLock::new(HashMap::new()),
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

    /// Merge a batch of directory entries, replacing whole values.
    ///
    /// Every entry is a copy of a committed Raft membership record — derived
    /// locally by [`crate::gossip`] or copied from a peer that derived it — so
    /// no source knows half of a node's addresses. Which addresses the record
    /// carries is membership's business: a member with no client address is a
    /// node nothing can forward a client request to, and merging it *clears* a
    /// previously-known one rather than treating the gap as "unknown, don't
    /// touch". That is what lets a stale address be retracted at all. An entry
    /// with neither address describes no node and is skipped.
    ///
    /// In production both are always present — bootstrap and a split's
    /// `source_members` carry a full `NodeAddrs`, and `AddLearner` rejects an
    /// empty `client_addr` — so clearing is reachable only where a harness
    /// builds cluster-only membership on purpose.
    pub async fn merge_directory(&self, entries: impl IntoIterator<Item = (u64, NodeAddrs)>) {
        let mut dir = self.directory.write().await;
        for (node_id, addrs) in entries {
            if addrs.cluster_addr.is_empty() && addrs.client_addr.is_empty() {
                continue;
            }
            dir.insert(node_id, addrs);
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
    /// `None` covers both an unknown node and a known node that advertises no
    /// client address. Callers must treat it as "cannot reach this node's client
    /// API", never dialling the empty string.
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
    /// Bootstrap seeds are included, and a directory entry supersedes a seed for
    /// the same node.
    pub async fn peers_excluding_self(&self) -> Vec<(u64, String)> {
        let mut by_id: HashMap<u64, String> = self.seed_peers.iter().cloned().collect();
        for (id, addrs) in self.directory.read().await.iter() {
            if !addrs.cluster_addr.is_empty() {
                by_id.insert(*id, addrs.cluster_addr.clone());
            }
        }
        by_id.remove(&self.self_node_id);

        let mut peers: Vec<(u64, String)> = by_id.into_iter().collect();
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
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([
            (1, NodeAddrs::cluster_only("a")),
            (2, NodeAddrs::cluster_only("b")),
            (3, NodeAddrs::cluster_only("c")),
        ])
        .await;
        let peers = reg.peers_excluding_self().await;
        assert_eq!(peers, vec![(2, "b".into()), (3, "c".into())]);
    }

    /// A seed is a dial hint until membership supplies the real entry, which
    /// then supersedes it — the seed must not resurrect a stale address.
    #[tokio::test]
    async fn seed_peers_are_gossip_targets_until_the_directory_supersedes_them() {
        let reg = ShardRegistry::new(1, [(2, "seed:17001".to_string())]);
        assert_eq!(
            reg.peers_excluding_self().await,
            vec![(2, "seed:17001".into())]
        );

        reg.merge_directory([(2, NodeAddrs::new("real:17001", "real:17000"))])
            .await;
        assert_eq!(
            reg.peers_excluding_self().await,
            vec![(2, "real:17001".into())]
        );
    }

    /// A seed says nothing about a node's addresses, so it must not surface as
    /// a directory entry, be reported by the admin service, or be gossiped out.
    #[tokio::test]
    async fn seed_peers_do_not_enter_the_directory() {
        let reg = ShardRegistry::new(1, [(2, "seed:17001".to_string())]);
        assert_eq!(reg.directory_addr(2).await, None);
        assert_eq!(reg.snapshot_for_gossip().await.0, vec![]);
    }

    #[tokio::test]
    async fn merge_directory_ignores_empty_addr() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, NodeAddrs::default()), (3, NodeAddrs::cluster_only("c"))])
            .await;
        assert_eq!(reg.directory_addr(2).await, None);
        assert_eq!(reg.directory_addr(3).await, Some("c".into()));
    }

    /// Whole-value replacement: an entry is a snapshot of one membership record,
    /// so a later one overwrites both fields together.
    #[tokio::test]
    async fn merge_directory_replaces_whole_entry() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, NodeAddrs::new("host:17001", "host:17000"))])
            .await;
        reg.merge_directory([(2, NodeAddrs::new("moved:17001", "moved:17000"))])
            .await;

        assert_eq!(reg.directory_addr(2).await, Some("moved:17001".into()));
        assert_eq!(reg.client_addr(2).await, Some("moved:17000".into()));
    }

    /// The case that separates whole-value replacement from the field-wise rule
    /// it replaced, and the reason the rule changed: a membership record with no
    /// client address means the node advertises none, so merging it *clears* a
    /// previously-known one. Under the field-wise rule the empty field was
    /// "unknown, don't touch", and a stale client address could never be
    /// retracted.
    #[tokio::test]
    async fn merge_directory_clears_a_client_addr_the_new_entry_lacks() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, NodeAddrs::new("host:17001", "host:17000"))])
            .await;
        reg.merge_directory([(2, NodeAddrs::cluster_only("host:17001"))])
            .await;

        assert_eq!(reg.client_addr(2).await, None);
        assert_eq!(reg.directory_addr(2).await, Some("host:17001".into()));
    }

    /// The mirror, as a characterisation test rather than a live scenario:
    /// `AddLearner` rejects an empty `cluster_addr`, so membership cannot
    /// produce this entry. It pins the rule's symmetry — neither field is
    /// privileged — and the resulting node correctly stops being a gossip
    /// target.
    #[tokio::test]
    async fn merge_directory_clears_a_cluster_addr_the_new_entry_lacks() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, NodeAddrs::new("host:17001", "host:17000"))])
            .await;
        reg.merge_directory([(2, NodeAddrs::new("", "host:17000"))])
            .await;

        assert_eq!(reg.directory_addr(2).await, None);
        assert_eq!(reg.client_addr(2).await, Some("host:17000".into()));
        assert!(reg.peers_excluding_self().await.is_empty());
    }

    /// A node that advertises no client address is a truthful membership entry,
    /// not a gap: nothing can forward a client request to it.
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
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(1, NodeAddrs::new("c1:17001", "c1:17000"))])
            .await;
        let (dir, _) = reg.snapshot_for_gossip().await;
        assert_eq!(dir, vec![(1, NodeAddrs::new("c1:17001", "c1:17000"))]);
    }
}
