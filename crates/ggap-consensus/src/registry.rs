//! In-memory, eventually-consistent view of cluster-wide shard placement and
//! per-shard Raft status, populated by the gossip task ([`crate::gossip`]).
//!
//! Its directory is the source of truth for node addresses: membership carries
//! ids, and every send resolves one through here. Shard entries are a
//! rebuildable cache and are never persisted; the directory is written out by
//! the gossip task and read back at startup
//! ([`ggap_storage::DirectoryStore`]) so a restarted node can resolve peers
//! before anyone gossips to it — persistence buys immediacy, not authority.
//!
//! It lets any node answer `ListShards` / `ClusterStatus` for shards it does
//! not host locally, degrading gracefully (entries simply age) when a peer is
//! unreachable. It holds no gRPC types; the proto <-> [`ShardEntry`] conversion
//! lives in [`crate::gossip`].

use std::collections::HashMap;

use tokio::sync::RwLock;
use tokio::time::Instant;

use ggap_types::{DirectoryEntry, ShardId};

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
    /// `node_id -> entry`. Each node publishes its own descriptor; gossip
    /// copies it everywhere, and [`Self::merge_directory`] orders copies by
    /// incarnation. A retired node's entry is a tombstone that outranks every
    /// descriptor for that id.
    directory: RwLock<HashMap<u64, DirectoryEntry>>,
    /// Best-known entry per shard.
    shards: RwLock<HashMap<ShardId, ShardEntry>>,
}

impl ShardRegistry {
    /// Create a registry with bootstrap gossip peers as `(node_id,
    /// cluster_addr)`. A node added to a shard by `AddLearner` needs none: it
    /// reaches the cluster through the Raft membership it is given, and a
    /// restart resolves its peers from the persisted directory. Seeds are for
    /// nodes in no membership, which nobody will ever dial — the observer
    /// harness, and later `ggap-pd`.
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

    /// Merge a batch of descriptors, ordered by incarnation: **highest wins**,
    /// ties resolved in favour of the incoming entry.
    ///
    /// A descriptor is authored by the node it describes, so its incarnation is
    /// a clock over exactly one writer's publications and comparing two copies
    /// is unambiguous. That is what lets a node move: it restarts at a higher
    /// incarnation and outbids every stale copy in flight.
    ///
    /// Incarnation 0 is reserved for a descriptor written on a node's behalf:
    /// `AddLearner` records where a joining node is before that node has said
    /// so itself. A node's own publications start at 1 and therefore always
    /// supersede such a hint. Ties go to the incoming entry, which keeps a feed
    /// of hints last-write-wins among themselves.
    ///
    /// A descriptor is a whole value: merging one with no client address
    /// *clears* a previously-known one rather than treating the gap as
    /// "unknown, don't touch". An entry with neither address describes no node
    /// and is skipped.
    ///
    /// A tombstone sits outside that ordering entirely: it beats every
    /// descriptor for its id, and nothing beats it. Rank could not do this job,
    /// because a peer may hold a copy at an incarnation nobody else has seen,
    /// and a removal one stray copy can undo is not a removal.
    pub async fn merge_directory<E: Into<DirectoryEntry>>(
        &self,
        entries: impl IntoIterator<Item = (u64, E)>,
    ) {
        let mut dir = self.directory.write().await;
        for (node_id, entry) in entries {
            let entry = entry.into();
            match (dir.get(&node_id), &entry) {
                // Absolute: a removal is never undone.
                (Some(DirectoryEntry::Removed), _) => {}
                (_, DirectoryEntry::Removed) => {
                    dir.insert(node_id, entry);
                }
                (existing, DirectoryEntry::Live(desc)) => {
                    if desc.addrs.cluster_addr.is_empty() && desc.addrs.client_addr.is_empty() {
                        continue;
                    }
                    let outranked = existing
                        .and_then(|e| e.descriptor())
                        .is_some_and(|e| e.incarnation > desc.incarnation);
                    if !outranked {
                        dir.insert(node_id, entry);
                    }
                }
            }
        }
    }

    /// Tombstone a node's directory entry. Idempotent, and irreversible for as
    /// long as the cluster lives: see [`DirectoryEntry::Removed`].
    ///
    /// This only writes it locally. Gossip carries it to every peer, and it
    /// survives restarts through the persisted directory.
    pub async fn retire(&self, node_id: u64) {
        self.directory
            .write()
            .await
            .insert(node_id, DirectoryEntry::Removed);
    }

    /// Whether this node holds a tombstone for `node_id`.
    pub async fn is_retired(&self, node_id: u64) -> bool {
        self.directory
            .read()
            .await
            .get(&node_id)
            .is_some_and(|e| e.is_removed())
    }

    /// The node id the directory maps `cluster_addr` to, if any, ignoring
    /// tombstones. The reverse of [`Self::directory_addr`]: it answers "who is
    /// already at this address", which is how a removal frees one — the join
    /// path's duplicate check keys on it (tk-8d80).
    pub async fn node_id_at(&self, cluster_addr: &str) -> Option<u64> {
        self.directory
            .read()
            .await
            .iter()
            .find(|(_, entry)| {
                entry
                    .descriptor()
                    .is_some_and(|d| d.addrs.cluster_addr == cluster_addr)
            })
            .map(|(id, _)| *id)
    }

    /// Cluster address for a node id, if known. A retired node resolves to
    /// `None`, so every send to it fails exactly as one to an unknown id does.
    pub async fn directory_addr(&self, node_id: u64) -> Option<String> {
        self.directory
            .read()
            .await
            .get(&node_id)
            .and_then(|e| e.descriptor())
            .map(|d| d.addrs.cluster_addr.clone())
            .filter(|a| !a.is_empty())
    }

    /// Client-facing address for a node id, if known.
    ///
    /// `None` covers an unknown node, a retired one, and a known node that
    /// advertises no client address. Callers must treat it as "cannot reach
    /// this node's client API", never dialling the empty string.
    pub async fn client_addr(&self, node_id: u64) -> Option<String> {
        self.directory
            .read()
            .await
            .get(&node_id)
            .and_then(|e| e.descriptor())
            .map(|d| d.addrs.client_addr.clone())
            .filter(|a| !a.is_empty())
    }

    /// Gossip peers: every known node except self, as `(node_id, cluster_addr)`.
    /// Gossip dials the cluster port, so the client address is irrelevant here.
    /// Bootstrap seeds are included, and a directory entry supersedes a seed for
    /// the same node. A retired node is dropped even when a seed names it —
    /// otherwise the one thing a removal must stop, dialling a node that is
    /// gone, would go on every round.
    pub async fn peers_excluding_self(&self) -> Vec<(u64, String)> {
        let mut by_id: HashMap<u64, String> = self.seed_peers.iter().cloned().collect();
        for (id, entry) in self.directory.read().await.iter() {
            match entry.descriptor() {
                Some(desc) if !desc.addrs.cluster_addr.is_empty() => {
                    by_id.insert(*id, desc.addrs.cluster_addr.clone());
                }
                _ => {
                    by_id.remove(id);
                }
            }
        }
        by_id.remove(&self.self_node_id);

        let mut peers: Vec<(u64, String)> = by_id.into_iter().collect();
        // Deterministic order (the gossip task rotates over this).
        peers.sort_by_key(|(id, _)| *id);
        peers
    }

    /// The whole directory, tombstones included, sorted by node id. Gossip
    /// pushes it to peers and the gossip task persists it; both want a stable
    /// order, and both must carry tombstones — a removal spreads and survives
    /// restarts by exactly the same route a descriptor does.
    pub async fn directory_snapshot(&self) -> Vec<(u64, DirectoryEntry)> {
        let mut dir: Vec<(u64, DirectoryEntry)> = self
            .directory
            .read()
            .await
            .iter()
            .map(|(id, entry)| (*id, entry.clone()))
            .collect();
        dir.sort_by_key(|(id, _)| *id);
        dir
    }

    /// The full view to push to a peer (or return from `Exchange`). Sorted for
    /// deterministic output.
    pub async fn snapshot_for_gossip(&self) -> (Vec<(u64, DirectoryEntry)>, Vec<ShardEntry>) {
        let dir = self.directory_snapshot().await;
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

    use ggap_types::{NodeAddrs, NodeDescriptor};

    /// A descriptor a node published about itself.
    fn own(cluster: &str, client: &str, incarnation: u64) -> NodeDescriptor {
        NodeDescriptor::new(NodeAddrs::new(cluster, client), incarnation)
    }

    /// An incarnation-0 entry, as `AddLearner` produces.
    fn hint(addrs: NodeAddrs) -> NodeDescriptor {
        NodeDescriptor::hint(addrs)
    }

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
            (1, hint(NodeAddrs::cluster_only("a"))),
            (2, hint(NodeAddrs::cluster_only("b"))),
            (3, hint(NodeAddrs::cluster_only("c"))),
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

        reg.merge_directory([(2, hint(NodeAddrs::new("real:17001", "real:17000")))])
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

    /// The rule the whole removal rests on: a tombstone outranks every
    /// descriptor for its id, at any incarnation, arriving in any order. A peer
    /// partitioned across the removal comes back gossiping the copy it kept, and
    /// it must lose.
    #[tokio::test]
    async fn a_tombstone_beats_every_descriptor_for_its_id() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, own("old:17001", "old:17000", 3))])
            .await;

        reg.retire(2).await;
        assert!(reg.is_retired(2).await);
        assert_eq!(reg.directory_addr(2).await, None);
        assert_eq!(reg.client_addr(2).await, None);

        // A stale copy, the highest incarnation a peer could hold, and the
        // node's own republication after a restart: all lose.
        reg.merge_directory([(2, own("old:17001", "old:17000", 3))])
            .await;
        reg.merge_directory([(2, own("back:17001", "back:17000", u64::MAX))])
            .await;
        assert!(reg.is_retired(2).await);
        assert_eq!(reg.directory_addr(2).await, None);
    }

    /// The other direction: a tombstone arriving over gossip retires a node this
    /// registry still believes in, whatever rank it holds for it.
    #[tokio::test]
    async fn a_gossiped_tombstone_retires_a_live_entry() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, own("host:17001", "host:17000", 9))])
            .await;

        reg.merge_directory([(2, DirectoryEntry::Removed)]).await;

        assert!(reg.is_retired(2).await);
        assert!(reg.peers_excluding_self().await.is_empty());
    }

    /// A tombstone must travel and must be written down, so it has to appear in
    /// the snapshot both gossip and the persisted directory are built from.
    #[tokio::test]
    async fn tombstones_are_gossiped_and_persisted() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, own("host:17001", "host:17000", 1))])
            .await;
        reg.retire(2).await;

        assert_eq!(
            reg.directory_snapshot().await,
            vec![(2, DirectoryEntry::Removed)]
        );
    }

    /// Removal exists to stop the dialling, so a retired node must leave the
    /// peer list — even when a bootstrap seed still names it.
    #[tokio::test]
    async fn a_retired_node_is_not_a_gossip_target_even_as_a_seed() {
        let reg = ShardRegistry::new(1, [(2, "seed:17001".to_string())]);
        reg.merge_directory([(2, own("host:17001", "host:17000", 1))])
            .await;
        assert_eq!(reg.peers_excluding_self().await.len(), 1);

        reg.retire(2).await;
        assert!(reg.peers_excluding_self().await.is_empty());
    }

    /// Freeing the address is the point of the removal for tk-8d80: the reverse
    /// lookup stops naming the retired id, so a different node can be added
    /// there.
    #[tokio::test]
    async fn a_retired_nodes_address_is_free_for_another_id() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, own("host:17001", "host:17000", 1))])
            .await;
        assert_eq!(reg.node_id_at("host:17001").await, Some(2));

        reg.retire(2).await;
        assert_eq!(reg.node_id_at("host:17001").await, None);

        reg.merge_directory([(3, own("host:17001", "host:17000", 1))])
            .await;
        assert_eq!(reg.node_id_at("host:17001").await, Some(3));
        assert_eq!(reg.directory_addr(3).await, Some("host:17001".into()));
    }

    #[tokio::test]
    async fn merge_directory_ignores_empty_addr() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([
            (2, hint(NodeAddrs::default())),
            (3, hint(NodeAddrs::cluster_only("c"))),
        ])
        .await;
        assert_eq!(reg.directory_addr(2).await, None);
        assert_eq!(reg.directory_addr(3).await, Some("c".into()));
    }

    /// Whole-value replacement at equal incarnation: a descriptor overwrites
    /// both fields together, never one at a time.
    #[tokio::test]
    async fn merge_directory_replaces_whole_entry() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, hint(NodeAddrs::new("host:17001", "host:17000")))])
            .await;
        reg.merge_directory([(2, hint(NodeAddrs::new("moved:17001", "moved:17000")))])
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
        reg.merge_directory([(2, hint(NodeAddrs::new("host:17001", "host:17000")))])
            .await;
        reg.merge_directory([(2, hint(NodeAddrs::cluster_only("host:17001")))])
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
        reg.merge_directory([(2, hint(NodeAddrs::new("host:17001", "host:17000")))])
            .await;
        reg.merge_directory([(2, hint(NodeAddrs::new("", "host:17000")))])
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
        reg.merge_directory([(2, hint(NodeAddrs::cluster_only("host:17001")))])
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
        reg.merge_directory([(2, hint(NodeAddrs::new("", "host:17000")))])
            .await;

        assert!(reg.peers_excluding_self().await.is_empty());
        assert_eq!(reg.client_addr(2).await, Some("host:17000".into()));
    }

    /// The ordering rule itself: a stale copy still in flight loses to the
    /// descriptor the node published after it moved.
    #[tokio::test]
    async fn lower_incarnation_cannot_overwrite_higher() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, own("moved:17001", "moved:17000", 7))])
            .await;
        reg.merge_directory([(2, own("old:17001", "old:17000", 6))])
            .await;

        assert_eq!(reg.directory_addr(2).await, Some("moved:17001".into()));
        assert_eq!(reg.client_addr(2).await, Some("moved:17000".into()));
    }

    /// The move that motivates the epic: a node restarted at a new address wins
    /// over the entry every peer already holds.
    #[tokio::test]
    async fn higher_incarnation_replaces_a_known_address() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(2, own("old:17001", "old:17000", 1))])
            .await;
        reg.merge_directory([(2, own("moved:17001", "moved:17000", 2))])
            .await;

        assert_eq!(reg.directory_addr(2).await, Some("moved:17001".into()));
    }

    /// The coexistence rule, from both sides: an incarnation-0 hint fills in for
    /// a node that has not published yet, and loses to that node the moment it
    /// does — no matter which order the two arrive in.
    #[tokio::test]
    async fn a_self_published_descriptor_outranks_the_membership_derived_feed() {
        let reg = ShardRegistry::new(1, []);

        // Derived-first: membership fills the gap, self-publication takes over.
        reg.merge_directory([(2, hint(NodeAddrs::new("derived:17001", "derived:17000")))])
            .await;
        assert_eq!(reg.directory_addr(2).await, Some("derived:17001".into()));
        reg.merge_directory([(2, own("self:17001", "self:17000", 1))])
            .await;
        assert_eq!(reg.directory_addr(2).await, Some("self:17001".into()));

        // Derived-second: the feed re-runs every tick and must not claw it back.
        reg.merge_directory([(2, hint(NodeAddrs::new("derived:17001", "derived:17000")))])
            .await;
        assert_eq!(reg.directory_addr(2).await, Some("self:17001".into()));
    }

    /// Sole authorship: a peer gossiping a stale copy of *our* descriptor cannot
    /// change what we say about ourselves. Incarnation ordering is the whole
    /// mechanism — there is no special case for self.
    #[tokio::test]
    async fn a_peers_stale_copy_of_our_own_descriptor_loses() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(1, own("me:17001", "me:17000", 4))])
            .await;
        reg.merge_directory([(1, own("me-old:17001", "me-old:17000", 3))])
            .await;

        assert_eq!(reg.directory_addr(1).await, Some("me:17001".into()));
    }

    #[tokio::test]
    async fn snapshot_carries_both_addrs() {
        let reg = ShardRegistry::new(1, []);
        reg.merge_directory([(1, own("c1:17001", "c1:17000", 3))])
            .await;
        let (dir, _) = reg.snapshot_for_gossip().await;
        assert_eq!(dir, vec![(1, own("c1:17001", "c1:17000", 3).into())]);
    }
}
