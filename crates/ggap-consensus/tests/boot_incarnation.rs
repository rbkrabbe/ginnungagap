//! A node that moves must outrank the copies of itself its peers still hold.
//!
//! The rank is the boot counter in the data dir, so every restart here is a
//! real one: the `FjallStore` is dropped and reopened from the same directory,
//! and the incarnation under test is whatever the counter on disk yields.

use tempfile::TempDir;

use ggap_consensus::{merge_gossip_state, ShardRegistry};
use ggap_proto::v1::{GossipNode, GossipState};
use ggap_storage::fjall::FjallStore;
use ggap_storage::BootCounter;
use ggap_types::{NodeAddrs, NodeDescriptor};

fn desc(cluster: &str, client: &str, incarnation: u64) -> NodeDescriptor {
    NodeDescriptor::new(NodeAddrs::new(cluster, client), incarnation)
}

/// What a peer sends when it dials this node's `Exchange`.
fn gossip_from(sender: u64, nodes: &[(u64, NodeDescriptor)]) -> GossipState {
    GossipState {
        sender_node_id: sender,
        directory: nodes
            .iter()
            .map(|(id, d)| GossipNode {
                node_id: *id,
                cluster_addr: d.addrs.cluster_addr.clone(),
                client_addr: d.addrs.client_addr.clone(),
                incarnation: d.incarnation,
            })
            .collect(),
        shards: vec![],
    }
}

/// One boot of node 1: open the data dir, take an incarnation from it, publish
/// this node's descriptor at that rank into a fresh registry.
async fn boot(dir: &TempDir, cluster: &str, client: &str) -> (u64, ShardRegistry) {
    let store = FjallStore::open(dir.path()).unwrap();
    let incarnation = BootCounter::new(store).advance();
    let registry = ShardRegistry::new(1, []);
    registry
        .merge_directory([(1, desc(cluster, client, incarnation))])
        .await;
    (incarnation, registry)
}

/// The acceptance case. Node 1 boots at one address, restarts at another, and
/// the cluster converges on the new one: the peer that still holds the old
/// address adopts the new one, and — the part that arrival order alone does not
/// give — the stale copy gossiped back at node 1 loses to what node 1 says
/// about itself.
///
/// Every assertion here turns on the second boot publishing *above* the first.
/// A counter that stopped incrementing would leave both at incarnation 1, where
/// ties go to the incoming entry and the stale copy wins at both ends.
#[tokio::test]
async fn a_restarted_node_at_a_new_address_outranks_its_own_stale_copy() {
    let dir = TempDir::new().unwrap();

    let (first, _registry) = boot(&dir, "old:17001", "old:17000").await;

    // A peer that learned node 1 at its old address during that first boot.
    let peer = ShardRegistry::new(2, []);
    merge_gossip_state(
        &peer,
        gossip_from(1, &[(1, desc("old:17001", "old:17000", first))]),
    )
    .await;
    assert_eq!(peer.directory_addr(1).await, Some("old:17001".into()));

    // Node 1 restarts at a new address.
    let (second, moved) = boot(&dir, "new:17001", "new:17000").await;
    assert!(
        second > first,
        "a restart must publish above the boot before it ({second} vs {first})"
    );

    // The peer adopts the new address, and cannot be talked back out of it by a
    // third node still repeating the old one.
    merge_gossip_state(
        &peer,
        gossip_from(1, &[(1, desc("new:17001", "new:17000", second))]),
    )
    .await;
    merge_gossip_state(
        &peer,
        gossip_from(3, &[(1, desc("old:17001", "old:17000", first))]),
    )
    .await;
    assert_eq!(
        peer.directory_addr(1).await,
        Some("new:17001".into()),
        "a peer must converge on the moved node's new address"
    );

    // And the moved node keeps authorship of its own entry when a peer gossips
    // the stale copy back at it.
    merge_gossip_state(
        &moved,
        gossip_from(2, &[(1, desc("old:17001", "old:17000", first))]),
    )
    .await;
    assert_eq!(
        moved.directory_addr(1).await,
        Some("new:17001".into()),
        "a stale copy must not overwrite the descriptor at its own author"
    );
}

/// A wiped data dir restarts the count, so the moved node publishes at 1 again
/// and cannot outbid a peer holding a higher incarnation for its id. This is the
/// documented caveat, pinned so it stays a known cost rather than a surprise:
/// an address change made across a wipe needs a fresh node id.
#[tokio::test]
async fn a_wiped_data_dir_cannot_outbid_a_peers_higher_incarnation() {
    let dir = TempDir::new().unwrap();
    let (_, _) = boot(&dir, "old:17001", "old:17000").await;
    let (high, _) = boot(&dir, "old:17001", "old:17000").await;

    let peer = ShardRegistry::new(2, []);
    merge_gossip_state(
        &peer,
        gossip_from(1, &[(1, desc("old:17001", "old:17000", high))]),
    )
    .await;

    let wiped = TempDir::new().unwrap();
    let (after_wipe, _) = boot(&wiped, "new:17001", "new:17000").await;
    assert_eq!(after_wipe, 1, "a wiped data dir starts the count over");

    merge_gossip_state(
        &peer,
        gossip_from(1, &[(1, desc("new:17001", "new:17000", after_wipe))]),
    )
    .await;
    assert_eq!(
        peer.directory_addr(1).await,
        Some("old:17001".into()),
        "the wiped node is outranked by the peer's copy, and needs a fresh id"
    );
}
