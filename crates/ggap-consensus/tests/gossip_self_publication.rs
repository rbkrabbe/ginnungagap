//! A node is the sole author of its own addresses: it publishes its descriptor
//! into the directory whether or not it hosts a shard, and whether or not any
//! membership mentions it.

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use ggap_consensus::{GossipTask, ShardRegistry, ShardRouter};
use ggap_storage::fjall::FjallStore;
use ggap_storage::shard_map::ShardMap;
use ggap_types::{NodeAddrs, NodeDescriptor};

/// A router hosting nothing: no shards are registered, so there is no
/// membership for the directory to be derived from.
fn empty_router() -> (Arc<ShardRouter>, TempDir) {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();
    let shard_map = Arc::new(ShardMap::load(store).unwrap());
    (Arc::new(ShardRouter::new(shard_map)), tempdir)
}

/// Poll the registry until `f` holds, or fail. The gossip task refreshes on a
/// timer, so the first tick has to be waited for rather than triggered.
async fn eventually(
    registry: &ShardRegistry,
    what: &str,
    f: impl Fn(&[(u64, NodeDescriptor)]) -> bool,
) {
    for _ in 0..200 {
        let (dir, _) = registry.snapshot_for_gossip().await;
        if f(&dir) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// The acceptance case: with the router empty, the node's own descriptor still
/// lands in the directory and in the snapshot that gossip pushes to peers — so
/// a node that hosts no shard is still reachable, rather than going invisible.
#[tokio::test]
async fn a_node_hosting_no_shard_publishes_its_own_descriptor() {
    let (router, _tempdir) = empty_router();
    let registry = Arc::new(ShardRegistry::new(7, []));
    let cancel = CancellationToken::new();

    let handle = tokio::spawn(
        GossipTask::new(
            router,
            registry.clone(),
            7,
            NodeAddrs::new("host:17001", "host:17000"),
            3,
            cancel.clone(),
        )
        .with_interval(Duration::from_millis(10))
        .run(),
    );

    eventually(&registry, "self descriptor in the gossip snapshot", |dir| {
        dir == [(
            7,
            NodeDescriptor::new(NodeAddrs::new("host:17001", "host:17000"), 3),
        )]
    })
    .await;

    assert_eq!(registry.directory_addr(7).await, Some("host:17001".into()));
    assert_eq!(registry.client_addr(7).await, Some("host:17000".into()));

    cancel.cancel();
    let _ = handle.await;
}

/// Sole authorship end to end: once the node has published, a peer's stale copy
/// of *our* descriptor cannot displace it. The gossip task is stopped first, so
/// the assertion turns on the merge rule alone and cannot be masked by a
/// republication landing between the merge and the check.
#[tokio::test]
async fn a_stale_copy_of_our_descriptor_cannot_displace_our_own() {
    let (router, _tempdir) = empty_router();
    let registry = Arc::new(ShardRegistry::new(7, []));
    let cancel = CancellationToken::new();

    let handle = tokio::spawn(
        GossipTask::new(
            router,
            registry.clone(),
            7,
            NodeAddrs::new("host:17001", "host:17000"),
            3,
            cancel.clone(),
        )
        .with_interval(Duration::from_millis(10))
        .run(),
    );

    eventually(&registry, "the first self publication", |dir| {
        dir.iter().any(|(id, _)| *id == 7)
    })
    .await;

    // Stop publishing before merging, so nothing can republish underneath us.
    cancel.cancel();
    let _ = handle.await;

    // What a peer that has not yet heard about the move would send us.
    registry
        .merge_directory([(
            7,
            NodeDescriptor::new(NodeAddrs::new("old:17001", "old:17000"), 2),
        )])
        .await;

    assert_eq!(
        registry.directory_addr(7).await,
        Some("host:17001".into()),
        "a lower-incarnation copy must never displace our own descriptor"
    );
    assert_eq!(registry.client_addr(7).await, Some("host:17000".into()));
}
