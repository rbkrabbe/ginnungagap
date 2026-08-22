//! The directory is cached on disk so a restarted node can resolve its peers
//! immediately, instead of failing sends until some peer dials it.
//!
//! Every restart here is a real one: the `FjallStore` is dropped and reopened
//! from the same data directory, so the assertions turn on what was written to
//! disk rather than on a handle that outlived the "restart".

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use ggap_consensus::{merge_gossip_state, GossipTask, ShardRegistry, ShardRouter};
use ggap_proto::v1::{GossipNode, GossipState};
use ggap_storage::fjall::FjallStore;
use ggap_storage::shard_map::ShardMap;
use ggap_storage::DirectoryStore;
use ggap_types::{NodeAddrs, NodeDescriptor};

/// A router hosting nothing: the node under test learns about its peers from
/// gossip alone, so no membership can quietly supply what the disk should.
fn router_on(store: Arc<FjallStore>) -> Arc<ShardRouter> {
    let shard_map = Arc::new(ShardMap::load(store).unwrap());
    Arc::new(ShardRouter::new(shard_map))
}

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

/// Run node 1's gossip task against `store` until the directory it holds has
/// been written out, then stop it. The task never reaches a peer — the
/// addresses in these tests are unroutable — so only the local publish and
/// persist steps do anything.
async fn persist_directory(store: Arc<FjallStore>, registry: Arc<ShardRegistry>) {
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(
        GossipTask::new(
            router_on(store.clone()),
            registry.clone(),
            1,
            NodeAddrs::new("node1:17001", "node1:17000"),
            1,
            cancel.clone(),
        )
        .with_interval(Duration::from_millis(10))
        .with_rpc_timeout(Duration::from_millis(10))
        .with_directory_store(DirectoryStore::new(store.clone()))
        .run(),
    );

    let on_disk = DirectoryStore::new(store);
    for _ in 0..200 {
        if on_disk.load().iter().any(|(id, _)| *id != 1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    cancel.cancel();
    let _ = handle.await;
    assert!(
        on_disk.load().iter().any(|(id, _)| *id != 1),
        "the gossip task never persisted the peers it had learned"
    );
}

/// The acceptance case. Node 1 learns nodes 2 and 3 from an inbound `Exchange`,
/// restarts, and resolves both before anything gossips to it: no gossip task
/// runs on the restarted node, and nodes 2 and 3 exist only as addresses, so
/// being dialled cannot be the mechanism that makes this pass.
#[tokio::test]
async fn a_restarted_node_resolves_every_peer_before_any_gossip() {
    let tempdir = TempDir::new().unwrap();

    {
        let store = FjallStore::open(tempdir.path()).unwrap();
        let registry = Arc::new(ShardRegistry::new(1, []));
        merge_gossip_state(
            &registry,
            gossip_from(
                2,
                &[
                    (2, desc("node2:17001", "node2:17000", 1)),
                    (3, desc("node3:17001", "node3:17000", 4)),
                ],
            ),
        )
        .await;
        persist_directory(store, registry).await;
    }

    let store = FjallStore::open(tempdir.path()).unwrap();
    let registry = ShardRegistry::new(1, []);
    registry
        .merge_directory(DirectoryStore::new(store).load())
        .await;

    assert_eq!(
        registry.directory_addr(2).await,
        Some("node2:17001".into()),
        "a restarted node must resolve its peers from disk"
    );
    assert_eq!(registry.client_addr(2).await, Some("node2:17000".into()));
    assert_eq!(registry.directory_addr(3).await, Some("node3:17001".into()));
    assert_eq!(
        registry.peers_excluding_self().await,
        vec![
            (2, "node2:17001".to_string()),
            (3, "node3:17001".to_string())
        ],
        "the restored directory must also supply gossip targets"
    );
}

/// Incarnations survive the restart, so a stale copy still in flight loses to
/// what the node had already learned. Restoring at incarnation 0 would let one
/// win.
#[tokio::test]
async fn a_restored_entry_keeps_its_incarnation() {
    let tempdir = TempDir::new().unwrap();

    {
        let store = FjallStore::open(tempdir.path()).unwrap();
        let registry = Arc::new(ShardRegistry::new(1, []));
        merge_gossip_state(
            &registry,
            gossip_from(2, &[(2, desc("moved:17001", "moved:17000", 4))]),
        )
        .await;
        persist_directory(store, registry).await;
    }

    let store = FjallStore::open(tempdir.path()).unwrap();
    let registry = ShardRegistry::new(1, []);
    registry
        .merge_directory(DirectoryStore::new(store).load())
        .await;

    merge_gossip_state(
        &registry,
        gossip_from(3, &[(2, desc("old:17001", "old:17000", 3))]),
    )
    .await;

    assert_eq!(registry.directory_addr(2).await, Some("moved:17001".into()));
}

/// No record at all: an empty directory and a node that still converges once a
/// peer dials it. This is the state every fresh node starts in.
#[tokio::test]
async fn a_missing_record_starts_empty_and_still_converges() {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();

    let registry = ShardRegistry::new(1, []);
    let restored = DirectoryStore::new(store).load();
    assert_eq!(restored, vec![]);
    registry.merge_directory(restored).await;
    assert_eq!(registry.directory_addr(2).await, None);

    merge_gossip_state(
        &registry,
        gossip_from(2, &[(2, desc("node2:17001", "node2:17000", 1))]),
    )
    .await;
    assert_eq!(registry.directory_addr(2).await, Some("node2:17001".into()));
}

/// A corrupt record must be a warning and an empty directory, never a failed
/// start: the cache is worth less than the node.
#[tokio::test]
async fn a_corrupt_record_starts_empty_and_still_converges() {
    let tempdir = TempDir::new().unwrap();

    {
        let store = FjallStore::open(tempdir.path()).unwrap();
        let registry = Arc::new(ShardRegistry::new(1, []));
        merge_gossip_state(
            &registry,
            gossip_from(2, &[(2, desc("node2:17001", "node2:17000", 1))]),
        )
        .await;
        persist_directory(store.clone(), registry).await;

        // Truncating the record leaves bincode with a prefix it cannot decode.
        let key = ggap_storage::keys::node_key("directory");
        let mut bytes = store.node.get(&key).unwrap().unwrap().to_vec();
        bytes.truncate(bytes.len() / 2);
        store.node.insert(&key, bytes).unwrap();
    }

    let store = FjallStore::open(tempdir.path()).unwrap();
    let registry = ShardRegistry::new(1, []);
    let restored = DirectoryStore::new(store).load();
    assert_eq!(restored, vec![], "a corrupt record must load as empty");
    registry.merge_directory(restored).await;

    merge_gossip_state(
        &registry,
        gossip_from(2, &[(2, desc("node2:17001", "node2:17000", 1))]),
    )
    .await;
    assert_eq!(registry.directory_addr(2).await, Some("node2:17001".into()));
}
