//! Single-node shard split integration test.
//!
//! Verifies that splitting a shard correctly partitions data and routes
//! reads/writes to the correct shard after the split.
//!
//! Run with:
//!   cargo test -p ggap-server --test split_single_node -- --nocapture

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, ServerState};
use tempfile::TempDir;

use ggap_consensus::{
    build_raft_config, run_split_handler, GgapLogStorage, GgapNetworkFactory, GgapRaft,
    GgapStateMachine, OpenRaftCluster, OpenRaftNode, RaftNode, ShardRouter, SplitCoordinator,
    SplitCoordinatorConfig,
};
use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::traits::StateMachineStore;
use ggap_storage::ShardMap;
use ggap_types::{KvCommand, KvResponse, ReadMode, WriteMode};

/// Helper to set up a single-node cluster with one shard.
struct TestSetup {
    router: Arc<ShardRouter>,
    shard_map: Arc<ShardMap>,
    split_coordinator: Arc<SplitCoordinator>,
    fsm: Arc<FjallStateMachine>,
    raft: Arc<GgapRaft>,
    _tempdir: TempDir,
}

async fn setup() -> TestSetup {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();

    // ShardMap must be created before FSM so we can pass it in.
    let shard_map = Arc::new(ShardMap::load(store.clone()).unwrap());
    shard_map.initialize_default().await.unwrap();

    // Split channel: FjallStateMachine sends events; background handler receives them.
    let (split_tx, split_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut fsm_builder = FjallStateMachine::new(store.clone());
    fsm_builder.set_split_sender(split_tx);
    fsm_builder.set_shard_map(shard_map.clone());
    let fsm = Arc::new(fsm_builder);

    let raft_cfg = build_raft_config(50, 150, 300, 500);
    let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), 0);
    let sm = GgapStateMachine::new(fsm.clone(), 0);
    let raft = Arc::new(
        GgapRaft::new(
            1,
            raft_cfg.clone(),
            GgapNetworkFactory::new(0),
            log_store,
            sm,
        )
        .await
        .unwrap(),
    );

    // Initialize as single-node cluster.
    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members).await.unwrap();

    // Wait for leader.
    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "become leader")
        .await
        .unwrap();

    let router = Arc::new(ShardRouter::new(shard_map.clone()));
    let node = Arc::new(OpenRaftNode::new(
        raft.clone(),
        fsm.clone(),
        0,
        1,
        tokio::time::Duration::from_millis(100),
    ));
    let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));
    router.add_shard(0, node, cluster).await;

    // Spawn background split handler: creates new shard Raft instances on split events.
    tokio::spawn(run_split_handler(
        split_rx,
        store.clone(),
        fsm.clone(),
        router.clone(),
        1, // node_id
        raft_cfg,
    ));

    let split_coordinator = Arc::new(SplitCoordinator::new(SplitCoordinatorConfig {
        router: router.clone(),
        shard_map: shard_map.clone(),
    }));

    TestSetup {
        router,
        shard_map,
        split_coordinator,
        fsm,
        raft,
        _tempdir: tempdir,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_partitions_data_correctly() {
    let s = setup().await;

    // Write keys across the alphabet.
    let keys = ["apple", "banana", "cherry", "mango", "orange", "zebra"];
    for key in &keys {
        let resp = s
            .raft
            .client_write(KvCommand::Put {
                key: key.to_string(),
                value: format!("val-{key}").into_bytes(),
                ttl_ns: None,
                expect_version: 0,
            })
            .await
            .unwrap();
        assert!(
            matches!(resp.data, KvResponse::Written { .. }),
            "failed to write {key}"
        );
    }

    // Verify all 6 keys are in shard 0.
    assert_eq!(s.shard_map.all_shards().await.len(), 1);

    // Split shard 0 at "m" → shard 0: ["", "m"), shard 1: ["m", "")
    let new_shard_id = s.split_coordinator.split(0, "m").await.unwrap();
    assert_eq!(new_shard_id, 1);

    // Verify ShardMap has 2 shards now.
    let shards = s.shard_map.all_shards().await;
    assert_eq!(shards.len(), 2);

    let shard0 = s.shard_map.get_shard(0).await.unwrap();
    assert_eq!(shard0.range.start, "");
    assert_eq!(shard0.range.end, "m");

    let shard1 = s.shard_map.get_shard(1).await.unwrap();
    assert_eq!(shard1.range.start, "m");
    assert_eq!(shard1.range.end, "");

    // Verify routing: keys < "m" route to shard 0.
    for key in &["apple", "banana", "cherry"] {
        let node = s.router.route_read(key).await.unwrap();
        assert_eq!(node.shard_id(), 0, "key '{key}' should route to shard 0");
    }

    // Verify routing: keys >= "m" route to shard 1.
    for key in &["mango", "orange", "zebra"] {
        let node = s.router.route_read(key).await.unwrap();
        assert_eq!(node.shard_id(), 1, "key '{key}' should route to shard 1");
    }

    // Verify data: keys in shard 0's storage.
    for key in &["apple", "banana", "cherry"] {
        let entry = s.fsm.get(0, key, 0).await.unwrap();
        assert!(
            entry.is_some(),
            "key '{key}' should exist in shard 0 storage"
        );
    }
    // Keys >= "m" should NOT be in shard 0's storage anymore.
    for key in &["mango", "orange", "zebra"] {
        let entry = s.fsm.get(0, key, 0).await.unwrap();
        assert!(
            entry.is_none(),
            "key '{key}' should NOT exist in shard 0 storage"
        );
    }

    // Verify data: keys in shard 1's storage.
    for key in &["mango", "orange", "zebra"] {
        let entry = s.fsm.get(1, key, 0).await.unwrap();
        assert!(
            entry.is_some(),
            "key '{key}' should exist in shard 1 storage"
        );
        let e = entry.unwrap();
        assert_eq!(e.value, format!("val-{key}").into_bytes());
    }
    // Keys < "m" should NOT be in shard 1's storage.
    for key in &["apple", "banana", "cherry"] {
        let entry = s.fsm.get(1, key, 0).await.unwrap();
        assert!(
            entry.is_none(),
            "key '{key}' should NOT exist in shard 1 storage"
        );
    }

    s.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_after_split_route_correctly() {
    let s = setup().await;

    // Write initial data.
    s.raft
        .client_write(KvCommand::Put {
            key: "alpha".into(),
            value: b"1".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();
    s.raft
        .client_write(KvCommand::Put {
            key: "zulu".into(),
            value: b"2".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();

    // Split at "m".
    s.split_coordinator.split(0, "m").await.unwrap();

    // Write new keys through the router (simulating gRPC path).
    let node_d = s.router.route_write("delta").await.unwrap();
    assert_eq!(node_d.shard_id(), 0);
    let resp = node_d
        .propose(
            KvCommand::Put {
                key: "delta".into(),
                value: b"new-lower".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            },
            WriteMode::Majority,
        )
        .await
        .unwrap();
    assert!(matches!(resp, KvResponse::Written { .. }));

    let node_p = s.router.route_write("papa").await.unwrap();
    assert_eq!(node_p.shard_id(), 1);
    let resp = node_p
        .propose(
            KvCommand::Put {
                key: "papa".into(),
                value: b"new-upper".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            },
            WriteMode::Majority,
        )
        .await
        .unwrap();
    assert!(matches!(resp, KvResponse::Written { .. }));

    // Verify: "delta" in shard 0, "papa" in shard 1.
    let delta = s.fsm.get(0, "delta", 0).await.unwrap().unwrap();
    assert_eq!(delta.value, b"new-lower");

    let papa = s.fsm.get(1, "papa", 0).await.unwrap().unwrap();
    assert_eq!(papa.value, b"new-upper");

    s.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_rejects_invalid_key() {
    let s = setup().await;

    // Split with empty key should fail.
    let err = s.split_coordinator.split(0, "").await;
    assert!(err.is_err());

    // Split non-existent shard should fail.
    let err = s.split_coordinator.split(99, "m").await;
    assert!(err.is_err());

    s.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn double_split_works() {
    let s = setup().await;

    // Write keys.
    for key in ["a", "d", "g", "m", "p", "z"] {
        s.raft
            .client_write(KvCommand::Put {
                key: key.into(),
                value: key.as_bytes().to_vec(),
                ttl_ns: None,
                expect_version: 0,
            })
            .await
            .unwrap();
    }

    // First split at "m": shard 0 = ["", "m"), shard 1 = ["m", "")
    let shard1 = s.split_coordinator.split(0, "m").await.unwrap();
    assert_eq!(shard1, 1);

    // Second split at "d" on shard 0: shard 0 = ["", "d"), shard 2 = ["d", "m")
    let shard2 = s.split_coordinator.split(0, "d").await.unwrap();
    assert_eq!(shard2, 2);

    // Verify 3 shards.
    assert_eq!(s.shard_map.all_shards().await.len(), 3);

    // Check routing.
    assert_eq!(s.router.route_read("a").await.unwrap().shard_id(), 0);
    assert_eq!(s.router.route_read("d").await.unwrap().shard_id(), 2);
    assert_eq!(s.router.route_read("g").await.unwrap().shard_id(), 2);
    assert_eq!(s.router.route_read("m").await.unwrap().shard_id(), 1);
    assert_eq!(s.router.route_read("z").await.unwrap().shard_id(), 1);

    // Check data in correct shards.
    assert!(s.fsm.get(0, "a", 0).await.unwrap().is_some());
    assert!(s.fsm.get(0, "d", 0).await.unwrap().is_none()); // moved to shard 2
    assert!(s.fsm.get(2, "d", 0).await.unwrap().is_some());
    assert!(s.fsm.get(2, "g", 0).await.unwrap().is_some());
    assert!(s.fsm.get(1, "m", 0).await.unwrap().is_some());
    assert!(s.fsm.get(1, "z", 0).await.unwrap().is_some());

    s.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_within_shard_works_after_split() {
    let s = setup().await;

    // Write some keys.
    for key in ["a", "b", "c", "x", "y", "z"] {
        s.raft
            .client_write(KvCommand::Put {
                key: key.into(),
                value: key.as_bytes().to_vec(),
                ttl_ns: None,
                expect_version: 0,
            })
            .await
            .unwrap();
    }

    // Split at "m".
    s.split_coordinator.split(0, "m").await.unwrap();

    // Scan within shard 0 (keys < "m").
    let node0 = s.router.route_scan("a", "m").await.unwrap();
    assert_eq!(node0.shard_id(), 0);
    let (entries, _) = node0.scan("a", "m", 100, ReadMode::Eventual).await.unwrap();
    let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, vec!["a", "b", "c"]);

    // Scan within shard 1 (keys >= "m").
    let node1 = s.router.route_scan("m", "").await.unwrap();
    assert_eq!(node1.shard_id(), 1);
    let (entries, _) = node1.scan("m", "", 100, ReadMode::Eventual).await.unwrap();
    let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, vec!["x", "y", "z"]);

    s.raft.shutdown().await.unwrap();
}
