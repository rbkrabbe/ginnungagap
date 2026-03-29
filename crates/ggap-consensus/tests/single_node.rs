use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ggap_consensus::{
    build_raft_config, GgapLogStorage, GgapNetworkFactory, GgapRaft, GgapStateMachine,
};
use ggap_storage::{
    fjall::{FjallStateMachine, FjallStore},
    traits::StateMachineStore,
};
use ggap_types::{KvCommand, KvResponse};
use openraft::{BasicNode, ServerState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_leader_write_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = FjallStore::open(dir.path()).unwrap();
    let fsm = Arc::new(FjallStateMachine::new(store.clone()));
    let log_store = GgapLogStorage::new(store.clone(), 0);
    let sm = GgapStateMachine::new(fsm.clone(), 0);
    let net = GgapNetworkFactory::new(0);
    let cfg = build_raft_config(50, 150, 300, 500);

    let raft = Arc::new(GgapRaft::new(1, cfg, net, log_store, sm).await.unwrap());

    // Initialize as a single-node cluster.
    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members).await.unwrap();

    // Wait for this node to become leader.
    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "become leader")
        .await
        .expect("node should become leader within 5s");

    // Write a key via client_write.
    let resp = raft
        .client_write(KvCommand::Put {
            key: "hello".into(),
            value: b"world".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();

    assert!(
        matches!(resp.data, KvResponse::Written { .. }),
        "expected Written response, got {:?}",
        resp.data
    );

    // Read back via the FSM directly (after linearizability is guaranteed by
    // the write above).
    let entry = fsm.get(0, "hello", 0).await.unwrap().unwrap();
    assert_eq!(entry.value, b"world");
    assert_eq!(entry.key, "hello");

    raft.shutdown().await.unwrap();
}
