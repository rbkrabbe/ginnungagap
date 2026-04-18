//! Three-node integration test.
//!
//! Starts a real Raft cluster entirely in-process using loopback gRPC.
//! Each node binds to 127.0.0.1:0 so the OS picks a free port; the actual
//! address is known before `raft.initialize()` is called, avoiding any
//! listen-vs-init race.
//!
//! Run with:
//!   cargo test -p ggap-server --test three_node_cluster -- --nocapture

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, ServerState};
use tempfile::TempDir;
use tokio::net::TcpListener;

use ggap_consensus::{
    build_raft_config, run_split_handler, GgapLogStorage, GgapNetworkFactory, GgapRaft,
    GgapStateMachine, OpenRaftCluster, OpenRaftNode, RaftNode, ShardRouter, SplitCoordinator,
    SplitCoordinatorConfig,
};
use ggap_server::{serve_client_with_listener, serve_cluster_with_listener, KvServiceConfig};
use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::traits::StateMachineStore;
use ggap_storage::ShardMap;
use ggap_types::{GgapError, KvCommand, KvResponse, ReadMode, WriteMode};

// ---------------------------------------------------------------------------
// TestNode — a single in-process Raft node with gRPC servers running
// ---------------------------------------------------------------------------

struct TestNode {
    id: u64,
    raft: Arc<GgapRaft>,
    fsm: Arc<FjallStateMachine>,
    cluster_addr: SocketAddr,
    raft_node: Arc<OpenRaftNode>,
    // Kept alive so the servers stay running; aborted on drop via TestCluster::shutdown.
    _handles: Vec<tokio::task::JoinHandle<()>>,
    // Kept alive so the tempdir is not deleted while the node is running.
    _tempdir: TempDir,
}

async fn start_node(id: u64) -> TestNode {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();

    // ShardMap created before FSM so we can inject it.
    let shard_map = Arc::new(ShardMap::load(store.clone()).unwrap());
    shard_map.initialize_default().await.unwrap();

    // Split channel: state machine signals background handler on apply.
    let (split_tx, split_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut fsm_builder = FjallStateMachine::new(store.clone());
    fsm_builder.set_split_sender(split_tx);
    fsm_builder.set_shard_map(shard_map.clone());
    let fsm = Arc::new(fsm_builder);

    let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), 0);
    let sm = GgapStateMachine::new(fsm.clone(), 0);
    // Fast timeouts so tests finish quickly.
    let raft_cfg = build_raft_config(50, 150, 300, 500);
    let raft = Arc::new(
        GgapRaft::new(
            id,
            raft_cfg.clone(),
            GgapNetworkFactory::new(0),
            log_store,
            sm,
        )
        .await
        .unwrap_or_else(|e| panic!("node {id}: raft init failed: {e}")),
    );

    // Pre-bind on port 0 → OS picks a free port we can pass to BasicNode.
    let cluster_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster_addr = cluster_listener.local_addr().unwrap();
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let raft_node = Arc::new(OpenRaftNode::new(
        raft.clone(),
        fsm.clone(),
        0,
        id,
        tokio::time::Duration::from_millis(100),
    ));
    let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));

    // Build a single-shard router for this node.
    let router = Arc::new(ShardRouter::new(shard_map.clone()));
    router.add_shard(0, raft_node.clone(), cluster).await;

    // Spawn background split handler.
    tokio::spawn(run_split_handler(
        split_rx,
        store.clone(),
        fsm.clone(),
        router.clone(),
        id,
        raft_cfg,
    ));

    let split_coordinator = Arc::new(SplitCoordinator::new(SplitCoordinatorConfig {
        router: router.clone(),
        shard_map: shard_map.clone(),
    }));

    let mut handles = Vec::new();

    let r = router.clone();
    let sc = split_coordinator.clone();
    let sm2 = shard_map.clone();
    handles.push(tokio::spawn(async move {
        if let Err(e) = serve_cluster_with_listener(cluster_listener, r, sc, sm2).await {
            eprintln!("node {id} cluster server: {e}");
        }
    }));

    let r = router.clone();
    handles.push(tokio::spawn(async move {
        if let Err(e) =
            serve_client_with_listener(client_listener, r, id, KvServiceConfig::default()).await
        {
            eprintln!("node {id} client server: {e}");
        }
    }));

    TestNode {
        id,
        raft,
        fsm,
        cluster_addr,
        raft_node,
        _handles: handles,
        _tempdir: tempdir,
    }
}

// ---------------------------------------------------------------------------
// TestCluster — lifecycle helper
// ---------------------------------------------------------------------------

struct TestCluster {
    nodes: Vec<TestNode>,
}

impl TestCluster {
    /// Start `count` nodes and initialise them as a single Raft cluster.
    async fn start(count: usize) -> Self {
        let mut nodes = Vec::with_capacity(count);
        for id in 1..=(count as u64) {
            nodes.push(start_node(id).await);
        }

        // Build the full member map with each node's cluster address.
        let members: BTreeMap<u64, BasicNode> = nodes
            .iter()
            .map(|n| {
                (
                    n.id,
                    BasicNode {
                        addr: n.cluster_addr.to_string(),
                    },
                )
            })
            .collect();

        // Only one node calls initialize(); the others learn about the cluster
        // through the consensus protocol.
        nodes[0]
            .raft
            .initialize(members)
            .await
            .unwrap_or_else(|e| panic!("cluster init failed: {e}"));

        Self { nodes }
    }

    /// Block until one node reports `ServerState::Leader`; returns its index.
    async fn wait_for_leader(&self) -> usize {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            for (i, node) in self.nodes.iter().enumerate() {
                if node.raft.metrics().borrow().state == ServerState::Leader {
                    return i;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no leader elected within 10 s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until every node has applied at least `min_index` log entries.
    async fn wait_for_all_applied(&self, min_index: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        'outer: loop {
            let all_caught_up = self.nodes.iter().all(|n| {
                n.raft
                    .metrics()
                    .borrow()
                    .last_applied
                    .map(|id| id.index >= min_index)
                    .unwrap_or(false)
            });
            if all_caught_up {
                break 'outer;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "not all nodes applied index {min_index} within 10 s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Gracefully shutdown all nodes and abort the server tasks.
    async fn shutdown(self) {
        for node in self.nodes {
            node.raft
                .shutdown()
                .await
                .unwrap_or_else(|e| eprintln!("shutdown: {e}"));
            for h in node._handles {
                h.abort();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verifies leader election, a write, and linearizable reads from all nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_leader_election_and_basic_ops() {
    let cluster = TestCluster::start(3).await;

    let leader_idx = cluster.wait_for_leader().await;
    let leader = &cluster.nodes[leader_idx];

    // Write via the leader.
    let resp = leader
        .raft
        .client_write(KvCommand::Put {
            key: "hello".into(),
            value: b"world".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();
    let applied_index = resp.log_id().index;
    assert!(
        matches!(resp.data, KvResponse::Written { .. }),
        "got {:?}",
        resp.data
    );

    // Wait for all followers to catch up.
    cluster.wait_for_all_applied(applied_index).await;

    // Linearizable read from the leader — exercises ensure_linearizable().
    let entry = leader
        .raft_node
        .read("hello", 0, ReadMode::Linearizable)
        .await
        .unwrap()
        .expect("leader returned None for 'hello'");
    assert_eq!(entry.value, b"world");

    // Verify replication: all nodes (including followers) carry the data in
    // their FSMs.
    for node in &cluster.nodes {
        let entry = node
            .fsm
            .get(0, "hello", 0)
            .await
            .unwrap_or_else(|e| panic!("node {} fsm.get failed: {e}", node.id))
            .unwrap_or_else(|| panic!("node {} FSM missing 'hello'", node.id));
        assert_eq!(entry.value, b"world", "node {} value mismatch", node.id);
    }

    cluster.shutdown().await;
}

/// Verifies that after the leader is shut down, a new leader is elected and
/// the cluster continues to accept writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_leader_failover() {
    let mut cluster = TestCluster::start(3).await;

    let leader_idx = cluster.wait_for_leader().await;

    // Write something before the failover.
    let resp = cluster.nodes[leader_idx]
        .raft
        .client_write(KvCommand::Put {
            key: "pre".into(),
            value: b"failover".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();
    let pre_index = resp.log_id().index;
    cluster.wait_for_all_applied(pre_index).await;

    // Shut down the current leader.
    let old_leader = cluster.nodes.remove(leader_idx);
    old_leader.raft.shutdown().await.unwrap();
    for h in old_leader._handles {
        h.abort();
    }
    drop(old_leader._tempdir);

    // A new leader should emerge from the remaining two nodes.
    let new_leader_idx = cluster.wait_for_leader().await;
    let new_leader = &cluster.nodes[new_leader_idx];

    // Write via the new leader.
    let resp = new_leader
        .raft
        .client_write(KvCommand::Put {
            key: "post".into(),
            value: b"elected".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();
    assert!(
        matches!(resp.data, KvResponse::Written { .. }),
        "got {:?}",
        resp.data
    );
    let post_index = resp.log_id().index;
    cluster.wait_for_all_applied(post_index).await;

    // Both surviving nodes should see both keys via their FSMs.
    for node in &cluster.nodes {
        let pre = node
            .fsm
            .get(0, "pre", 0)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("node {} FSM missing 'pre'", node.id));
        assert_eq!(pre.value, b"failover");

        let post = node
            .fsm
            .get(0, "post", 0)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("node {} FSM missing 'post'", node.id));
        assert_eq!(post.value, b"elected");
    }

    cluster.shutdown().await;
}

/// Verifies that a follower can serve a sequential read after the leader has
/// replicated the entry to all nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_read_from_follower() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;

    let resp = cluster.nodes[leader_idx]
        .raft
        .client_write(KvCommand::Put {
            key: "seq_key".into(),
            value: b"seq_val".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();
    let applied_index = resp.log_id().index;
    cluster.wait_for_all_applied(applied_index).await;

    // Pick a follower (first node that is not the leader).
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let entry = cluster.nodes[follower_idx]
        .raft_node
        .read("seq_key", 0, ReadMode::Sequential)
        .await
        .unwrap()
        .expect("follower missing seq_key after replication");
    assert_eq!(entry.value, b"seq_val");

    cluster.shutdown().await;
}

/// Verifies that every node can serve an eventual read after replication.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventual_read_from_any_node() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;

    let resp = cluster.nodes[leader_idx]
        .raft
        .client_write(KvCommand::Put {
            key: "evt_key".into(),
            value: b"evt_val".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();
    cluster.wait_for_all_applied(resp.log_id().index).await;

    for node in &cluster.nodes {
        let entry = node
            .raft_node
            .read("evt_key", 0, ReadMode::Eventual)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("node {} missing evt_key", node.id));
        assert_eq!(entry.value, b"evt_val", "node {} value mismatch", node.id);
    }

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Admin operation tests
// ---------------------------------------------------------------------------

/// Verifies that `cluster_status` returns correct term, leader, and membership
/// after election in a 3-node cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_status_reflects_elected_leader_and_membership() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;
    let leader = &cluster.nodes[leader_idx];

    let status = leader.raft_node.cluster_status();

    // After election the term must be at least 1.
    assert!(status.term >= 1, "term should be >= 1, got {}", status.term);

    // The leader should report itself.
    assert_eq!(
        status.leader_id,
        Some(leader.id),
        "leader_id should be {}, got {:?}",
        leader.id,
        status.leader_id
    );

    // Membership must contain all 3 original voters.
    let voter_ids: BTreeSet<u64> = status.voters.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        voter_ids,
        BTreeSet::from([1, 2, 3]),
        "expected voters {{1,2,3}}, got {voter_ids:?}"
    );

    // Each member should have a non-empty cluster address.
    for (nid, addr) in &status.voters {
        assert!(
            !addr.is_empty(),
            "node {nid} has empty cluster addr in membership"
        );
    }

    // No learners in a fresh 3-node cluster.
    assert!(
        status.learners.is_empty(),
        "expected no learners, got {:?}",
        status.learners
    );

    // last_applied should be > 0 (at least the initial membership log entry).
    assert!(
        status.last_applied > 0,
        "last_applied should be > 0, got {}",
        status.last_applied
    );

    // Follower should also return cluster_status (may have slightly stale data
    // but membership and term should still be valid).
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let f_status = cluster.nodes[follower_idx].raft_node.cluster_status();
    let f_voter_ids: BTreeSet<u64> = f_status.voters.iter().map(|(id, _)| *id).collect();
    assert!(
        f_status.term >= 1,
        "follower term should be >= 1, got {}",
        f_status.term
    );
    assert_eq!(
        f_voter_ids,
        BTreeSet::from([1, 2, 3]),
        "follower sees wrong voters: {f_voter_ids:?}"
    );

    cluster.shutdown().await;
}

/// Verifies that `add_learner` on the leader succeeds and the learner appears
/// in the Raft membership. The learner node does not need to be running for
/// the membership change to commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_learner_updates_membership() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;
    let leader = &cluster.nodes[leader_idx];

    // Add node 99 as a learner (no actual node running — that's fine,
    // the membership change still commits through Raft).
    leader
        .raft_node
        .add_learner(99, "127.0.0.1:19999".to_string())
        .await
        .expect("add_learner should succeed on leader");

    // Poll until node 99 appears in membership (replaces fragile sleep).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let metrics = leader.raft.metrics().borrow().clone();
        let all_node_ids: BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(nid, _)| *nid)
            .collect();
        if all_node_ids.contains(&99) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for node 99 in membership, got {all_node_ids:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Verify via cluster_status that node 99 is a learner, not a voter.
    let status = leader.raft_node.cluster_status();
    let voter_ids: BTreeSet<u64> = status.voters.iter().map(|(id, _)| *id).collect();
    let learner_ids: BTreeSet<u64> = status.learners.iter().map(|(id, _)| *id).collect();
    assert!(
        learner_ids.contains(&99),
        "node 99 should appear in learners, got {learner_ids:?}"
    );
    assert!(
        !voter_ids.contains(&99),
        "node 99 should not be a voter, voters = {voter_ids:?}"
    );

    // Original 3 voters should still be intact.
    assert_eq!(
        voter_ids,
        BTreeSet::from([1, 2, 3]),
        "original voters should be unchanged"
    );

    cluster.shutdown().await;
}

/// Verifies that `add_learner` called on a follower returns `NotLeader`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_learner_on_follower_returns_not_leader() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;

    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let err = cluster.nodes[follower_idx]
        .raft_node
        .add_learner(99, "127.0.0.1:19999".to_string())
        .await
        .expect_err("add_learner on follower should fail");

    assert!(
        matches!(err, GgapError::NotLeader { .. }),
        "expected NotLeader, got {err:?}"
    );

    // The error should carry a leader hint.
    if let GgapError::NotLeader { leader } = &err {
        assert!(
            leader.is_some(),
            "NotLeader error should include a leader address hint"
        );
    }

    cluster.shutdown().await;
}

/// Verifies that `change_membership` correctly shrinks the voter set and that
/// `retain=true` demotes the removed voter to a learner instead of ejecting it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_membership_demotes_removed_voter_to_learner() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;
    let leader = &cluster.nodes[leader_idx];

    // Write some data before the membership change.
    let resp = leader
        .raft
        .client_write(KvCommand::Put {
            key: "before_change".into(),
            value: b"v1".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();
    cluster.wait_for_all_applied(resp.log_id().index).await;

    // Pick a non-leader node to remove from voters.
    let removed_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let removed_id = cluster.nodes[removed_idx].id;
    let remaining_ids: BTreeSet<u64> = cluster
        .nodes
        .iter()
        .map(|n| n.id)
        .filter(|&id| id != removed_id)
        .collect();

    // Change membership to exclude the removed node.
    leader
        .raft_node
        .change_membership(remaining_ids.clone())
        .await
        .expect("change_membership should succeed");

    // Poll until voter set matches the expected remaining nodes.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let poll_status = leader.raft_node.cluster_status();
        let current_voter_ids: BTreeSet<u64> =
            poll_status.voters.iter().map(|(id, _)| *id).collect();
        if current_voter_ids == remaining_ids {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for voter set {remaining_ids:?}, got {current_voter_ids:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Verify the removed node is now a learner (retain=true) via cluster_status.
    let status = leader.raft_node.cluster_status();
    let new_voter_ids: BTreeSet<u64> = status.voters.iter().map(|(id, _)| *id).collect();
    let learner_ids: BTreeSet<u64> = status.learners.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        new_voter_ids, remaining_ids,
        "voter set should be {remaining_ids:?}, got {new_voter_ids:?}"
    );
    assert!(
        learner_ids.contains(&removed_id),
        "node {removed_id} should be a learner, but learners = {learner_ids:?}"
    );

    // Verify writes still work under the new 2-voter config.
    let resp = leader
        .raft_node
        .propose(
            KvCommand::Put {
                key: "after_change".into(),
                value: b"v2".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            },
            WriteMode::Majority,
        )
        .await
        .expect("write should succeed with 2-voter config");
    assert!(
        matches!(resp, KvResponse::Written { .. }),
        "expected Written, got {resp:?}"
    );

    cluster.shutdown().await;
}

/// Verifies that `change_membership` on a follower returns `NotLeader`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_membership_on_follower_returns_not_leader() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;

    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let err = cluster.nodes[follower_idx]
        .raft_node
        .change_membership(BTreeSet::from([1, 2]))
        .await
        .expect_err("change_membership on follower should fail");

    assert!(
        matches!(err, GgapError::NotLeader { .. }),
        "expected NotLeader, got {err:?}"
    );

    cluster.shutdown().await;
}
