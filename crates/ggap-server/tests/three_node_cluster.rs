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

use openraft::ServerState;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use ggap_consensus::{
    build_raft_config, run_split_handler, GgapLogStorage, GgapNetworkFactory, GgapNode, GgapRaft,
    GgapStateMachine, GossipTask, OpenRaftCluster, OpenRaftNode, RaftNode, ShardRegistry,
    ShardRouter, SplitCoordinator, SplitCoordinatorConfig,
};
use ggap_server::{
    serve_client_with_listener, serve_cluster_with_listener, AdminServiceForTesting,
    KvServiceConfig,
};
use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::traits::StateMachineStore;
use ggap_storage::ShardMap;
use ggap_types::{GgapError, KvCommand, KvResponse, NodeAddrs, ReadMode, WriteMode};

use ggap_proto::v1::admin_service_server::AdminService;
use ggap_proto::v1::{
    AddLearnerRequest, ClusterStatusRequest, ListShardsRequest, NodeInfo, ShardInfoProto,
};

// ---------------------------------------------------------------------------
// TestNode — a single in-process Raft node with gRPC servers running
// ---------------------------------------------------------------------------

struct TestNode {
    id: u64,
    raft: Arc<GgapRaft>,
    fsm: Arc<FjallStateMachine>,
    cluster_addr: SocketAddr,
    /// The address the client listener is actually bound to. The directory
    /// entry other nodes learn about this node must resolve to this.
    client_addr: SocketAddr,
    /// The advertised form of `client_addr` — what goes into Raft membership
    /// and, derived from it, the directory.
    advertised_client_addr: String,
    registry: Arc<ShardRegistry>,
    raft_node: Arc<OpenRaftNode>,
    /// In-process AdminService over the same router and registry the node's
    /// cluster server exposes, so tests can call admin RPCs without a client.
    admin: AdminServiceForTesting,
    // Kept alive so the servers stay running; aborted on drop via TestCluster::shutdown.
    _handles: Vec<tokio::task::JoinHandle<()>>,
    // Kept alive so the tempdir is not deleted while the node is running.
    _tempdir: TempDir,
}

/// `gossip = false` starts every server but the gossip task, so only Raft can
/// reach the directory. Tests asserting membership is the source of truth for
/// an address need that: with gossip running, the assertion passes either way.
async fn start_node(id: u64, gossip: bool) -> TestNode {
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

    // Pre-bind on port 0 → OS picks a free port we can pass to GgapNode.
    let cluster_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster_addr = cluster_listener.local_addr().unwrap();
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();

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

    // Cluster registry + fast gossip task (short interval for snappy tests).
    // Both listeners bind 127.0.0.1 on an ephemeral port, so the bind address is
    // also the advertised one.
    let self_client_addr = client_addr.to_string();
    let registry = Arc::new(ShardRegistry::new(id, []));

    let mut handles = Vec::new();

    if gossip {
        handles.push(tokio::spawn(
            GossipTask::new(
                router.clone(),
                registry.clone(),
                id,
                cluster_addr.to_string(),
                CancellationToken::new(),
            )
            .with_interval(Duration::from_millis(50))
            .with_rpc_timeout(Duration::from_secs(1))
            .run(),
        ));
    }

    let r = router.clone();
    let sc = split_coordinator.clone();
    let sm2 = shard_map.clone();
    let reg = registry.clone();
    handles.push(tokio::spawn(async move {
        if let Err(e) = serve_cluster_with_listener(cluster_listener, r, sc, sm2, reg, vec![]).await
        {
            eprintln!("node {id} cluster server: {e}");
        }
    }));

    let r = router.clone();
    handles.push(tokio::spawn(async move {
        if let Err(e) =
            serve_client_with_listener(client_listener, r, id, KvServiceConfig::default(), vec![])
                .await
        {
            eprintln!("node {id} client server: {e}");
        }
    }));

    TestNode {
        id,
        raft,
        fsm,
        cluster_addr,
        client_addr,
        advertised_client_addr: self_client_addr,
        admin: AdminServiceForTesting::new(
            router.clone(),
            split_coordinator.clone(),
            shard_map.clone(),
            registry.clone(),
        ),
        registry,
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
        Self::start_with_gossip(count, true).await
    }

    /// As [`TestCluster::start`], but optionally without any gossip task
    /// running. See [`start_node`].
    async fn start_with_gossip(count: usize, gossip: bool) -> Self {
        let mut nodes = Vec::with_capacity(count);
        for id in 1..=(count as u64) {
            nodes.push(start_node(id, gossip).await);
        }

        // Build the full member map with both of each node's addresses, exactly
        // as `ggap-node`'s seed bootstrap does.
        let members: BTreeMap<u64, GgapNode> = nodes
            .iter()
            .map(|n| {
                (
                    n.id,
                    GgapNode::from(NodeAddrs::new(
                        n.cluster_addr.to_string(),
                        n.advertised_client_addr.clone(),
                    )),
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

    /// Block until one node reports `ServerState::Leader` *and* every node
    /// has applied the initial bootstrap membership entry. The latter is
    /// required because openraft checks for an in-flight config change
    /// before checking leadership: without the extra wait, admin RPCs
    /// racing bootstrap see `Consensus("already undergoing a configuration
    /// change")` instead of the expected `NotLeader`, and `add_learner`
    /// calls on the leader fail outright. Returns the leader's index.
    async fn wait_for_leader(&self) -> usize {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let leader_idx = loop {
            let mut found = None;
            for (i, node) in self.nodes.iter().enumerate() {
                if node.raft.metrics().borrow().state == ServerState::Leader {
                    found = Some(i);
                    break;
                }
            }
            if let Some(i) = found {
                break i;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no leader elected within 10 s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        loop {
            let all_applied = self
                .nodes
                .iter()
                .all(|n| n.raft.metrics().borrow().last_applied.is_some());
            if all_applied {
                return leader_idx;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "initial membership not committed within 10 s"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
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

    // Each member should have both addresses in membership.
    for (nid, addrs) in &status.voters {
        assert!(
            !addrs.cluster_addr.is_empty(),
            "node {nid} has empty cluster addr in membership"
        );
        assert!(
            !addrs.client_addr.is_empty(),
            "node {nid} has empty client addr in membership"
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
        .add_learner(99, NodeAddrs::new("127.0.0.1:19999", "127.0.0.1:19998"))
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

/// Assert a `NotLeader` carries both halves of the hint, and that they agree.
///
/// The id is what a forwarder actually resolves through the directory,
/// so it must be present; the address is the fallback. Rather than pinning the
/// hint to whichever node was leader when the test started — the leader can
/// move at any time — this checks the pair is internally consistent: the node
/// named by `leader_id` is the one listening on `leader`.
fn assert_leader_hint_consistent(err: &GgapError, cluster: &TestCluster) {
    let GgapError::NotLeader { leader_id, leader } = err else {
        panic!("expected NotLeader, got {err:?}");
    };

    let id = leader_id.expect("NotLeader should include the leader's node id");
    let addr = leader
        .as_ref()
        .expect("NotLeader should include a leader address hint");

    let node = cluster
        .nodes
        .iter()
        .find(|n| n.id == id)
        .unwrap_or_else(|| panic!("leader_id {id} does not name any node in the cluster"));

    assert_eq!(
        node.cluster_addr.to_string(),
        *addr,
        "leader_id {id} and leader address disagree about which node is leader"
    );
}

/// Look up a node's entry in a `ClusterStatus` response, voter or learner.
fn node_info_for(resp: &ggap_proto::v1::ClusterStatusResponse, node_id: u64) -> Option<&NodeInfo> {
    resp.nodes
        .iter()
        .chain(resp.learners.iter())
        .find(|n| n.node_id == node_id)
}

/// The payoff assertion for this epic: with no gossip task running anywhere,
/// every node still reports every peer's *client* address. Membership is the
/// only path that address can have taken — a follower never called
/// `initialize()`, so it holds what Raft replicated to it and nothing else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_addr_comes_from_membership_without_gossip() {
    let cluster = TestCluster::start_with_gossip(3, false).await;
    let leader_idx = cluster.wait_for_leader().await;
    let follower = &cluster.nodes[(0..3).find(|&i| i != leader_idx).unwrap()];

    // Nothing seeds the directory and nothing refreshes it, so it is empty —
    // this node's own entry included. Every address in the response below has
    // to have come from membership.
    let (directory, _) = follower.registry.snapshot_for_gossip().await;
    assert!(
        directory.is_empty(),
        "node {} has directory entries with gossip stopped: {directory:?}",
        follower.id,
    );

    let resp = follower
        .admin
        .cluster_status(tonic::Request::new(ClusterStatusRequest {
            shard_id: Some(0),
        }))
        .await
        .expect("cluster_status should succeed")
        .into_inner();

    for peer in &cluster.nodes {
        let info = node_info_for(&resp, peer.id)
            .unwrap_or_else(|| panic!("node {} missing from cluster status", peer.id));
        assert_eq!(
            info.client_addr, peer.advertised_client_addr,
            "node {} reported the wrong client address for peer {}",
            follower.id, peer.id
        );
        assert_eq!(
            info.cluster_addr,
            peer.cluster_addr.to_string(),
            "node {} reported the wrong cluster address for peer {}",
            follower.id,
            peer.id
        );
    }

    cluster.shutdown().await;
}

/// A learner joined through the `AddLearner` RPC reaches committed membership
/// with both addresses, and every node sees both — again with gossip stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_learner_rpc_puts_both_addresses_in_membership() {
    let cluster = TestCluster::start_with_gossip(3, false).await;
    let leader_idx = cluster.wait_for_leader().await;
    let leader = &cluster.nodes[leader_idx];

    // Node 99 need not be running for the membership change to commit.
    let resp = leader
        .admin
        .add_learner(tonic::Request::new(AddLearnerRequest {
            shard_id: Some(0),
            node: Some(NodeInfo {
                node_id: 99,
                cluster_addr: "127.0.0.1:19999".to_string(),
                client_addr: "127.0.0.1:19998".to_string(),
            }),
        }))
        .await
        .expect("add_learner should succeed on leader")
        .into_inner();
    assert!(resp.ok, "add_learner failed: {}", resp.error);

    // Assert on a follower: it holds only what Raft replicated to it.
    let follower = &cluster.nodes[(0..3).find(|&i| i != leader_idx).unwrap()];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = follower
            .admin
            .cluster_status(tonic::Request::new(ClusterStatusRequest {
                shard_id: Some(0),
            }))
            .await
            .unwrap()
            .into_inner();
        if let Some(info) = node_info_for(&status, 99) {
            assert_eq!(info.cluster_addr, "127.0.0.1:19999");
            assert_eq!(
                info.client_addr, "127.0.0.1:19998",
                "learner's client address was dropped on the way through Raft"
            );
            assert!(
                status.learners.iter().any(|n| n.node_id == 99),
                "node 99 should be a learner, not a voter"
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node 99 never reached the follower's membership"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    cluster.shutdown().await;
}

/// An `AddLearner` with no client address is rejected outright.
///
/// Membership is now the source of truth for the client address, so admitting a
/// learner without one produces a member nothing can forward a client request
/// to, and nothing later fills the gap in. Failing the join is the only loud
/// moment available.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_learner_rpc_rejects_empty_client_addr() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;
    let leader = &cluster.nodes[leader_idx];

    let status = leader
        .admin
        .add_learner(tonic::Request::new(AddLearnerRequest {
            shard_id: Some(0),
            node: Some(NodeInfo {
                node_id: 99,
                cluster_addr: "127.0.0.1:19999".to_string(),
                client_addr: String::new(),
            }),
        }))
        .await
        .expect_err("empty client_addr must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("client_addr"),
        "message should name the offending field, got {:?}",
        status.message()
    );

    // Rejected before Raft: no membership change was proposed.
    let membership_ids: BTreeSet<u64> = leader
        .raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .nodes()
        .map(|(nid, _)| *nid)
        .collect();
    assert!(
        !membership_ids.contains(&99),
        "rejected learner reached membership: {membership_ids:?}"
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
        .add_learner(99, NodeAddrs::new("127.0.0.1:19999", "127.0.0.1:19998"))
        .await
        .expect_err("add_learner on follower should fail");

    // The error should carry a complete leader hint: id and address.
    assert_leader_hint_consistent(&err, &cluster);

    cluster.shutdown().await;
}

/// Verifies that a linearizable read served by a follower returns `NotLeader`
/// with a leader hint, not an opaque `Consensus` error.
///
/// Regression test: `ensure_linearizable_or_lease` used to map every openraft
/// error — including `ForwardToLeader` — to `GgapError::Consensus`, so the
/// forwarding hint never reached `ggap_to_status` and clients saw gRPC
/// `INTERNAL` instead of `UNAVAILABLE` plus `ggap-leader-addr`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_read_on_follower_returns_not_leader() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;

    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();
    let err = cluster.nodes[follower_idx]
        .raft_node
        .read("any_key", 0, ReadMode::Linearizable)
        .await
        .expect_err("linearizable read on follower should fail");

    // The error should carry a leader hint, which is the whole point: it is what
    // `ggap_to_status` turns into the `ggap-leader-id` / `ggap-leader-addr`
    // metadata.
    assert_leader_hint_consistent(&err, &cluster);

    // A linearizable read on the leader still succeeds — the forwarding check
    // must not have broken the happy path.
    cluster.nodes[leader_idx]
        .raft_node
        .read("any_key", 0, ReadMode::Linearizable)
        .await
        .expect("linearizable read on leader should succeed");

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

    assert_leader_hint_consistent(&err, &cluster);

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Gossip / shard-registry tests
// ---------------------------------------------------------------------------

/// A lightweight node that hosts no shard. It runs only a gossip task (dialing
/// a seed peer) and exposes an in-process AdminService backed by its registry,
/// so we can assert it learns remote shard status purely via gossip.
struct Observer {
    admin: AdminServiceForTesting,
    handle: tokio::task::JoinHandle<()>,
    _tempdir: TempDir,
}

async fn start_observer(id: u64, seed_id: u64, seed_addr: SocketAddr) -> Observer {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();
    // No initialize_default: this node's shard map is empty, so any shard it
    // reports must have been learned via gossip.
    let shard_map = Arc::new(ShardMap::load(store).unwrap());
    let router = Arc::new(ShardRouter::new(shard_map.clone()));
    let split_coordinator = Arc::new(SplitCoordinator::new(SplitCoordinatorConfig {
        router: router.clone(),
        shard_map: shard_map.clone(),
    }));
    // Self addr is a dummy (the observer serves nothing and never dials itself);
    // the seed gives it an entry point into the cluster.
    let self_addr = format!("127.0.0.1:1{id}");
    // The observer is in no shard's membership, so nothing puts it in any
    // directory — including its own. It reaches the cluster through the seed.
    let registry = Arc::new(ShardRegistry::new(id, [(seed_id, seed_addr.to_string())]));
    let handle = tokio::spawn(
        GossipTask::new(
            router.clone(),
            registry.clone(),
            id,
            self_addr,
            CancellationToken::new(),
        )
        .with_interval(Duration::from_millis(50))
        .with_rpc_timeout(Duration::from_secs(1))
        .run(),
    );
    let admin = AdminServiceForTesting::new(router, split_coordinator, shard_map, registry);
    Observer {
        admin,
        handle,
        _tempdir: tempdir,
    }
}

async fn list_shard(admin: &AdminServiceForTesting, shard_id: u64) -> Option<ShardInfoProto> {
    let resp = admin
        .list_shards(tonic::Request::new(ListShardsRequest {}))
        .await
        .unwrap()
        .into_inner();
    resp.shards.into_iter().find(|s| s.shard_id == shard_id)
}

/// A node that does not host a shard can still report that shard's correct
/// consensus state once gossip converges, and reports an explicit "no snapshot"
/// marker (age = None) for a shard it has never heard of — distinct from a real
/// leaderless/zero state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gossip_reports_remote_shard_status_from_non_hosting_node() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;
    let leader = &cluster.nodes[leader_idx];

    // A write so last_applied > 0 and there is real consensus state to gossip.
    let resp = leader
        .raft
        .client_write(KvCommand::Put {
            key: "k".into(),
            value: b"v".to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();
    cluster.wait_for_all_applied(resp.log_id().index).await;
    let expected = leader.raft_node.cluster_status();

    // Observer hosts no shard; seed it with the leader's cluster address.
    let observer = start_observer(99, leader.id, leader.cluster_addr).await;

    // Poll until the observer has learned shard 0 via gossip.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let info = loop {
        if let Some(info) = list_shard(&observer.admin, 0).await {
            if info.leader_id.is_some() && info.last_applied >= expected.last_applied {
                break info;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "observer did not learn shard 0 via gossip"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(
        info.leader_id,
        Some(leader.id),
        "observer should see the real leader"
    );
    assert_eq!(
        info.term, expected.term,
        "observer should see the real term"
    );
    let voters: BTreeSet<u64> = info.voters.iter().copied().collect();
    assert_eq!(
        voters,
        BTreeSet::from([1, 2, 3]),
        "observer should see all voters"
    );
    // Remote data carries a real (gossip-derived) age, not "no snapshot".
    assert!(
        info.last_observed_age_ms.is_some(),
        "remote shard must carry a snapshot age"
    );

    // ClusterStatus for the remote shard also works (not NotFound).
    let cs = observer
        .admin
        .cluster_status(tonic::Request::new(ClusterStatusRequest {
            shard_id: Some(0),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cs.leader_id, Some(leader.id));
    assert!(cs.last_observed_age_ms.is_some());

    // A shard the observer has never heard of: honest zeros + age = None,
    // distinguishable from a genuinely leaderless shard (which would carry an age).
    let unknown = observer
        .admin
        .cluster_status(tonic::Request::new(ClusterStatusRequest {
            shard_id: Some(12345),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unknown.leader_id, None);
    assert_eq!(unknown.term, 0);
    assert_eq!(
        unknown.last_observed_age_ms, None,
        "unknown shard must be distinguishable from leaderless"
    );

    observer.handle.abort();
    cluster.shutdown().await;
}

/// When every host of a shard becomes unreachable, a non-hosting node keeps
/// answering with the last-known consensus state and a growing age, rather than
/// failing the RPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gossip_degrades_to_stale_when_hosts_unreachable() {
    let cluster = TestCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;
    let leader_id = cluster.nodes[leader_idx].id;
    let leader_addr = cluster.nodes[leader_idx].cluster_addr;

    let observer = start_observer(99, leader_id, leader_addr).await;

    // Converge first.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(info) = list_shard(&observer.admin, 0).await {
            if info.last_observed_age_ms.is_some() && info.leader_id.is_some() {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "observer never converged on shard 0"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Take the whole cluster down — no host remains reachable.
    cluster.shutdown().await;

    // The observer must keep answering (Ok) with a growing age rather than
    // failing the RPC, and must not fabricate fresh state.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let info = list_shard(&observer.admin, 0)
            .await
            .expect("shard 0 should still be listed after hosts go down");
        assert!(
            info.last_observed_age_ms.is_some(),
            "stale entry should still carry an age"
        );
        if info.last_observed_age_ms.unwrap() >= 500 {
            // Still reports the last-known leader (stale, not fabricated/zeroed).
            assert_eq!(info.leader_id, Some(leader_id));
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "snapshot age never grew after all hosts went down"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    observer.handle.abort();
}

/// Every node learns every other node's *client* address.
///
/// Both addresses come from Raft membership, so within a shard no gossip is
/// needed at all — see `client_addr_comes_from_membership_without_gossip`.
/// Here the gossip task is running, and the assertion is repeated after several
/// more ticks: a refresh that re-derived the directory badly, or a gossiped copy
/// that overwrote a good entry with a worse one, would show up as a flap that a
/// single check immediately after convergence would miss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gossip_converges_on_every_node_client_addr() {
    let cluster = TestCluster::start(3).await;
    cluster.wait_for_leader().await;

    let expected: Vec<(u64, String)> = cluster
        .nodes
        .iter()
        .map(|n| (n.id, n.client_addr.to_string()))
        .collect();

    // Converge: every node knows every node's client address.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_known = true;
        for node in &cluster.nodes {
            for (peer_id, _) in &expected {
                if node.registry.client_addr(*peer_id).await.is_none() {
                    all_known = false;
                }
            }
        }
        if all_known {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "gossip never converged on all client addresses"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ~10 further gossip ticks at the 50ms test interval, so the
    // membership-derived refresh has run many times over.
    tokio::time::sleep(Duration::from_millis(500)).await;

    for node in &cluster.nodes {
        for (peer_id, peer_client_addr) in &expected {
            assert_eq!(
                node.registry.client_addr(*peer_id).await.as_ref(),
                Some(peer_client_addr),
                "node {} lost or mismatched node {peer_id}'s client address",
                node.id,
            );
        }
        // The cluster address must survive the same traffic — a merge bug in
        // either direction is a failure.
        for peer in &cluster.nodes {
            assert_eq!(
                node.registry.directory_addr(peer.id).await,
                Some(peer.cluster_addr.to_string()),
                "node {} lost node {}'s cluster address",
                node.id,
                peer.id,
            );
        }
    }

    cluster.shutdown().await;
}

/// A node's two addresses are distinct and independently reported: the client
/// address must not be the cluster address wearing a different accessor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_and_cluster_addrs_are_distinct_per_node() {
    let cluster = TestCluster::start(3).await;
    cluster.wait_for_leader().await;

    for node in &cluster.nodes {
        assert_ne!(
            node.client_addr, node.cluster_addr,
            "test harness should bind two different ports"
        );
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let probe = cluster.nodes[0].id;
    let target = &cluster.nodes[1];
    loop {
        if cluster.nodes[0]
            .registry
            .client_addr(target.id)
            .await
            .is_some()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {probe} never learned node {}'s client address",
            target.id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let learned_client = cluster.nodes[0].registry.client_addr(target.id).await;
    let learned_cluster = cluster.nodes[0].registry.directory_addr(target.id).await;
    assert_eq!(
        learned_client.as_deref(),
        Some(target.client_addr.to_string()).as_deref()
    );
    assert_eq!(
        learned_cluster.as_deref(),
        Some(target.cluster_addr.to_string()).as_deref()
    );
    assert_ne!(learned_client, learned_cluster);

    cluster.shutdown().await;
}
