//! Three-node integration test.
//!
//! Starts a real Raft cluster entirely in-process using loopback gRPC.
//! Each node binds to 127.0.0.1:0 so the OS picks a free port; the actual
//! address is known before `raft.initialize()` is called, avoiding any
//! listen-vs-init race.
//!
//! Run with:
//!   cargo test -p ggap-server --test three_node_cluster -- --nocapture
//!
//! Benchmark with (once a bench harness is added):
//!   cargo bench -p ggap-server

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, ServerState};
use tempfile::TempDir;
use tokio::net::TcpListener;

use ggap_consensus::{
    build_raft_config, GgapLogStorage, GgapNetworkFactory, GgapRaft, GgapStateMachine,
    OpenRaftCluster, OpenRaftNode, RaftNode,
};
use ggap_server::{serve_client_with_listener, serve_cluster_with_listener};
use ggap_storage::fjall::{FjallStateMachine, FjallStore};
use ggap_storage::traits::StateMachineStore;
use ggap_types::{KvCommand, KvResponse, ReadMode};

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
    let fsm = Arc::new(FjallStateMachine::new(store.clone()));
    let log_store = GgapLogStorage::new(store.clone(), 0);
    let sm = GgapStateMachine::new(fsm.clone(), 0);
    // Fast timeouts so tests finish quickly.
    let cfg = build_raft_config(50, 150, 300);
    let raft = Arc::new(
        GgapRaft::new(id, cfg, GgapNetworkFactory, log_store, sm)
            .await
            .unwrap_or_else(|e| panic!("node {id}: raft init failed: {e}")),
    );

    // Pre-bind on port 0 → OS picks a free port we can pass to BasicNode.
    let cluster_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster_addr = cluster_listener.local_addr().unwrap();
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));
    let raft_node = Arc::new(OpenRaftNode::new(raft.clone(), fsm.clone(), 0, id));

    let mut handles = Vec::new();

    let c = cluster.clone();
    handles.push(tokio::spawn(async move {
        if let Err(e) = serve_cluster_with_listener(cluster_listener, c).await {
            // Server exits when the listener is closed — log only genuine errors.
            eprintln!("node {id} cluster server: {e}");
        }
    }));

    let n = raft_node.clone();
    handles.push(tokio::spawn(async move {
        if let Err(e) = serve_client_with_listener(client_listener, n, id).await {
            eprintln!("node {id} client server: {e}");
        }
    }));

    TestNode { id, raft, fsm, cluster_addr, raft_node, _handles: handles, _tempdir: tempdir }
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
            .map(|n| (n.id, BasicNode { addr: n.cluster_addr.to_string() }))
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
            node.raft.shutdown().await.unwrap_or_else(|e| eprintln!("shutdown: {e}"));
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
    assert!(matches!(resp.data, KvResponse::Written { .. }), "got {:?}", resp.data);

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
    // their FSMs.  Lease-based linearizable reads from followers are Phase 5;
    // for now read directly from the FSM after confirmed convergence.
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
    assert!(matches!(resp.data, KvResponse::Written { .. }), "got {:?}", resp.data);
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
