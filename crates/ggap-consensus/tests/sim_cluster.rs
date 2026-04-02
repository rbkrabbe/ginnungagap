//! Phase 6 — Deterministic Simulation Tests
//!
//! Uses in-process openraft with `SimNetwork` (no TCP/gRPC), a seeded
//! `FaultController` for reproducible message drops/partitions, and
//! `tokio::time` pausing for deterministic time control.

#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use openraft::{
    error::{NetworkError, RPCError, RaftError, Unreachable},
    network::RPCOption,
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
    AnyError, BasicNode, ChangeMembers, RaftNetwork, RaftNetworkFactory, ServerState,
};
use rand::{rngs::StdRng, RngExt, SeedableRng};
use tempfile::TempDir;
use tokio::sync::{Mutex, RwLock};

use ggap_consensus::{
    build_raft_config, GgapLogStorage, GgapRaft, GgapStateMachine, GgapTypeConfig,
};
use ggap_storage::{
    fjall::{FjallLogStorage, FjallStateMachine, FjallStore},
    traits::StateMachineStore,
};
use ggap_types::KvCommand;

// ---------------------------------------------------------------------------
// FaultController
// ---------------------------------------------------------------------------

struct FaultController {
    /// Unidirectional blocked pairs stored symmetrically: `partition(a,b)` inserts
    /// both `(a,b)` and `(b,a)`.
    partitions: RwLock<HashSet<(u64, u64)>>,
    drop_rng: Mutex<StdRng>,
    drop_rate_ppm: AtomicU32,
}

impl FaultController {
    fn new(seed: u64) -> Arc<Self> {
        Arc::new(FaultController {
            partitions: RwLock::new(HashSet::new()),
            drop_rng: Mutex::new(StdRng::seed_from_u64(seed)),
            drop_rate_ppm: AtomicU32::new(0),
        })
    }

    async fn partition(&self, a: u64, b: u64) {
        let mut p = self.partitions.write().await;
        p.insert((a, b));
        p.insert((b, a));
    }

    async fn repair(&self, a: u64, b: u64) {
        let mut p = self.partitions.write().await;
        p.remove(&(a, b));
        p.remove(&(b, a));
    }

    fn set_drop_rate(&self, ppm: u32) {
        self.drop_rate_ppm.store(ppm, Ordering::Relaxed);
    }

    async fn should_drop(&self, from: u64, to: u64) -> bool {
        {
            let p = self.partitions.read().await;
            if p.contains(&(from, to)) {
                return true;
            }
        }
        let rate = self.drop_rate_ppm.load(Ordering::Relaxed);
        if rate == 0 {
            return false;
        }
        let mut rng = self.drop_rng.lock().await;
        let roll: u32 = rng.random_range(0..1_000_000);
        roll < rate
    }
}

// ---------------------------------------------------------------------------
// NodeRegistry
// ---------------------------------------------------------------------------

type NodeRegistry = Arc<RwLock<HashMap<u64, Arc<GgapRaft>>>>;

// ---------------------------------------------------------------------------
// SimNetworkFactory
// ---------------------------------------------------------------------------

struct SimNetworkFactory {
    from_id: u64,
    registry: NodeRegistry,
    fault: Arc<FaultController>,
}

impl RaftNetworkFactory<GgapTypeConfig> for SimNetworkFactory {
    type Network = SimNetwork;

    async fn new_client(&mut self, target_id: u64, _node: &BasicNode) -> SimNetwork {
        SimNetwork {
            from_id: self.from_id,
            target_id,
            registry: self.registry.clone(),
            fault: self.fault.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// SimNetwork
// ---------------------------------------------------------------------------

struct SimNetwork {
    from_id: u64,
    target_id: u64,
    registry: NodeRegistry,
    fault: Arc<FaultController>,
}

impl SimNetwork {
    fn rpc_unreachable(msg: impl std::fmt::Display) -> RPCError<u64, BasicNode, RaftError<u64>> {
        RPCError::Unreachable(Unreachable::new(&AnyError::error(msg.to_string())))
    }

    fn iss_unreachable(
        msg: impl std::fmt::Display,
    ) -> RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>> {
        RPCError::Unreachable(Unreachable::new(&AnyError::error(msg.to_string())))
    }
}

impl RaftNetwork<GgapTypeConfig> for SimNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<GgapTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.fault.should_drop(self.from_id, self.target_id).await {
            return Err(Self::rpc_unreachable("simulated drop"));
        }
        let target = {
            let reg = self.registry.read().await;
            match reg.get(&self.target_id) {
                Some(r) => r.clone(),
                None => return Err(Self::rpc_unreachable("target not in registry")),
            }
        };
        target
            .append_entries(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&AnyError::error(e.to_string()))))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.fault.should_drop(self.from_id, self.target_id).await {
            return Err(Self::rpc_unreachable("simulated drop"));
        }
        let target = {
            let reg = self.registry.read().await;
            match reg.get(&self.target_id) {
                Some(r) => r.clone(),
                None => return Err(Self::rpc_unreachable("target not in registry")),
            }
        };
        target
            .vote(rpc)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&AnyError::error(e.to_string()))))
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<GgapTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        if self.fault.should_drop(self.from_id, self.target_id).await {
            return Err(Self::iss_unreachable("simulated drop"));
        }
        let target = {
            let reg = self.registry.read().await;
            match reg.get(&self.target_id) {
                Some(r) => r.clone(),
                None => return Err(Self::iss_unreachable("target not in registry")),
            }
        };
        target
            .install_snapshot(rpc)
            .await
            .map_err(Self::iss_unreachable)
    }
}

// ---------------------------------------------------------------------------
// SimNode + SimCluster
// ---------------------------------------------------------------------------

struct SimNode {
    id: u64,
    raft: Arc<GgapRaft>,
    fsm: Arc<FjallStateMachine>,
    _tempdir: TempDir,
}

struct SimCluster {
    nodes: Vec<SimNode>,
    registry: NodeRegistry,
    fault: Arc<FaultController>,
}

impl SimCluster {
    /// Build and initialize an `count`-node cluster with the given snapshot threshold.
    async fn start(count: u64, snapshot_threshold: u64) -> Self {
        let fault = FaultController::new(0xdead_beef_cafe_babe);
        let registry: NodeRegistry = Arc::new(RwLock::new(HashMap::new()));
        let cfg = build_raft_config(50, 150, 300, snapshot_threshold);

        let mut nodes = Vec::new();
        let mut initial_members: BTreeMap<u64, BasicNode> = BTreeMap::new();

        for id in 1..=count {
            let dir = tempfile::tempdir().unwrap();
            let store = FjallStore::open(dir.path()).unwrap();
            let fsm = Arc::new(FjallStateMachine::new(store.clone()));
            let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), 0);
            let sm = GgapStateMachine::new(fsm.clone(), 0);
            let net = SimNetworkFactory {
                from_id: id,
                registry: registry.clone(),
                fault: fault.clone(),
            };
            let raft = Arc::new(
                GgapRaft::new(id, cfg.clone(), net, log_store, sm)
                    .await
                    .unwrap(),
            );
            registry.write().await.insert(id, raft.clone());
            initial_members.insert(id, BasicNode::default());
            nodes.push(SimNode {
                id,
                raft,
                fsm,
                _tempdir: dir,
            });
        }

        // Bootstrap from node 1.
        nodes[0].raft.initialize(initial_members).await.unwrap();

        SimCluster {
            nodes,
            registry,
            fault,
        }
    }

    /// Return the current leader, or `None` if no node thinks it is the leader.
    fn current_leader(&self) -> Option<u64> {
        for node in &self.nodes {
            if node.raft.metrics().borrow().state == ServerState::Leader {
                return Some(node.id);
            }
        }
        None
    }

    /// Advance paused time in increments until a leader is elected, panic if
    /// no leader appears after `max_rounds` × 300 ms of simulated time.
    async fn wait_for_leader(&self) -> u64 {
        for _ in 0..30 {
            tokio::time::advance(Duration::from_millis(300)).await;
            drain_tasks(300).await;
            if let Some(id) = self.current_leader() {
                return id;
            }
        }
        panic!("no leader elected after 9 s of simulated time");
    }

    /// Write a key via `client_write` on `leader_id`; returns committed log index.
    async fn write(&self, leader_id: u64, key: &str, value: &[u8]) -> u64 {
        let node = self.node(leader_id);
        let resp = node
            .raft
            .client_write(KvCommand::Put {
                key: key.into(),
                value: value.to_vec(),
                ttl_ns: None,
                expect_version: 0,
            })
            .await
            .unwrap_or_else(|e| panic!("client_write({key}) failed: {e}"));
        resp.log_id().index
    }

    /// Read a key directly from the FSM of `node_id` (bypasses linearizability).
    async fn read(&self, node_id: u64, key: &str) -> Option<Vec<u8>> {
        self.node(node_id)
            .fsm
            .get(0, key, 0)
            .await
            .unwrap()
            .map(|e| e.value)
    }

    fn node(&self, id: u64) -> &SimNode {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .unwrap_or_else(|| panic!("node {id} not found"))
    }

    async fn partition(&self, a: u64, b: u64) {
        self.fault.partition(a, b).await;
    }

    async fn repair(&self, a: u64, b: u64) {
        self.fault.repair(a, b).await;
    }

    /// Deregister a node (so SimNetwork returns Unreachable for it) and shut it down.
    async fn kill_node(&self, id: u64) {
        self.registry.write().await.remove(&id);
        let _ = self.node(id).raft.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Yield the tokio scheduler `n` times, giving all queued tasks a chance to run.
async fn drain_tasks(n: usize) {
    for _ in 0..n {
        tokio::task::yield_now().await;
    }
}

// ---------------------------------------------------------------------------
// Linearizability checker
// ---------------------------------------------------------------------------

struct OpRecord {
    key: String,
    #[allow(dead_code)]
    log_index: u64,
}

/// Read-after-write check: every key that was successfully written must be
/// visible in a subsequent FSM read.  Returns `false` on any violation.
fn check_read_after_write(history: &[OpRecord], reads: &[(String, bool)]) -> bool {
    for (read_key, visible) in reads {
        if !visible && history.iter().any(|op| &op.key == read_key) {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Concurrent writer task
// ---------------------------------------------------------------------------

/// A single writer task that writes `num_writes` keys with a unique prefix,
/// retrying on errors with leader re-discovery from the registry.
async fn writer_task(
    writer_id: usize,
    num_writes: usize,
    registry: NodeRegistry,
    result_tx: tokio::sync::mpsc::Sender<OpRecord>,
) {
    let mut cur_leader_id: Option<u64> = None;

    for i in 0..num_writes {
        let key = format!("w{writer_id}_key{i:02}");
        let value = format!("w{writer_id}_val{i}").into_bytes();

        let log_index = loop {
            // Find the current leader from the registry.
            let leader_raft = {
                let reg = registry.read().await;
                let cached = cur_leader_id.and_then(|lid| {
                    let r = reg.get(&lid)?;
                    if r.metrics().borrow().state == ServerState::Leader {
                        Some(r.clone())
                    } else {
                        None
                    }
                });
                match cached {
                    Some(r) => r,
                    None => {
                        // Scan for a leader.
                        let found = reg.iter().find_map(|(&id, r)| {
                            if r.metrics().borrow().state == ServerState::Leader {
                                Some((id, r.clone()))
                            } else {
                                None
                            }
                        });
                        match found {
                            Some((id, r)) => {
                                cur_leader_id = Some(id);
                                r
                            }
                            None => {
                                // No leader yet; yield and retry.
                                tokio::task::yield_now().await;
                                continue;
                            }
                        }
                    }
                }
            };

            match leader_raft
                .client_write(KvCommand::Put {
                    key: key.clone(),
                    value: value.clone(),
                    ttl_ns: None,
                    expect_version: 0,
                })
                .await
            {
                Ok(resp) => break resp.log_id().index,
                Err(_) => {
                    cur_leader_id = None; // force re-discovery
                    tokio::task::yield_now().await;
                }
            }
        };

        let _ = result_tx.send(OpRecord { key, log_index }).await;
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_election_under_paused_time() {
    let cluster = SimCluster::start(3, 1000).await;
    let leader = cluster.wait_for_leader().await;
    assert!(
        (1..=3).contains(&leader),
        "expected leader id in 1..=3, got {leader}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_leader_failure_and_reelection() {
    let cluster = SimCluster::start(3, 1000).await;
    let leader = cluster.wait_for_leader().await;

    let idx1 = cluster.write(leader, "k1", b"v1").await;
    assert!(idx1 >= 1);

    // Remove the leader from the registry so it appears unreachable, then shut down.
    cluster.kill_node(leader).await;
    drain_tasks(50).await;

    // The surviving two nodes should elect a new leader.
    let new_leader = cluster.wait_for_leader().await;
    assert_ne!(
        new_leader, leader,
        "new leader must differ from the killed node"
    );

    // Write via the new leader.
    let idx2 = cluster.write(new_leader, "k2", b"v2").await;
    assert!(idx2 > idx1, "second write should have higher log index");

    assert_eq!(
        cluster.read(new_leader, "k2").await.as_deref(),
        Some(b"v2".as_ref())
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_partition_and_heal() {
    let cluster = SimCluster::start(3, 1000).await;
    let leader = cluster.wait_for_leader().await;

    cluster.write(leader, "before", b"pre").await;

    // Isolate the first non-leader node.
    let isolated = cluster
        .nodes
        .iter()
        .find(|n| n.id != leader)
        .map(|n| n.id)
        .unwrap();
    let other = cluster
        .nodes
        .iter()
        .find(|n| n.id != leader && n.id != isolated)
        .map(|n| n.id)
        .unwrap();

    cluster.partition(isolated, leader).await;
    cluster.partition(isolated, other).await;

    // Majority side (leader + other) should still accept writes.
    cluster.write(leader, "during", b"majority").await;
    tokio::time::advance(Duration::from_millis(300)).await;
    drain_tasks(300).await;

    assert_eq!(
        cluster.read(other, "during").await.as_deref(),
        Some(b"majority".as_ref()),
        "non-isolated peer should have the key written during partition"
    );

    // Heal both directions of the partition.
    cluster.repair(isolated, leader).await;
    cluster.repair(isolated, other).await;

    // Give the isolated node time to catch up.
    tokio::time::advance(Duration::from_millis(600)).await;
    drain_tasks(400).await;

    assert_eq!(
        cluster.read(isolated, "during").await.as_deref(),
        Some(b"majority".as_ref()),
        "isolated node should replicate after partition heals"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_message_drop_linearizability() {
    let cluster = SimCluster::start(3, 1000).await;
    let mut cur_leader = cluster.wait_for_leader().await;

    // 20 % message drop rate.
    cluster.fault.set_drop_rate(200_000);

    let mut history = Vec::new();
    let mut reads = Vec::new();

    for i in 0u32..20 {
        let key = format!("key{i:02}");
        let value = format!("val{i}").into_bytes();

        // Retry the write until it succeeds, routing to the current leader and
        // re-discovering it when we receive a `ForwardToLeader` error.
        let log_index = loop {
            let raft = cluster.node(cur_leader).raft.clone();
            let key2 = key.clone();
            let value2 = value.clone();
            let (tx, mut rx) = tokio::sync::oneshot::channel::<Result<u64, ()>>();
            tokio::spawn(async move {
                match raft
                    .client_write(KvCommand::Put {
                        key: key2,
                        value: value2,
                        ttl_ns: None,
                        expect_version: 0,
                    })
                    .await
                {
                    Ok(resp) => {
                        let _ = tx.send(Ok(resp.log_id().index));
                    }
                    Err(_) => {
                        // Any error (ForwardToLeader, consensus, etc.) — signal retry.
                        let _ = tx.send(Err(()));
                    }
                }
            });

            // Pump time + tasks until the write settles.
            let outcome = loop {
                tokio::time::advance(Duration::from_millis(100)).await;
                drain_tasks(100).await;
                match rx.try_recv() {
                    Ok(r) => break r,
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => continue,
                    Err(_) => break Err(()),
                }
            };

            match outcome {
                Ok(idx) => break idx,
                Err(()) => {
                    // Leadership may have shifted — re-discover and retry.
                    drain_tasks(50).await;
                    cur_leader = cluster.wait_for_leader().await;
                }
            }
        };

        history.push(OpRecord {
            key: key.clone(),
            log_index,
        });

        // Read directly from the current leader's FSM.
        let visible = cluster.read(cur_leader, &key).await.is_some();
        reads.push((key, visible));
    }

    assert!(
        check_read_after_write(&history, &reads),
        "read-after-write linearizability violated"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_message_drop_linearizability_concurrent() {
    let cluster = SimCluster::start(3, 1000).await;
    let _leader = cluster.wait_for_leader().await;

    // 20 % message drop rate.
    cluster.fault.set_drop_rate(200_000);

    const NUM_WRITERS: usize = 5;
    const WRITES_PER_WRITER: usize = 10;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<OpRecord>(NUM_WRITERS * WRITES_PER_WRITER);

    let mut handles = Vec::new();
    for w in 0..NUM_WRITERS {
        let registry = cluster.registry.clone();
        let tx = tx.clone();
        handles.push(tokio::spawn(writer_task(
            w,
            WRITES_PER_WRITER,
            registry,
            tx,
        )));
    }
    drop(tx); // close sender so rx drains cleanly after writers finish

    // Pump simulated time until all writers complete.
    let mut rounds = 0;
    loop {
        tokio::time::advance(Duration::from_millis(100)).await;
        drain_tasks(200).await;
        if handles.iter().all(|h| h.is_finished()) {
            break;
        }
        rounds += 1;
        if rounds > 2000 {
            panic!("concurrent writers did not complete after 200 s of simulated time");
        }
    }

    // Propagate any panics from writer tasks.
    for h in handles {
        h.await.unwrap();
    }

    // Collect all OpRecords.
    let mut history = Vec::new();
    while let Ok(record) = rx.try_recv() {
        history.push(record);
    }
    assert_eq!(
        history.len(),
        NUM_WRITERS * WRITES_PER_WRITER,
        "expected {} committed writes, got {}",
        NUM_WRITERS * WRITES_PER_WRITER,
        history.len()
    );

    // Verify every key is readable from the leader's FSM.
    let leader = cluster.wait_for_leader().await;
    let mut reads = Vec::new();
    for record in &history {
        let visible = cluster.read(leader, &record.key).await.is_some();
        reads.push((record.key.clone(), visible));
    }

    assert!(
        check_read_after_write(&history, &reads),
        "read-after-write violated with concurrent writers"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_snapshot_catchup() {
    // Snapshot threshold = 10 → snapshot triggered every ~10 log entries.
    let cluster = SimCluster::start(3, 10).await;
    let mut leader = cluster.wait_for_leader().await;

    // Write 20 keys, crossing the snapshot threshold at least twice.
    // Advance in small increments between writes so heartbeats keep firing.
    for i in 0u32..20 {
        let key = format!("snap{i:02}");
        // Re-discover leader in case it changed.
        if cluster.current_leader() != Some(leader) {
            leader = cluster.wait_for_leader().await;
        }
        cluster
            .write(leader, &key, format!("v{i}").as_bytes())
            .await;
        // Small advance to let snapshot build tasks run.
        tokio::time::advance(Duration::from_millis(60)).await;
        drain_tasks(100).await;
    }

    // Longer settle to allow snapshot building and replication.
    for _ in 0..5 {
        tokio::time::advance(Duration::from_millis(100)).await;
        drain_tasks(200).await;
    }

    // Re-discover the current leader.
    leader = cluster
        .current_leader()
        .unwrap_or_else(|| panic!("no leader after writes"));

    // All 20 keys must be in the leader's FSM.
    for i in 0u32..20 {
        let key = format!("snap{i:02}");
        assert!(
            cluster.read(leader, &key).await.is_some(),
            "leader FSM missing {key}"
        );
    }

    // ------------------------------------------------------------------
    // Add a fresh 4th node and let it catch up via snapshot install.
    // ------------------------------------------------------------------
    let new_id: u64 = 4;
    let dir4 = tempfile::tempdir().unwrap();
    let store4 = FjallStore::open(dir4.path()).unwrap();
    let fsm4 = Arc::new(FjallStateMachine::new(store4.clone()));
    let log_store4 = GgapLogStorage::new(FjallLogStorage(store4.clone()), 0);
    let sm4 = GgapStateMachine::new(fsm4.clone(), 0);
    let net4 = SimNetworkFactory {
        from_id: new_id,
        registry: cluster.registry.clone(),
        fault: cluster.fault.clone(),
    };
    let cfg4 = build_raft_config(50, 150, 300, 10);
    let raft4 = Arc::new(
        GgapRaft::new(new_id, cfg4, net4, log_store4, sm4)
            .await
            .unwrap(),
    );
    // Register node 4 so SimNetwork can find it.
    cluster.registry.write().await.insert(new_id, raft4.clone());

    // Add as learner (non-blocking) — the leader starts replicating.
    cluster
        .node(leader)
        .raft
        .add_learner(new_id, BasicNode::default(), false)
        .await
        .expect("add_learner failed");

    // Advance time to let the leader send the snapshot to node 4.
    for _ in 0..10 {
        tokio::time::advance(Duration::from_millis(100)).await;
        drain_tasks(200).await;
    }

    // Promote node 4 to a full voter via change_membership, spawned so we can
    // pump time while openraft waits for the joint-consensus phases to commit.
    let raft_leader = cluster.node(leader).raft.clone();
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let mut voters = BTreeSet::new();
        voters.insert(new_id);
        let _ = raft_leader
            .change_membership(ChangeMembers::AddVoterIds(voters), false)
            .await;
        let _ = done_tx.send(());
    });

    // Pump until change_membership completes (up to 6 s of sim time).
    for _ in 0..30 {
        tokio::time::advance(Duration::from_millis(200)).await;
        drain_tasks(300).await;
        if done_rx.try_recv().is_ok() {
            break;
        }
    }

    // Final settle so snapshot data propagates into fsm4.
    for _ in 0..5 {
        tokio::time::advance(Duration::from_millis(100)).await;
        drain_tasks(200).await;
    }

    // Node 4's FSM must contain all 20 keys (installed via snapshot).
    for i in 0u32..20 {
        let key = format!("snap{i:02}");
        assert!(
            fsm4.get(0, &key, 0).await.unwrap().is_some(),
            "node 4 FSM missing {key} after snapshot catchup"
        );
    }

    let _ = raft4.shutdown().await;
    drop(dir4);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_membership_change_under_partition() {
    // Start a 3-node cluster.
    let cluster = SimCluster::start(3, 1000).await;
    let leader = cluster.wait_for_leader().await;

    // Write initial data.
    cluster.write(leader, "init", b"data").await;
    tokio::time::advance(Duration::from_millis(200)).await;
    drain_tasks(200).await;

    // Add node 4 with its own storage (learner, non-blocking).
    let new_id: u64 = 4;
    let dir4 = tempfile::tempdir().unwrap();
    let store4 = FjallStore::open(dir4.path()).unwrap();
    let fsm4 = Arc::new(FjallStateMachine::new(store4.clone()));
    let log_store4 = GgapLogStorage::new(FjallLogStorage(store4.clone()), 0);
    let sm4 = GgapStateMachine::new(fsm4.clone(), 0);
    let net4 = SimNetworkFactory {
        from_id: new_id,
        registry: cluster.registry.clone(),
        fault: cluster.fault.clone(),
    };
    let cfg4 = build_raft_config(50, 150, 300, 1000);
    let raft4 = Arc::new(
        GgapRaft::new(new_id, cfg4, net4, log_store4, sm4)
            .await
            .unwrap(),
    );
    cluster.registry.write().await.insert(new_id, raft4.clone());

    // Start the membership change (add_learner is non-blocking here).
    cluster
        .node(leader)
        .raft
        .add_learner(new_id, BasicNode::default(), false)
        .await
        .expect("add_learner failed");

    // Immediately partition the leader from one follower to stress joint-consensus.
    let follower = cluster
        .nodes
        .iter()
        .find(|n| n.id != leader)
        .map(|n| n.id)
        .unwrap();
    cluster.partition(leader, follower).await;

    // Advance time: let the cluster detect partition and elect a new leader.
    for _ in 0..20 {
        tokio::time::advance(Duration::from_millis(300)).await;
        drain_tasks(300).await;
    }

    // Heal the partition.
    cluster.repair(leader, follower).await;

    // Allow time for convergence.
    for _ in 0..10 {
        tokio::time::advance(Duration::from_millis(200)).await;
        drain_tasks(200).await;
    }

    // The cluster must have a stable leader.
    let final_leader = cluster.wait_for_leader().await;
    assert!(
        (1..=4).contains(&final_leader),
        "expected a valid leader after partition heals, got {final_leader}"
    );

    // All original nodes that are still in the registry must agree on the initial key.
    let surviving_ids: Vec<u64> = {
        let reg = cluster.registry.read().await;
        reg.keys().copied().collect()
    };
    // Every surviving original node must have "init".
    for id in &surviving_ids {
        if *id <= 3 {
            // original nodes only
            assert_eq!(
                cluster.read(*id, "init").await.as_deref(),
                Some(b"data".as_ref()),
                "node {id} lost the initial write after membership change + partition"
            );
        }
    }

    let _ = raft4.shutdown().await;
    drop(dir4);
}
