//! Write-throughput benchmark for a 3-node Raft cluster.
//!
//! Starts a real 3-node cluster with loopback gRPC, finds the leader,
//! then drives Put RPCs through the KV gRPC service with UUID keys and
//! 1–2 KB random values.
//!
//! Leader redirects (Code::Unavailable + ggap-leader-addr header) are
//! followed automatically, so the measurement is not sensitive to the
//! rare leader changes that may occur under heavy load.
//!
//! Run with:
//!   cargo bench -p ggap-server --bench kv_write

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use openraft::{BasicNode, ServerState};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use uuid::Uuid;

use ggap_consensus::{
    build_raft_config, GgapLogStorage, GgapNetworkFactory, GgapRaft, GgapStateMachine,
    OpenRaftCluster, OpenRaftNode,
};
use ggap_proto::v1::{kv_service_client::KvServiceClient, PutRequest};
use ggap_server::{serve_client_with_listener, serve_cluster_with_listener};
use ggap_storage::fjall::{FjallStateMachine, FjallStore};

// ---------------------------------------------------------------------------
// Tuning knobs
// ---------------------------------------------------------------------------

const TOTAL_WRITES: usize = 1_000_000;

/// Maximum number of in-flight gRPC calls. HTTP/2 multiplexes them over
/// a single connection.
const CONCURRENCY: usize = 512;

/// Conservative timeouts — keeps the leader stable under write pressure.
/// (heartbeat, election_min, election_max) in milliseconds.
const HEARTBEAT_MS: u64 = 500;
const ELECTION_MIN_MS: u64 = 1_500;
const ELECTION_MAX_MS: u64 = 3_000;

/// Pre-generated random buffer; value slices are cut from it.
const RAND_BUF_SIZE: usize = 4 * 1024 * 1024;

const REPORT_INTERVAL: usize = 100_000;

// ---------------------------------------------------------------------------
// Cluster setup
// ---------------------------------------------------------------------------

struct BenchNode {
    id: u64,
    raft: Arc<GgapRaft>,
    cluster_addr: SocketAddr,
    client_addr: SocketAddr,
    _tempdir: TempDir,
    _handles: Vec<tokio::task::JoinHandle<()>>,
}

async fn start_node(id: u64) -> BenchNode {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();
    let fsm = Arc::new(FjallStateMachine::new(store.clone()));
    let log_store = GgapLogStorage::new(store.clone(), 0);
    let sm = GgapStateMachine::new(fsm.clone(), 0);
    let cfg = build_raft_config(HEARTBEAT_MS, ELECTION_MIN_MS, ELECTION_MAX_MS);
    let raft = Arc::new(
        GgapRaft::new(id, cfg, GgapNetworkFactory, log_store, sm)
            .await
            .unwrap_or_else(|e| panic!("node {id} init: {e}")),
    );

    let cluster_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster_addr = cluster_listener.local_addr().unwrap();
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();

    let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));
    let raft_node = Arc::new(OpenRaftNode::new(raft.clone(), fsm.clone(), 0, id));

    let mut handles = Vec::new();

    let c = cluster.clone();
    handles.push(tokio::spawn(async move {
        if let Err(e) = serve_cluster_with_listener(cluster_listener, c).await {
            eprintln!("node {id} cluster: {e}");
        }
    }));

    let n = raft_node.clone();
    handles.push(tokio::spawn(async move {
        if let Err(e) = serve_client_with_listener(client_listener, n, id).await {
            eprintln!("node {id} client: {e}");
        }
    }));

    BenchNode {
        id,
        raft,
        cluster_addr,
        client_addr,
        _tempdir: tempdir,
        _handles: handles,
    }
}

struct BenchCluster {
    nodes: Vec<BenchNode>,
}

impl BenchCluster {
    async fn start(count: usize) -> Self {
        let mut nodes = Vec::with_capacity(count);
        for id in 1..=(count as u64) {
            nodes.push(start_node(id).await);
        }
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
        nodes[0]
            .raft
            .initialize(members)
            .await
            .expect("cluster init failed");
        Self { nodes }
    }

    async fn wait_for_leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            for (i, node) in self.nodes.iter().enumerate() {
                if node.raft.metrics().borrow().state == ServerState::Leader {
                    return i;
                }
            }
            assert!(Instant::now() < deadline, "no leader within 15 s");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

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
// Leader-following gRPC client
// ---------------------------------------------------------------------------

async fn connect(addr: SocketAddr) -> KvServiceClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::new(format!("http://{addr}"))
        .expect("invalid addr")
        .connect()
        .await
        .unwrap_or_else(|e| panic!("connect to {addr}: {e}"));
    KvServiceClient::new(channel)
}

// ---------------------------------------------------------------------------
// Benchmark entry point
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // ── Cluster setup ─────────────────────────────────────────────────────
    eprint!("Starting 3-node cluster … ");
    let cluster = BenchCluster::start(3).await;
    let leader_idx = cluster.wait_for_leader().await;
    let leader_addr = cluster.nodes[leader_idx].client_addr;
    eprintln!(
        "leader is node {} ({})",
        cluster.nodes[leader_idx].id, leader_addr
    );

    // Snapshot of (raft, client_addr) pairs for leader re-discovery on redirect.
    let nodes_info: Arc<Vec<(Arc<GgapRaft>, SocketAddr)>> = Arc::new(
        cluster
            .nodes
            .iter()
            .map(|n| (n.raft.clone(), n.client_addr))
            .collect(),
    );

    // Shared gRPC client — swapped out whenever a leader redirect is received.
    let shared_client: Arc<RwLock<KvServiceClient<tonic::transport::Channel>>> =
        Arc::new(RwLock::new(connect(leader_addr).await));

    // ── Random value buffer ────────────────────────────────────────────────
    let rand_buf: Arc<Vec<u8>> =
        Arc::new((0..RAND_BUF_SIZE).map(|_| rand::random::<u8>()).collect());

    // ── Benchmark ─────────────────────────────────────────────────────────
    eprintln!("\nWriting {TOTAL_WRITES} keys ({CONCURRENCY} in-flight, 1–2 KB values) …\n");
    let start = Instant::now();

    let stream = futures::stream::iter(0..TOTAL_WRITES)
        .map(|i| {
            // Generate key / value parameters synchronously to avoid Send
            // issues with thread-local RNGs inside async blocks.
            let key = Uuid::new_v4().to_string();
            let value_len = 1024 + rand::random::<u64>() as usize % 1025; // [1024, 2048]
            let offset = rand::random::<u64>() as usize % (RAND_BUF_SIZE - value_len + 1);

            let shared_client = shared_client.clone();
            let nodes_info = nodes_info.clone();
            let rand_buf = rand_buf.clone();

            async move {
                let value = rand_buf[offset..offset + value_len].to_vec();
                loop {
                    let mut client = shared_client.read().await.clone();
                    match client
                        .put(PutRequest {
                            key: key.clone(),
                            value: value.clone(),
                            ..Default::default()
                        })
                        .await
                    {
                        Ok(_) => return,
                        Err(status) if status.code() == tonic::Code::Unavailable => {
                            // The server returned NotLeader. Follow the hint
                            // (ggap-leader-addr header) or poll cluster metrics.
                            let new_addr = status
                                .metadata()
                                .get("ggap-leader-addr")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|s| s.parse::<SocketAddr>().ok());

                            // Back off briefly so the new leader can stabilise.
                            tokio::time::sleep(Duration::from_millis(200)).await;

                            let addr = match new_addr {
                                Some(a) => a,
                                None => {
                                    // No hint — scan raft metrics directly.
                                    let deadline = Instant::now() + Duration::from_secs(5);
                                    loop {
                                        let found = nodes_info.iter().find_map(|(raft, addr)| {
                                            (raft.metrics().borrow().state == ServerState::Leader)
                                                .then_some(*addr)
                                        });
                                        if let Some(a) = found {
                                            break a;
                                        }
                                        assert!(
                                            Instant::now() < deadline,
                                            "write {i}: no leader after redirect"
                                        );
                                        tokio::time::sleep(Duration::from_millis(100)).await;
                                    }
                                }
                            };

                            *shared_client.write().await = connect(addr).await;
                            eprintln!("  → redirected write {i} to {addr}");
                        }
                        Err(e) => panic!("write {i} failed: {e}"),
                    }
                }
            }
        })
        .buffer_unordered(CONCURRENCY);

    futures::pin_mut!(stream);

    let mut done = 0usize;
    while let Some(()) = stream.next().await {
        done += 1;
        if done % REPORT_INTERVAL == 0 {
            let secs = start.elapsed().as_secs_f64();
            eprintln!(
                "  {:>9} / {TOTAL_WRITES}  {:.0} writes/s",
                done,
                done as f64 / secs
            );
        }
    }

    let elapsed = start.elapsed();
    let wps = TOTAL_WRITES as f64 / elapsed.as_secs_f64();
    // Average value size ≈ 1.5 KB
    let mbs = TOTAL_WRITES as f64 * 1536.0 / (1u64 << 20) as f64 / elapsed.as_secs_f64();

    println!("\n── Results ────────────────────────────────────────────────");
    println!("  Keys written  : {TOTAL_WRITES}");
    println!("  Concurrency   : {CONCURRENCY} in-flight");
    println!("  Elapsed       : {:.2} s", elapsed.as_secs_f64());
    println!("  Throughput    : {:.0} writes/s", wps);
    println!("  Data rate     : ~{:.1} MB/s  (avg 1.5 KB / key)", mbs);

    cluster.shutdown().await;
}
