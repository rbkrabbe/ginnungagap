//! Verifies that inbound gRPC calls on all three services (`KvService`,
//! `AdminService`, `RaftService`) emit the OTel-aligned Prometheus histogram
//! `rpc_server_call_duration_seconds` with the expected `(rpc.method,
//! rpc.response.status_code)` label pairs.
//!
//! A single-node in-process Raft cluster is started, a `DebuggingRecorder` is
//! installed process-globally (guarded by `OnceLock` — this is the only test
//! in the workspace that installs a metrics recorder), and each service is
//! exercised against a tonic client connected to a loopback listener.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use openraft::BasicNode;
use tempfile::TempDir;
use tokio::net::TcpListener;

use ggap_consensus::{
    build_raft_config, GgapLogStorage, GgapNetworkFactory, GgapRaft, GgapStateMachine,
    OpenRaftCluster, OpenRaftNode, ShardRegistry, ShardRouter, SplitCoordinator,
    SplitCoordinatorConfig,
};
use ggap_proto::v1::{
    admin_service_client::AdminServiceClient, kv_service_client::KvServiceClient,
    raft_service_client::RaftServiceClient, AddLearnerRequest, CasRequest, ClusterStatusRequest,
    DeleteRequest, GetRequest, ListShardsRequest, NodeInfo, PutRequest, RaftMessage, ScanRequest,
};
use ggap_server::{serve_client_with_listener, serve_cluster_with_listener, KvServiceConfig};
use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::ShardMap;

const METRIC: &str = "rpc_server_call_duration_seconds";

static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();

fn install_recorder() -> &'static Snapshotter {
    SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        recorder
            .install()
            .expect("DebuggingRecorder must install exactly once per process");
        snap
    })
}

struct TestNode {
    client_addr: SocketAddr,
    cluster_addr: SocketAddr,
    raft: Arc<GgapRaft>,
    _handles: Vec<tokio::task::JoinHandle<()>>,
    _tempdir: TempDir,
}

async fn start_single_node() -> TestNode {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();

    let shard_map = Arc::new(ShardMap::load(store.clone()).unwrap());
    shard_map.initialize_default().await.unwrap();

    let (split_tx, _split_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut fsm_builder = FjallStateMachine::new(store.clone());
    fsm_builder.set_split_sender(split_tx);
    fsm_builder.set_shard_map(shard_map.clone());
    let fsm = Arc::new(fsm_builder);

    let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), 0);
    let sm = GgapStateMachine::new(fsm.clone(), 0);
    let raft_cfg = build_raft_config(50, 150, 300, 500);
    let raft = Arc::new(
        GgapRaft::new(
            1,
            raft_cfg.clone(),
            GgapNetworkFactory::new(0),
            log_store,
            sm,
        )
        .await
        .expect("raft init"),
    );

    let cluster_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster_addr = cluster_listener.local_addr().unwrap();
    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();

    let raft_node = Arc::new(OpenRaftNode::new(
        raft.clone(),
        fsm.clone(),
        0,
        1,
        tokio::time::Duration::from_millis(100),
    ));
    let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));

    let router = Arc::new(ShardRouter::new(shard_map.clone()));
    router.add_shard(0, raft_node, cluster).await;

    let split_coordinator = Arc::new(SplitCoordinator::new(SplitCoordinatorConfig {
        router: router.clone(),
        shard_map: shard_map.clone(),
    }));

    let registry = Arc::new(ShardRegistry::new(1, [(1, cluster_addr.to_string())]));
    let mut handles = Vec::new();
    let r = router.clone();
    let sc = split_coordinator;
    let sm2 = shard_map.clone();
    handles.push(tokio::spawn(async move {
        let _ = serve_cluster_with_listener(cluster_listener, r, sc, sm2, registry).await;
    }));
    let r = router.clone();
    handles.push(tokio::spawn(async move {
        let _ = serve_client_with_listener(client_listener, r, 1, KvServiceConfig::default()).await;
    }));

    // Single-node bootstrap.
    let members: BTreeMap<u64, BasicNode> = BTreeMap::from([(
        1,
        BasicNode {
            addr: cluster_addr.to_string(),
        },
    )]);
    raft.initialize(members).await.expect("cluster init");

    // Wait until this node is leader so writes are accepted.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while raft.metrics().borrow().state != openraft::ServerState::Leader {
        assert!(
            tokio::time::Instant::now() < deadline,
            "single node never became leader"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    TestNode {
        client_addr,
        cluster_addr,
        raft,
        _handles: handles,
        _tempdir: tempdir,
    }
}

/// Collect histogram observation counts keyed by `(rpc.method, rpc.response.status_code)`.
fn duration_counts(snap: &Snapshotter) -> HashMap<(String, String), usize> {
    let mut out = HashMap::new();
    for (key, _unit, _desc, value) in snap.snapshot().into_vec() {
        let (_kind, k) = key.into_parts();
        if k.name() != METRIC {
            continue;
        }
        let mut method = None;
        let mut status = None;
        for label in k.labels() {
            let label: &metrics::Label = label;
            match label.key() {
                "rpc.method" => method = Some(label.value().to_string()),
                "rpc.response.status_code" => status = Some(label.value().to_string()),
                _ => {}
            }
        }
        if let (Some(m), Some(s)) = (method, status) {
            if let DebugValue::Histogram(values) = value {
                *out.entry((m, s)).or_insert(0) += values.len();
            }
        }
    }
    out
}

/// Verify every `rpc_server_call_duration_seconds` series carries
/// `rpc.system.name = "grpc"`.
fn assert_grpc_system(snap: &Snapshotter) {
    for (key, _unit, _desc, _value) in snap.snapshot().into_vec() {
        let (_kind, k) = key.into_parts();
        if k.name() != METRIC {
            continue;
        }
        let has_grpc = k
            .labels()
            .any(|l| l.key() == "rpc.system.name" && l.value() == "grpc");
        assert!(
            has_grpc,
            "series missing rpc.system.name=grpc: labels={:?}",
            k.labels().collect::<Vec<_>>()
        );
    }
}

fn has(counts: &HashMap<(String, String), usize>, method: &str, status: &str) -> bool {
    counts
        .get(&(method.to_string(), status.to_string()))
        .copied()
        .unwrap_or(0)
        >= 1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_services_emit_rpc_server_call_duration_metrics() {
    let snap = install_recorder();
    let node = start_single_node().await;

    let client_endpoint = format!("http://{}", node.client_addr);
    let cluster_endpoint = format!("http://{}", node.cluster_addr);

    // ---------- KvService ----------
    let mut kv = KvServiceClient::connect(client_endpoint)
        .await
        .expect("connect to kv service");

    kv.put(PutRequest {
        key: "a".into(),
        value: b"1".to_vec(),
        ..Default::default()
    })
    .await
    .expect("put ok");
    kv.get(GetRequest {
        key: "a".into(),
        ..Default::default()
    })
    .await
    .expect("get ok");
    let err = kv
        .get(GetRequest {
            key: "missing".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    let err = kv
        .put(PutRequest {
            key: "".into(),
            value: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let err = kv
        .put(PutRequest {
            key: "a".into(),
            value: b"2".to_vec(),
            expect_version: 999,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Aborted);
    kv.delete(DeleteRequest {
        key: "a".into(),
        ..Default::default()
    })
    .await
    .expect("delete ok");
    kv.scan(ScanRequest {
        start_key: "".into(),
        end_key: "".into(),
        limit: 10,
        ..Default::default()
    })
    .await
    .expect("scan ok");
    kv.compare_and_swap(CasRequest {
        key: "b".into(),
        expected_value: b"".to_vec(),
        new_value: b"v".to_vec(),
        ..Default::default()
    })
    .await
    .expect("cas ok");

    // ---------- AdminService ----------
    let mut admin = AdminServiceClient::connect(cluster_endpoint.clone())
        .await
        .expect("connect to admin service");
    admin
        .cluster_status(ClusterStatusRequest { shard_id: None })
        .await
        .expect("cluster_status ok");
    admin
        .list_shards(ListShardsRequest {})
        .await
        .expect("list_shards ok");
    let err = admin
        .add_learner(AddLearnerRequest {
            node: Some(NodeInfo {
                node_id: 0,
                client_addr: String::new(),
                cluster_addr: "127.0.0.1:1".into(),
            }),
            shard_id: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // ---------- RaftService ----------
    let mut raft_client = RaftServiceClient::connect(cluster_endpoint)
        .await
        .expect("connect to raft service");
    let err = raft_client
        .vote(RaftMessage {
            shard_id: 999,
            data: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // ---------- Assertions ----------
    let counts = duration_counts(snap);
    assert_grpc_system(snap);

    // KvService
    assert!(
        has(&counts, "ginnungagap.v1.KvService/Put", "OK"),
        "kv Put Ok: {counts:?}"
    );
    assert!(
        has(&counts, "ginnungagap.v1.KvService/Get", "OK"),
        "kv Get Ok: {counts:?}"
    );
    assert!(
        has(&counts, "ginnungagap.v1.KvService/Get", "NOT_FOUND"),
        "kv Get NotFound: {counts:?}"
    );
    assert!(
        has(&counts, "ginnungagap.v1.KvService/Put", "INVALID_ARGUMENT"),
        "kv Put InvalidArgument: {counts:?}"
    );
    assert!(
        has(&counts, "ginnungagap.v1.KvService/Put", "ABORTED"),
        "kv Put Aborted: {counts:?}"
    );
    assert!(
        has(&counts, "ginnungagap.v1.KvService/Delete", "OK"),
        "kv Delete Ok: {counts:?}"
    );
    assert!(
        has(&counts, "ginnungagap.v1.KvService/Scan", "OK"),
        "kv Scan Ok: {counts:?}"
    );
    assert!(
        has(&counts, "ginnungagap.v1.KvService/CompareAndSwap", "OK"),
        "kv Cas Ok: {counts:?}"
    );

    // AdminService
    assert!(
        has(&counts, "ginnungagap.v1.AdminService/ClusterStatus", "OK"),
        "admin ClusterStatus Ok: {counts:?}"
    );
    assert!(
        has(&counts, "ginnungagap.v1.AdminService/ListShards", "OK"),
        "admin ListShards Ok: {counts:?}"
    );
    assert!(
        has(
            &counts,
            "ginnungagap.v1.AdminService/AddLearner",
            "INVALID_ARGUMENT"
        ),
        "admin AddLearner InvalidArgument: {counts:?}"
    );

    // RaftService
    assert!(
        has(&counts, "ginnungagap.v1.RaftService/Vote", "NOT_FOUND"),
        "raft Vote NotFound: {counts:?}"
    );

    node.raft.shutdown().await.ok();
    for h in node._handles {
        h.abort();
    }
}
