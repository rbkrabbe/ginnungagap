//! Verifies that `KvService` unary RPCs emit the expected Prometheus metric
//! series. A single-node in-process Raft cluster is started, a `DebuggingRecorder`
//! is installed process-globally (guarded by `OnceLock` — this is the only test
//! in the workspace that installs a metrics recorder), and each RPC is exercised
//! against a tonic client connected to a loopback listener.
//!
//! Asserts the `(method, status)` label pairs on `ggap_kv_requests_total` and
//! that `ggap_kv_request_duration_seconds` records at least one observation
//! per invocation.

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
    OpenRaftCluster, OpenRaftNode, ShardRouter, SplitCoordinator, SplitCoordinatorConfig,
};
use ggap_proto::v1::{
    kv_service_client::KvServiceClient, CasRequest, DeleteRequest, GetRequest, PutRequest,
    ScanRequest,
};
use ggap_server::{serve_client_with_listener, serve_cluster_with_listener, KvServiceConfig};
use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::ShardMap;

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

    let mut handles = Vec::new();
    let r = router.clone();
    let sc = split_coordinator;
    let sm2 = shard_map.clone();
    handles.push(tokio::spawn(async move {
        let _ = serve_cluster_with_listener(cluster_listener, r, sc, sm2).await;
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
        raft,
        _handles: handles,
        _tempdir: tempdir,
    }
}

/// Collect `ggap_kv_requests_total` samples into a `{(method, status) -> count}` map.
fn counter_totals(snap: &Snapshotter) -> HashMap<(String, String), u64> {
    let mut out = HashMap::new();
    for (key, _unit, _desc, value) in snap.snapshot().into_vec() {
        let (_kind, k) = key.into_parts();
        if k.name() != "ggap_kv_requests_total" {
            continue;
        }
        let mut method = None;
        let mut status = None;
        for label in k.labels() {
            let label: &metrics::Label = label;
            match label.key() {
                "method" => method = Some(label.value().to_string()),
                "status" => status = Some(label.value().to_string()),
                _ => {}
            }
        }
        if let (Some(m), Some(s)) = (method, status) {
            if let DebugValue::Counter(c) = value {
                *out.entry((m, s)).or_insert(0) += c;
            }
        }
    }
    out
}

/// Count distinct `(method, status)` tuples seen on the histogram metric.
fn histogram_method_statuses(snap: &Snapshotter) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (key, _unit, _desc, _value) in snap.snapshot().into_vec() {
        let (_kind, k) = key.into_parts();
        if k.name() != "ggap_kv_request_duration_seconds" {
            continue;
        }
        let mut method = None;
        let mut status = None;
        for label in k.labels() {
            let label: &metrics::Label = label;
            match label.key() {
                "method" => method = Some(label.value().to_string()),
                "status" => status = Some(label.value().to_string()),
                _ => {}
            }
        }
        if let (Some(m), Some(s)) = (method, status) {
            out.push((m, s));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_service_emits_metrics_for_each_rpc() {
    let snap = install_recorder();
    let node = start_single_node().await;

    let endpoint = format!("http://{}", node.client_addr);
    let mut client = KvServiceClient::connect(endpoint)
        .await
        .expect("connect to kv service");

    // ok: Put + Get
    client
        .put(PutRequest {
            key: "a".into(),
            value: b"1".to_vec(),
            ..Default::default()
        })
        .await
        .expect("put ok");
    client
        .get(GetRequest {
            key: "a".into(),
            ..Default::default()
        })
        .await
        .expect("get ok");

    // not_found: Get missing key
    let err = client
        .get(GetRequest {
            key: "missing".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // invalid: Put with empty key
    let err = client
        .put(PutRequest {
            key: "".into(),
            value: b"x".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // conflict: Put with stale expect_version
    let err = client
        .put(PutRequest {
            key: "a".into(),
            value: b"2".to_vec(),
            expect_version: 999,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Aborted);

    // ok: Delete, Scan, CompareAndSwap
    client
        .delete(DeleteRequest {
            key: "a".into(),
            ..Default::default()
        })
        .await
        .expect("delete ok");
    client
        .scan(ScanRequest {
            start_key: "".into(),
            end_key: "".into(),
            limit: 10,
            ..Default::default()
        })
        .await
        .expect("scan ok");
    client
        .compare_and_swap(CasRequest {
            key: "b".into(),
            expected_value: b"".to_vec(),
            new_value: b"v".to_vec(),
            ..Default::default()
        })
        .await
        .expect("cas ok");

    let totals = counter_totals(snap);
    let get = |m: &str, s: &str| *totals.get(&(m.to_string(), s.to_string())).unwrap_or(&0);

    assert!(get("put", "ok") >= 1, "put ok missing: {totals:?}");
    assert!(get("get", "ok") >= 1, "get ok missing: {totals:?}");
    assert!(
        get("get", "not_found") >= 1,
        "get not_found missing: {totals:?}"
    );
    assert!(
        get("put", "invalid") >= 1,
        "put invalid missing: {totals:?}"
    );
    assert!(
        get("put", "conflict") >= 1,
        "put conflict missing: {totals:?}"
    );
    assert!(get("delete", "ok") >= 1, "delete ok missing: {totals:?}");
    assert!(get("scan", "ok") >= 1, "scan ok missing: {totals:?}");
    assert!(
        get("compare_and_swap", "ok") >= 1,
        "cas ok missing: {totals:?}"
    );

    // Histogram must emit for every (method, status) pair the counter saw.
    let hist = histogram_method_statuses(snap);
    for key in totals.keys() {
        assert!(
            hist.iter().any(|(m, s)| m == &key.0 && s == &key.1),
            "histogram missing {key:?}; saw {hist:?}",
        );
    }

    node.raft.shutdown().await.ok();
    for h in node._handles {
        h.abort();
    }
}
