//! Verifies that `RaftMetricsTask` emits the expected Prometheus gauges after
//! running against a live single-node Raft cluster.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use openraft::BasicNode;
use tempfile::TempDir;

use ggap_consensus::{
    build_raft_config, GgapLogStorage, GgapNetworkFactory, GgapRaft, GgapStateMachine,
    RaftMetricsTask,
};
use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::ShardMap;
use tokio_util::sync::CancellationToken;

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

async fn start_single_node_raft() -> (Arc<GgapRaft>, TempDir) {
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
    let sm = GgapStateMachine::new(fsm, 0);
    let raft_cfg = build_raft_config(50, 150, 300, 500);
    let raft = Arc::new(
        GgapRaft::new(1, raft_cfg, GgapNetworkFactory::new(0), log_store, sm)
            .await
            .expect("raft init"),
    );

    let members: BTreeMap<u64, BasicNode> = BTreeMap::from([(
        1,
        BasicNode {
            addr: "127.0.0.1:0".into(),
        },
    )]);
    raft.initialize(members).await.expect("cluster init");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while raft.metrics().borrow().state != openraft::ServerState::Leader {
        assert!(
            tokio::time::Instant::now() < deadline,
            "single node never became leader"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    (raft, tempdir)
}

fn gauge_values(snap: &Snapshotter) -> HashMap<(String, String), f64> {
    let mut out = HashMap::new();
    for (key, _unit, _desc, value) in snap.snapshot().into_vec() {
        let (_kind, k) = key.into_parts();
        let name = k.name().to_string();
        if !name.starts_with("raft_") {
            continue;
        }
        let shard_id = k
            .labels()
            .find(|l| l.key() == "shard_id")
            .map(|l| l.value().to_string())
            .unwrap_or_default();
        if let DebugValue::Gauge(v) = value {
            out.insert((name, shard_id), v.into_inner());
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_metrics_task_emits_gauges() {
    let snap = install_recorder();
    let (raft, _dir) = start_single_node_raft().await;

    let cancel = CancellationToken::new();
    tokio::spawn(
        RaftMetricsTask::new(raft.clone(), 0, cancel.clone())
            .with_poll_interval(Duration::from_millis(50))
            .run(),
    );

    // Allow several poll cycles to fire.
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel.cancel();

    let gauges = gauge_values(snap);

    let expected = [
        "raft_current_term",
        "raft_last_applied_index",
        "raft_last_log_index",
        "raft_commit_lag",
        "raft_is_leader",
    ];
    for name in expected {
        assert!(
            gauges.contains_key(&(name.to_string(), "0".to_string())),
            "missing gauge {name} with shard_id=0; present: {gauges:?}",
        );
    }

    // Leader gauge must be 1.0 since this is a single-node cluster.
    assert_eq!(
        gauges[&("raft_is_leader".to_string(), "0".to_string())],
        1.0,
        "single-node must report raft_is_leader=1"
    );

    raft.shutdown().await.ok();
}
