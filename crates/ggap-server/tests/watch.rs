//! Watch fan-out integration tests.
//!
//! Starts a single-node Raft cluster with a broadcast channel wired into the
//! FSM and the KvService, then exercises the Watch RPC: key-range filtering,
//! event ordering, cancel, and lag-based cancellation.
//!
//! Run with:
//!   cargo test -p ggap-server --test watch -- --nocapture

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, ServerState};
use tempfile::TempDir;
use tokio::net::TcpListener;

use ggap_consensus::{
    build_raft_config, GgapLogStorage, GgapNetworkFactory, GgapRaft, GgapStateMachine,
    OpenRaftCluster, OpenRaftNode, ShardRouter,
};
use ggap_server::{serve_client_with_listener, KvServiceConfig};
use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::ShardMap;
use ggap_types::{DomainWatchEvent, WatchEventKind};

use ggap_proto::v1::{
    kv_service_client::KvServiceClient, watch_request, EventType, PutRequest, WatchCreateRequest,
    WatchRequest,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct TestNode {
    raft: Arc<GgapRaft>,
    client_addr: String,
    /// Broadcast sender — can be used to inject events directly (bypassing Raft).
    watch_tx: tokio::sync::broadcast::Sender<DomainWatchEvent>,
    _handles: Vec<tokio::task::JoinHandle<()>>,
    _tempdir: TempDir,
}

async fn start_watch_node(broadcast_capacity: usize) -> TestNode {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();

    let shard_map = Arc::new(ShardMap::load(store.clone()).unwrap());
    shard_map.initialize_default().await.unwrap();

    let (watch_tx, _) = tokio::sync::broadcast::channel::<DomainWatchEvent>(broadcast_capacity);

    let fsm = Arc::new(FjallStateMachine::new(store.clone()).with_watch(watch_tx.clone()));

    let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), 0);
    let sm = GgapStateMachine::new(fsm.clone(), 0);
    let raft_cfg = build_raft_config(50, 150, 300, 500);
    let raft = Arc::new(
        GgapRaft::new(1, raft_cfg, GgapNetworkFactory::new(0), log_store, sm)
            .await
            .unwrap(),
    );

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members).await.unwrap();

    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "become leader")
        .await
        .expect("node should become leader");

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

    let client_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = format!("http://{}", client_listener.local_addr().unwrap());

    let r = router.clone();
    let kv_config = KvServiceConfig {
        max_key_bytes: 4096,
        max_value_bytes: 1_048_576,
        watch_tx: Some(watch_tx.clone()),
        watch_output_buffer: 128,
    };

    let mut handles = Vec::new();
    handles.push(tokio::spawn(async move {
        if let Err(e) = serve_client_with_listener(client_listener, r, 1, kv_config).await {
            eprintln!("client server: {e}");
        }
    }));

    TestNode {
        raft,
        client_addr,
        watch_tx,
        _handles: handles,
        _tempdir: tempdir,
    }
}

/// Helper to build a fake `DomainWatchEvent` for testing.
fn fake_put_event(key: &str) -> DomainWatchEvent {
    DomainWatchEvent {
        kind: WatchEventKind::Put,
        shard_id: 0,
        key: key.to_string(),
        entry: Some(ggap_types::KvEntry {
            key: key.to_string(),
            value: b"v".to_vec(),
            version: 1,
            created_at_ns: 0,
            modified_at_ns: 0,
            expires_at_ns: None,
        }),
        version: 1,
        raft_index: 0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Basic watch: write three keys, verify events arrive in order with correct types.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_receives_put_and_delete_events() {
    let node = start_watch_node(1024).await;

    let mut client = KvServiceClient::connect(node.client_addr.clone())
        .await
        .unwrap();

    // Open a watch stream on all keys.
    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<WatchRequest>(16);
    req_tx
        .send(WatchRequest {
            request: Some(watch_request::Request::Create(WatchCreateRequest {
                start_key: String::new(),
                end_key: String::new(),
                start_index: 0,
            })),
        })
        .await
        .unwrap();

    let watch_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);
    let mut watch_resp = client.watch(watch_stream).await.unwrap().into_inner();

    // Give the server a moment to set up the subscription.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Put two keys.
    client
        .put(PutRequest {
            key: "alpha".into(),
            value: b"v1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .put(PutRequest {
            key: "beta".into(),
            value: b"v2".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Delete one key.
    client
        .delete(ggap_proto::v1::DeleteRequest {
            key: "alpha".into(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Collect 3 events.
    let mut events = Vec::new();
    for _ in 0..3 {
        let evt = tokio::time::timeout(Duration::from_secs(5), watch_resp.message())
            .await
            .expect("timeout waiting for watch event")
            .expect("stream error")
            .expect("stream ended early");
        events.push(evt);
    }

    // First event: Put alpha
    assert_eq!(events[0].r#type, EventType::Put as i32);
    assert!(!events[0].canceled);
    let kv0 = events[0].kv.as_ref().unwrap();
    assert_eq!(kv0.key, "alpha");
    assert_eq!(kv0.value, b"v1");

    // Second event: Put beta
    assert_eq!(events[1].r#type, EventType::Put as i32);
    let kv1 = events[1].kv.as_ref().unwrap();
    assert_eq!(kv1.key, "beta");

    // Third event: Delete alpha
    assert_eq!(events[2].r#type, EventType::Delete as i32);

    // All events should carry the same non-zero watch_id.
    let wid = events[0].watch_id;
    assert!(wid > 0);
    assert_eq!(events[1].watch_id, wid);
    assert_eq!(events[2].watch_id, wid);

    drop(req_tx);
    node.raft.shutdown().await.unwrap();
}

/// Watch with key range filter: only keys in [start_key, end_key) are delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_key_range_filtering() {
    let node = start_watch_node(1024).await;

    let mut client = KvServiceClient::connect(node.client_addr.clone())
        .await
        .unwrap();

    // Watch only keys in ["b", "d").
    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<WatchRequest>(16);
    req_tx
        .send(WatchRequest {
            request: Some(watch_request::Request::Create(WatchCreateRequest {
                start_key: "b".into(),
                end_key: "d".into(),
                start_index: 0,
            })),
        })
        .await
        .unwrap();

    let watch_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);
    let mut watch_resp = client.watch(watch_stream).await.unwrap().into_inner();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write keys a, b, c, d — only b and c should be delivered.
    for key in &["a", "b", "c", "d"] {
        client
            .put(PutRequest {
                key: key.to_string(),
                value: b"val".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let mut events = Vec::new();
    for _ in 0..2 {
        let evt = tokio::time::timeout(Duration::from_secs(5), watch_resp.message())
            .await
            .expect("timeout")
            .expect("stream error")
            .expect("stream ended");
        events.push(evt);
    }

    assert_eq!(events[0].kv.as_ref().unwrap().key, "b");
    assert_eq!(events[1].kv.as_ref().unwrap().key, "c");

    // Ensure no spurious event arrives within a short window.
    let extra = tokio::time::timeout(Duration::from_millis(300), watch_resp.message()).await;
    assert!(extra.is_err(), "no extra event expected");

    drop(req_tx);
    node.raft.shutdown().await.unwrap();
}

/// When the broadcast buffer overflows (lag), the server sends canceled=true and closes.
///
/// We inject events directly into the broadcast channel (bypassing Raft) so we
/// can flood faster than tonic's HTTP/2 layer drains the output. With a tiny
/// broadcast capacity (2) and hundreds of rapid sends, the watch task's
/// broadcast receiver inevitably lags.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_lagged_receiver_gets_canceled() {
    // Tiny broadcast capacity so overflow is easy to trigger.
    let node = start_watch_node(2).await;

    let mut client = KvServiceClient::connect(node.client_addr.clone())
        .await
        .unwrap();

    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<WatchRequest>(16);
    req_tx
        .send(WatchRequest {
            request: Some(watch_request::Request::Create(WatchCreateRequest {
                start_key: String::new(),
                end_key: String::new(),
                start_index: 0,
            })),
        })
        .await
        .unwrap();

    let watch_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);
    let mut watch_resp = client.watch(watch_stream).await.unwrap().into_inner();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Flood the broadcast channel directly — no Raft bottleneck.
    // With capacity=2 and 500 synchronous sends, the watch task's receiver
    // will certainly lag because broadcast::send() never blocks the sender.
    for i in 0..500 {
        let _ = node.watch_tx.send(fake_put_event(&format!("key-{i:04}")));
    }

    // Drain events until we see canceled=true or the stream ends.
    let mut saw_canceled = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), watch_resp.message()).await {
            Ok(Ok(Some(evt))) => {
                if evt.canceled {
                    saw_canceled = true;
                    break;
                }
            }
            Ok(Ok(None)) => break,
            Ok(Err(_)) => break,
            Err(_) => panic!("timeout waiting for canceled event"),
        }
    }
    assert!(saw_canceled, "expected a canceled=true event on lag");

    drop(req_tx);
    node.raft.shutdown().await.unwrap();
}
