//! Cross-shard scan integration tests.
//!
//! Exercises `KvServiceImpl::do_scan`'s cross-shard hop logic and the
//! `ScanContinuation` token round-trip via the public `KvService` trait.
//!
//! Run with:
//!   cargo test -p ggap-server --test cross_shard_scan -- --nocapture

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::ServerState;
use tempfile::TempDir;
use tonic::Request;

use ggap_consensus::{
    build_raft_config, run_split_handler, GgapLogStorage, GgapNetworkFactory, GgapNode, GgapRaft,
    GgapStateMachine, OpenRaftCluster, OpenRaftNode, ShardRouter, SplitCoordinator,
    SplitCoordinatorConfig,
};
use ggap_proto::v1::{kv_service_server::KvService, ScanRequest};
use ggap_server::{KvServiceConfig, KvServiceForTesting};
use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::ShardMap;
use ggap_types::{KvCommand, ScanContinuation};

struct TestSetup {
    router: Arc<ShardRouter>,
    split_coordinator: Arc<SplitCoordinator>,
    raft: Arc<GgapRaft>,
    service: KvServiceForTesting,
    _tempdir: TempDir,
}

async fn setup() -> TestSetup {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();

    let shard_map = Arc::new(ShardMap::load(store.clone()).unwrap());
    shard_map.initialize_default().await.unwrap();

    let (split_tx, split_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut fsm_builder = FjallStateMachine::new(store.clone());
    fsm_builder.set_split_sender(split_tx);
    fsm_builder.set_shard_map(shard_map.clone());
    let fsm = Arc::new(fsm_builder);

    let raft_cfg = build_raft_config(50, 150, 300, 500);
    let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), 0);
    let sm = GgapStateMachine::new(fsm.clone(), 0);
    let raft = Arc::new(
        GgapRaft::new(
            1,
            raft_cfg.clone(),
            GgapNetworkFactory::new(0),
            log_store,
            sm,
        )
        .await
        .unwrap(),
    );

    let mut members = BTreeMap::new();
    members.insert(1u64, GgapNode::default());
    raft.initialize(members).await.unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "become leader")
        .await
        .unwrap();

    let router = Arc::new(ShardRouter::new(shard_map.clone()));
    let node = Arc::new(OpenRaftNode::new(
        raft.clone(),
        fsm.clone(),
        0,
        1,
        tokio::time::Duration::from_millis(100),
    ));
    let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));
    router.add_shard(0, node, cluster).await;

    tokio::spawn(run_split_handler(
        split_rx,
        store.clone(),
        fsm.clone(),
        router.clone(),
        1,
        raft_cfg,
    ));

    let split_coordinator = Arc::new(SplitCoordinator::new(SplitCoordinatorConfig {
        router: router.clone(),
        shard_map: shard_map.clone(),
    }));

    let cfg = KvServiceConfig::default();
    let service = KvServiceForTesting::new(
        router.clone(),
        1,
        cfg.max_key_bytes,
        cfg.max_value_bytes,
        cfg.watch_tx,
        cfg.watch_output_buffer,
    );

    TestSetup {
        router,
        split_coordinator,
        raft,
        service,
        _tempdir: tempdir,
    }
}

async fn put(s: &TestSetup, key: &str) {
    s.raft
        .client_write(KvCommand::Put {
            key: key.into(),
            value: key.as_bytes().to_vec(),
            ttl_ns: None,
            expect_version: 0,
        })
        .await
        .unwrap();
}

/// Drives `do_scan` to completion, returning all keys observed and how many
/// RPCs it took. Each call uses the supplied `limit`.
async fn drain(
    service: &KvServiceForTesting,
    start_key: &str,
    end_key: &str,
    limit: u32,
) -> (Vec<String>, usize) {
    let mut keys = Vec::new();
    let mut token: Vec<u8> = Vec::new();
    let mut rpcs = 0usize;
    loop {
        rpcs += 1;
        assert!(rpcs < 200, "drain runaway after {rpcs} RPCs");
        let req = ScanRequest {
            start_key: if token.is_empty() {
                start_key.into()
            } else {
                String::new()
            },
            end_key: end_key.into(),
            limit,
            page_token: token.clone(),
            consistency: 0,
        };
        let resp = service.scan(Request::new(req)).await.unwrap().into_inner();
        for kv in resp.kvs {
            keys.push(kv.key);
        }
        if resp.next_page_token.is_empty() {
            break;
        }
        token = resp.next_page_token;
    }
    (keys, rpcs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_spans_two_shards() {
    let s = setup().await;

    let written: Vec<&str> = vec!["a", "c", "e", "g", "m", "o", "q", "s", "u", "y"];
    for k in &written {
        put(&s, k).await;
    }
    s.split_coordinator.split(0, "m").await.unwrap();
    assert_eq!(s.router.shard_map().all_shards().await.len(), 2);

    // Small limit forces multiple hops, exercising token round-trip across
    // the shard boundary at "m".
    let (keys, rpcs) = drain(&s.service, "", "", 3).await;
    assert_eq!(keys, written);
    assert!(rpcs >= 4, "expected paginated walk, got {rpcs} rpcs");

    s.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_spans_three_shards_with_empty_middle() {
    let s = setup().await;

    // Write only into ranges ["a", "m") and ["s", "z").
    for k in &["a", "c", "e", "g"] {
        put(&s, k).await;
    }
    for k in &["s", "u", "y"] {
        put(&s, k).await;
    }

    // Two splits: ["", "m"), ["m", "s"), ["s", "")
    s.split_coordinator.split(0, "m").await.unwrap();
    s.split_coordinator.split(1, "s").await.unwrap();
    assert_eq!(s.router.shard_map().all_shards().await.len(), 3);

    // Drain the scan; verify all keys observed and that the empty middle
    // shard was auto-hopped server-side (i.e. no RPC returns zero rows
    // until the scan is complete).
    let (keys, rpcs) = drain(&s.service, "", "", 100).await;
    assert_eq!(keys, vec!["a", "c", "e", "g", "s", "u", "y"]);
    // Expected hops: RPC 1 returns shard 0 contents + token at "m"; RPC 2
    // auto-skips empty shard 1, drains shard 2, returns no token. So <= 2
    // data-bearing RPCs. We do NOT want one RPC per shard.
    assert!(rpcs <= 2, "expected at most 2 RPCs, got {rpcs}");

    s.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_auto_hops_leading_empty_shards() {
    let s = setup().await;

    // Write only into the last shard's future range.
    for k in &["s", "u", "y"] {
        put(&s, k).await;
    }
    // Split: ["", "m"), ["m", "s"), ["s", "")
    s.split_coordinator.split(0, "m").await.unwrap();
    s.split_coordinator.split(1, "s").await.unwrap();
    assert_eq!(s.router.shard_map().all_shards().await.len(), 3);

    // The first two shards are empty. A single RPC should hop through them
    // and surface the data from shard 2 — exactly the case auto-hop is
    // intended to handle (no token ping-pong on a sparse keyspace).
    let req = ScanRequest {
        start_key: String::new(),
        end_key: String::new(),
        limit: 100,
        page_token: Vec::new(),
        consistency: 0,
    };
    let resp = s
        .service
        .scan(Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let observed: Vec<String> = resp.kvs.into_iter().map(|k| k.key).collect();
    assert_eq!(observed, vec!["s", "u", "y"]);
    assert!(
        resp.next_page_token.is_empty(),
        "expected scan to finish in one RPC"
    );

    s.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_token_after_intervening_split() {
    let s = setup().await;

    for k in &["a", "c", "e", "g", "m", "o", "q", "s", "u", "y"] {
        put(&s, k).await;
    }

    // Page 1: limit=3, no split yet.
    let req = ScanRequest {
        start_key: String::new(),
        end_key: String::new(),
        limit: 3,
        page_token: Vec::new(),
        consistency: 0,
    };
    let resp = s
        .service
        .scan(Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let page1: Vec<String> = resp.kvs.into_iter().map(|k| k.key).collect();
    assert_eq!(page1, vec!["a", "c", "e"]);
    let token = resp.next_page_token;
    assert!(!token.is_empty());

    // Token routes by key, not by shard id; after split the same next_key
    // still resolves correctly.
    let decoded = ScanContinuation::decode(&token).unwrap();
    assert_eq!(decoded.next_key, "g");

    // Split while client is mid-scan.
    s.split_coordinator.split(0, "m").await.unwrap();

    // Resume — drive remaining pages with small limit to force more hops
    // across the new boundary.
    let mut all = page1;
    let mut tok = token;
    loop {
        let req = ScanRequest {
            start_key: String::new(),
            end_key: String::new(),
            limit: 3,
            page_token: tok.clone(),
            consistency: 0,
        };
        let resp = s
            .service
            .scan(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        for k in resp.kvs {
            all.push(k.key);
        }
        if resp.next_page_token.is_empty() {
            break;
        }
        tok = resp.next_page_token;
    }

    // At-least-once: every originally-written key is observed. Duplicates
    // are permitted on the split boundary (none expected here because the
    // split atomically moved keys in the Raft log).
    let mut sorted = all.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted,
        vec!["a", "c", "e", "g", "m", "o", "q", "s", "u", "y"]
    );

    s.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_with_invalid_page_token_rejected() {
    let s = setup().await;

    let req = ScanRequest {
        start_key: String::new(),
        end_key: String::new(),
        limit: 10,
        page_token: vec![0xff, 0xff, 0xff],
        consistency: 0,
    };
    let err = s.service.scan(Request::new(req)).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    s.raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_respects_end_key_across_shards() {
    let s = setup().await;

    for k in &["a", "c", "m", "o", "u", "y"] {
        put(&s, k).await;
    }
    s.split_coordinator.split(0, "m").await.unwrap();

    // end_key = "p" sits inside shard 1; scan must stop at "p".
    let (keys, _rpcs) = drain(&s.service, "", "p", 100).await;
    assert_eq!(keys, vec!["a", "c", "m", "o"]);

    s.raft.shutdown().await.unwrap();
}
