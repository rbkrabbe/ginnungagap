//! CORS preflight integration test.
//!
//! Starts a minimal single-node cluster server with one configured origin and
//! sends a raw HTTP/1.1 OPTIONS preflight request over a plain TCP connection.
//! Asserts that the response echoes the allowed origin back in
//! `access-control-allow-origin`.
//!
//! This test deliberately avoids reqwest/hyper so it exercises the actual
//! HTTP/1.1 path on the tonic-web + tower-http-cors stack.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use ggap_consensus::{
    build_raft_config, run_split_handler, GgapLogStorage, GgapNetworkFactory, GgapNode, GgapRaft,
    GgapStateMachine, OpenRaftCluster, OpenRaftNode, ShardRegistry, ShardRouter, SplitCoordinator,
    SplitCoordinatorConfig,
};
use ggap_server::{serve_cluster_with_listener, ClusterServiceConfig};
use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::ShardMap;

#[tokio::test]
async fn cors_preflight_returns_allow_origin() {
    let tempdir = TempDir::new().unwrap();
    let store = FjallStore::open(tempdir.path()).unwrap();

    let shard_map = Arc::new(ShardMap::load(store.clone()).unwrap());
    shard_map.initialize_default().await.unwrap();

    let (split_tx, split_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut fsm_builder = FjallStateMachine::new(store.clone());
    fsm_builder.set_split_sender(split_tx);
    fsm_builder.set_shard_map(shard_map.clone());
    let fsm = Arc::new(fsm_builder);

    // A single-node Raft dials nobody, so an empty directory is enough.
    let registry = Arc::new(ShardRegistry::new(1, []));
    let raft_cfg = build_raft_config(50, 150, 300, 500);
    let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), 0);
    let sm = GgapStateMachine::new(fsm.clone(), 0);
    let raft = Arc::new(
        GgapRaft::new(
            1,
            raft_cfg.clone(),
            GgapNetworkFactory::new(0, registry.clone()),
            log_store,
            sm,
        )
        .await
        .unwrap(),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let router = Arc::new(ShardRouter::new(shard_map.clone()));
    let node = Arc::new(OpenRaftNode::new(
        raft.clone(),
        fsm.clone(),
        0,
        1,
        registry.clone(),
        Duration::from_millis(300),
    ));
    let cluster = Arc::new(OpenRaftCluster::new(raft.clone()));
    router.add_shard(0, node, cluster).await;

    let split_coordinator = Arc::new(SplitCoordinator::new(SplitCoordinatorConfig {
        router: router.clone(),
        shard_map: shard_map.clone(),
        registry: registry.clone(),
    }));

    tokio::spawn(run_split_handler(
        split_rx,
        store.clone(),
        fsm.clone(),
        router.clone(),
        1,
        raft_cfg,
        registry.clone(),
    ));

    // Bootstrap single-node Raft.
    let members = BTreeMap::from([(1u64, GgapNode::default())]);
    raft.initialize(members).await.unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(|m| m.current_leader.is_some(), "leader elected")
        .await
        .unwrap();

    let allowed_origin = "http://localhost:5173";
    tokio::spawn(serve_cluster_with_listener(
        listener,
        router,
        split_coordinator,
        shard_map,
        registry,
        ClusterServiceConfig::with_cors_origins(vec![allowed_origin.to_string()]),
    ));

    // Give the server a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a raw HTTP/1.1 OPTIONS preflight request.
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "OPTIONS /ginnungagap.v1.AdminService/ListShards HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Origin: {allowed_origin}\r\n\
         Access-Control-Request-Method: POST\r\n\
         Access-Control-Request-Headers: content-type,x-grpc-web\r\n\
         Connection: close\r\n\
         \r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();

    let lower = response.to_lowercase();
    assert!(
        lower.contains(&format!("access-control-allow-origin: {allowed_origin}")),
        "expected 'access-control-allow-origin: {allowed_origin}' in response:\n{response}"
    );
    assert!(
        lower.contains("access-control-allow-methods"),
        "expected CORS allow-methods header:\n{response}"
    );
}
