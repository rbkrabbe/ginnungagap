mod admin_service;
mod convert;
mod gossip_service;
mod kv_service;
mod metrics;
mod raft_service;
mod tracing_layer;

use std::net::SocketAddr;
use std::sync::Arc;

use http::{HeaderName, HeaderValue, Method};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tower_http::cors::CorsLayer;

use ggap_consensus::{ShardRegistry, ShardRouter, SplitCoordinator};
use ggap_proto::v1::{
    admin_service_server::AdminServiceServer, gossip_service_server::GossipServiceServer,
    kv_service_server::KvServiceServer, raft_service_server::RaftServiceServer,
};
use ggap_storage::ShardMap;
use tonic_reflection::server::Builder as ReflectionBuilder;

use admin_service::AdminServiceImpl;
use gossip_service::GossipServiceImpl;
use kv_service::KvServiceImpl;
use raft_service::RaftServiceImpl;
use tracing_layer::OtelServerLayer;

// Re-exported so integration tests can construct the service and exercise
// cross-shard scan hopping without spinning up a real gRPC server.
pub use kv_service::KvServiceImpl as KvServiceForTesting;

// Re-exported so integration tests can query the admin status RPCs in-process
// (e.g. to assert gossip-derived consensus state for a non-hosted shard).
pub use admin_service::AdminServiceImpl as AdminServiceForTesting;

/// Configuration for the client-facing KV service.
#[derive(Clone, Debug)]
pub struct KvServiceConfig {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    /// Optional watch broadcast channel. When set, `Watch` RPCs are served;
    /// when absent, `Watch` returns `Unimplemented`.
    pub watch_tx: Option<tokio::sync::broadcast::Sender<ggap_types::DomainWatchEvent>>,
    /// Per-watch output buffer size (mpsc channel capacity between the
    /// broadcast consumer task and the gRPC response stream). Default: 128.
    pub watch_output_buffer: usize,
}

impl Default for KvServiceConfig {
    fn default() -> Self {
        KvServiceConfig {
            max_key_bytes: 4096,
            max_value_bytes: 1_048_576,
            watch_tx: None,
            watch_output_buffer: 128,
        }
    }
}

/// Build a CORS layer for the given allowed origins.
///
/// An empty `origins` list produces a restrictive layer (no origins allowed),
/// which is fine for the native h2 path where CORS is irrelevant.
fn build_cors_layer(origins: &[String]) -> CorsLayer {
    let allow_headers = [
        HeaderName::from_static("content-type"),
        HeaderName::from_static("x-grpc-web"),
        HeaderName::from_static("x-user-agent"),
        HeaderName::from_static("grpc-timeout"),
    ];
    let expose_headers = [
        HeaderName::from_static("grpc-status"),
        HeaderName::from_static("grpc-message"),
        HeaderName::from_static("grpc-status-details-bin"),
    ];

    let layer = CorsLayer::new()
        .allow_methods([Method::POST, Method::OPTIONS])
        .allow_headers(allow_headers)
        .expose_headers(expose_headers);

    if origins.is_empty() {
        return layer;
    }

    let parsed: Vec<HeaderValue> = origins.iter().filter_map(|o| o.parse().ok()).collect();

    layer.allow_origin(parsed)
}

pub async fn serve_client(
    addr: SocketAddr,
    router: Arc<ShardRouter>,
    node_id: u64,
    config: KvServiceConfig,
    cors_origins: Vec<String>,
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    let cors = build_cors_layer(&cors_origins);
    tracing::info!(%addr, "client gRPC server starting");
    tonic::transport::Server::builder()
        .accept_http1(true)
        .layer(cors)
        .layer(OtelServerLayer)
        .add_service(tonic_web::enable(KvServiceServer::new(KvServiceImpl::new(
            router,
            node_id,
            config.max_key_bytes,
            config.max_value_bytes,
            config.watch_tx,
            config.watch_output_buffer,
        ))))
        .add_service(reflection)
        .serve(addr)
        .await
        .map_err(Into::into)
}

/// Variant of [`serve_client`] that accepts a pre-bound [`TcpListener`].
pub async fn serve_client_with_listener(
    listener: TcpListener,
    router: Arc<ShardRouter>,
    node_id: u64,
    config: KvServiceConfig,
    cors_origins: Vec<String>,
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    let cors = build_cors_layer(&cors_origins);
    tonic::transport::Server::builder()
        .accept_http1(true)
        .layer(cors)
        .layer(OtelServerLayer)
        .add_service(tonic_web::enable(KvServiceServer::new(KvServiceImpl::new(
            router,
            node_id,
            config.max_key_bytes,
            config.max_value_bytes,
            config.watch_tx,
            config.watch_output_buffer,
        ))))
        .add_service(reflection)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .map_err(Into::into)
}

pub async fn serve_cluster(
    addr: SocketAddr,
    router: Arc<ShardRouter>,
    split_coordinator: Arc<SplitCoordinator>,
    shard_map: Arc<ShardMap>,
    registry: Arc<ShardRegistry>,
    cors_origins: Vec<String>,
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    let cors = build_cors_layer(&cors_origins);
    tracing::info!(%addr, "cluster gRPC server starting");
    let admin = AdminServiceImpl::new(
        router.clone(),
        split_coordinator,
        shard_map,
        registry.clone(),
    );
    tonic::transport::Server::builder()
        .accept_http1(true)
        .layer(cors)
        .layer(OtelServerLayer)
        .add_service(tonic_web::enable(RaftServiceServer::new(
            RaftServiceImpl::new(router),
        )))
        .add_service(tonic_web::enable(AdminServiceServer::new(admin)))
        .add_service(tonic_web::enable(GossipServiceServer::new(
            GossipServiceImpl::new(registry),
        )))
        .add_service(reflection)
        .serve(addr)
        .await
        .map_err(Into::into)
}

/// Variant of [`serve_cluster`] that accepts a pre-bound [`TcpListener`].
pub async fn serve_cluster_with_listener(
    listener: TcpListener,
    router: Arc<ShardRouter>,
    split_coordinator: Arc<SplitCoordinator>,
    shard_map: Arc<ShardMap>,
    registry: Arc<ShardRegistry>,
    cors_origins: Vec<String>,
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    let cors = build_cors_layer(&cors_origins);
    let admin = AdminServiceImpl::new(
        router.clone(),
        split_coordinator,
        shard_map,
        registry.clone(),
    );
    tonic::transport::Server::builder()
        .accept_http1(true)
        .layer(cors)
        .layer(OtelServerLayer)
        .add_service(tonic_web::enable(RaftServiceServer::new(
            RaftServiceImpl::new(router),
        )))
        .add_service(tonic_web::enable(AdminServiceServer::new(admin)))
        .add_service(tonic_web::enable(GossipServiceServer::new(
            GossipServiceImpl::new(registry),
        )))
        .add_service(reflection)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .map_err(Into::into)
}
