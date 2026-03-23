mod admin_service;
mod convert;
mod kv_service;
mod raft_service;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use ggap_consensus::{ShardRouter, SplitCoordinator};
use ggap_proto::v1::{
    admin_service_server::AdminServiceServer, kv_service_server::KvServiceServer,
    raft_service_server::RaftServiceServer,
};
use ggap_storage::ShardMap;
use tonic_reflection::server::Builder as ReflectionBuilder;

use admin_service::AdminServiceImpl;
use kv_service::KvServiceImpl;
use raft_service::RaftServiceImpl;

/// Configuration for the client-facing KV service.
#[derive(Clone, Debug)]
pub struct KvServiceConfig {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
}

impl Default for KvServiceConfig {
    fn default() -> Self {
        KvServiceConfig {
            max_key_bytes: 4096,
            max_value_bytes: 1_048_576,
        }
    }
}

pub async fn serve_client(
    addr: SocketAddr,
    router: Arc<ShardRouter>,
    node_id: u64,
    config: KvServiceConfig,
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    tracing::info!(%addr, "client gRPC server starting");
    tonic::transport::Server::builder()
        .add_service(KvServiceServer::new(KvServiceImpl::new(
            router,
            node_id,
            config.max_key_bytes,
            config.max_value_bytes,
        )))
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
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    tonic::transport::Server::builder()
        .add_service(KvServiceServer::new(KvServiceImpl::new(
            router,
            node_id,
            config.max_key_bytes,
            config.max_value_bytes,
        )))
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
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    tracing::info!(%addr, "cluster gRPC server starting");
    tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(RaftServiceImpl::new(router)))
        .add_service(AdminServiceServer::new(AdminServiceImpl::new(
            split_coordinator,
            shard_map,
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
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(RaftServiceImpl::new(router)))
        .add_service(AdminServiceServer::new(AdminServiceImpl::new(
            split_coordinator,
            shard_map,
        )))
        .add_service(reflection)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .map_err(Into::into)
}
