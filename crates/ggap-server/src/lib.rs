mod admin_service;
mod convert;
mod kv_service;
mod raft_service;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use ggap_consensus::{ClusterNode, RaftNode};
use ggap_proto::v1::{
    admin_service_server::AdminServiceServer, kv_service_server::KvServiceServer,
    raft_service_server::RaftServiceServer,
};
use tonic_reflection::server::Builder as ReflectionBuilder;

use admin_service::AdminServiceImpl;
use kv_service::KvServiceImpl;
use raft_service::RaftServiceImpl;

pub async fn serve_client<R: RaftNode>(
    addr: SocketAddr,
    raft: Arc<R>,
    node_id: u64,
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    tracing::info!(%addr, "client gRPC server starting");
    tonic::transport::Server::builder()
        .add_service(KvServiceServer::new(KvServiceImpl::new(raft, node_id)))
        .add_service(reflection)
        .serve(addr)
        .await
        .map_err(Into::into)
}

/// Variant of [`serve_client`] that accepts a pre-bound [`TcpListener`].
///
/// Useful in tests where you need to know the bound port before initialising
/// the Raft cluster (bind port 0, extract the address, then call this).
pub async fn serve_client_with_listener<R: RaftNode>(
    listener: TcpListener,
    raft: Arc<R>,
    node_id: u64,
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    tonic::transport::Server::builder()
        .add_service(KvServiceServer::new(KvServiceImpl::new(raft, node_id)))
        .add_service(reflection)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .map_err(Into::into)
}

pub async fn serve_cluster<CN: ClusterNode>(
    addr: SocketAddr,
    cluster: Arc<CN>,
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    tracing::info!(%addr, "cluster gRPC server starting");
    tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(RaftServiceImpl::new(cluster)))
        .add_service(AdminServiceServer::new(AdminServiceImpl))
        .add_service(reflection)
        .serve(addr)
        .await
        .map_err(Into::into)
}

/// Variant of [`serve_cluster`] that accepts a pre-bound [`TcpListener`].
pub async fn serve_cluster_with_listener<CN: ClusterNode>(
    listener: TcpListener,
    cluster: Arc<CN>,
) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(RaftServiceImpl::new(cluster)))
        .add_service(AdminServiceServer::new(AdminServiceImpl))
        .add_service(reflection)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .map_err(Into::into)
}
