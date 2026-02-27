mod admin_service;
mod convert;
mod kv_service;
mod raft_service;

use std::net::SocketAddr;
use std::sync::Arc;

use ggap_consensus::RaftNode;
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

pub async fn serve_cluster(addr: SocketAddr) -> anyhow::Result<()> {
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(ggap_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");
    tracing::info!(%addr, "cluster gRPC server starting");
    tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(RaftServiceImpl))
        .add_service(AdminServiceServer::new(AdminServiceImpl))
        .add_service(reflection)
        .serve(addr)
        .await
        .map_err(Into::into)
}
