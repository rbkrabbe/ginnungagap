use std::sync::Arc;

use ggap_consensus::{ClusterNode, ShardRouter};
use ggap_proto::v1::{raft_service_server::RaftService, RaftMessage};
use tonic::{Request, Response, Status, Streaming};

use crate::convert::ggap_to_status;

pub struct RaftServiceImpl {
    router: Arc<ShardRouter>,
}

impl RaftServiceImpl {
    pub fn new(router: Arc<ShardRouter>) -> Self {
        RaftServiceImpl { router }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn append_entries(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let msg = request.into_inner();
        let cluster = self
            .router
            .get_cluster(msg.shard_id)
            .await
            .ok_or_else(|| Status::not_found(format!("shard {} not found", msg.shard_id)))?;
        let out = cluster
            .append_entries(msg.data)
            .await
            .map_err(ggap_to_status)?;
        Ok(Response::new(RaftMessage {
            shard_id: msg.shard_id,
            data: out,
        }))
    }

    async fn vote(&self, request: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let msg = request.into_inner();
        let cluster = self
            .router
            .get_cluster(msg.shard_id)
            .await
            .ok_or_else(|| Status::not_found(format!("shard {} not found", msg.shard_id)))?;
        let out = cluster.vote(msg.data).await.map_err(ggap_to_status)?;
        Ok(Response::new(RaftMessage {
            shard_id: msg.shard_id,
            data: out,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<RaftMessage>, Status> {
        let mut stream = request.into_inner();
        let mut last_resp: Option<Vec<u8>> = None;
        let mut shard_id = 0u64;

        while let Some(msg) = stream.message().await? {
            shard_id = msg.shard_id;
            let cluster = self
                .router
                .get_cluster(msg.shard_id)
                .await
                .ok_or_else(|| Status::not_found(format!("shard {} not found", msg.shard_id)))?;
            let out = cluster
                .install_snapshot(msg.data)
                .await
                .map_err(ggap_to_status)?;
            last_resp = Some(out);
        }

        let data = last_resp.unwrap_or_default();
        Ok(Response::new(RaftMessage { shard_id, data }))
    }
}
