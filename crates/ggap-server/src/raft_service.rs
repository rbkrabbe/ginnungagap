use std::sync::Arc;

use ggap_consensus::ClusterNode;
use ggap_proto::v1::{raft_service_server::RaftService, RaftMessage};
use tonic::{Request, Response, Status, Streaming};

use crate::convert::ggap_to_status;

pub struct RaftServiceImpl<CN> {
    cluster: Arc<CN>,
}

impl<CN: ClusterNode> RaftServiceImpl<CN> {
    pub fn new(cluster: Arc<CN>) -> Self {
        RaftServiceImpl { cluster }
    }
}

#[tonic::async_trait]
impl<CN: ClusterNode> RaftService for RaftServiceImpl<CN> {
    async fn append_entries(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let payload = request.into_inner().data;
        let out = self.cluster.append_entries(payload).await.map_err(ggap_to_status)?;
        Ok(Response::new(RaftMessage { data: out }))
    }

    async fn vote(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let payload = request.into_inner().data;
        let out = self.cluster.vote(payload).await.map_err(ggap_to_status)?;
        Ok(Response::new(RaftMessage { data: out }))
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<RaftMessage>, Status> {
        let mut stream = request.into_inner();
        let mut last_resp: Option<Vec<u8>> = None;

        while let Some(msg) = stream.message().await? {
            let out = self
                .cluster
                .install_snapshot(msg.data)
                .await
                .map_err(ggap_to_status)?;
            last_resp = Some(out);
        }

        let data = last_resp.unwrap_or_default();
        Ok(Response::new(RaftMessage { data }))
    }
}
