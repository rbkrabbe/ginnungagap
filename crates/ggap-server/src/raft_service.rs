use std::sync::Arc;

use ggap_consensus::{ClusterNode, ShardRouter};
use ggap_proto::v1::{raft_service_server::RaftService, RaftMessage};
use tonic::{Request, Response, Status, Streaming};

use crate::convert::ggap_to_status;
use crate::metrics::record;

pub struct RaftServiceImpl {
    router: Arc<ShardRouter>,
}

// These helpers feed the tonic trait methods directly, so their error type is
// `tonic::Status` whether we like it or not — 176 bytes against the lint's
// 128-byte threshold.
#[allow(clippy::result_large_err)]
impl RaftServiceImpl {
    pub fn new(router: Arc<ShardRouter>) -> Self {
        RaftServiceImpl { router }
    }

    async fn do_append_entries(&self, msg: RaftMessage) -> Result<Response<RaftMessage>, Status> {
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

    async fn do_vote(&self, msg: RaftMessage) -> Result<Response<RaftMessage>, Status> {
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

    async fn do_install_snapshot(
        &self,
        mut stream: Streaming<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let mut last_resp: Option<Vec<u8>> = None;
        let mut shard_id = 0u64;

        while let Some(msg) = stream.message().await? {
            shard_id = msg.shard_id;
            let cluster =
                self.router.get_cluster(msg.shard_id).await.ok_or_else(|| {
                    Status::not_found(format!("shard {} not found", msg.shard_id))
                })?;
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

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn append_entries(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_append_entries(request.into_inner()).await;
        record(
            "ginnungagap.v1.RaftService/AppendEntries",
            &result,
            start.elapsed(),
        );
        result
    }

    async fn vote(&self, request: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_vote(request.into_inner()).await;
        record("ginnungagap.v1.RaftService/Vote", &result, start.elapsed());
        result
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<RaftMessage>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_install_snapshot(request.into_inner()).await;
        record(
            "ginnungagap.v1.RaftService/InstallSnapshot",
            &result,
            start.elapsed(),
        );
        result
    }
}
