use ggap_proto::v1::{raft_service_server::RaftService, RaftMessage};
use tonic::{Request, Response, Status, Streaming};

pub struct RaftServiceImpl;

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn append_entries(
        &self,
        _request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        Err(Status::unimplemented("Phase 4"))
    }

    async fn vote(
        &self,
        _request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        Err(Status::unimplemented("Phase 4"))
    }

    async fn install_snapshot(
        &self,
        _request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<RaftMessage>, Status> {
        Err(Status::unimplemented("Phase 4"))
    }
}
