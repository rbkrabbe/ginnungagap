use std::sync::Arc;

use ggap_consensus::SplitCoordinator;
use ggap_proto::v1::{
    admin_service_server::AdminService, AddLearnerRequest, AddLearnerResponse,
    ChangeMembershipRequest, ChangeMembershipResponse, ClusterStatusRequest, ClusterStatusResponse,
    ListShardsRequest, ListShardsResponse, ShardInfoProto, SplitShardRequest, SplitShardResponse,
    TransferLeaderRequest, TransferLeaderResponse,
};
use ggap_storage::ShardMap;
use tonic::{Request, Response, Status};

pub struct AdminServiceImpl {
    split_coordinator: Arc<SplitCoordinator>,
    shard_map: Arc<ShardMap>,
}

impl AdminServiceImpl {
    pub fn new(split_coordinator: Arc<SplitCoordinator>, shard_map: Arc<ShardMap>) -> Self {
        AdminServiceImpl {
            split_coordinator,
            shard_map,
        }
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn add_learner(
        &self,
        _request: Request<AddLearnerRequest>,
    ) -> Result<Response<AddLearnerResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn change_membership(
        &self,
        _request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn transfer_leader(
        &self,
        _request: Request<TransferLeaderRequest>,
    ) -> Result<Response<TransferLeaderResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn split_shard(
        &self,
        request: Request<SplitShardRequest>,
    ) -> Result<Response<SplitShardResponse>, Status> {
        let req = request.into_inner();
        if req.split_key.is_empty() {
            return Err(Status::invalid_argument("split_key must not be empty"));
        }

        match self
            .split_coordinator
            .split(req.shard_id, &req.split_key)
            .await
        {
            Ok(new_shard_id) => Ok(Response::new(SplitShardResponse {
                ok: true,
                new_shard_id,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(SplitShardResponse {
                ok: false,
                new_shard_id: 0,
                error: e.to_string(),
            })),
        }
    }

    async fn list_shards(
        &self,
        _request: Request<ListShardsRequest>,
    ) -> Result<Response<ListShardsResponse>, Status> {
        let shards = self.shard_map.all_shards().await;
        let protos = shards
            .into_iter()
            .map(|s| ShardInfoProto {
                shard_id: s.shard_id,
                range_start: s.range.start,
                range_end: s.range.end,
                state: format!("{:?}", s.state),
            })
            .collect();
        Ok(Response::new(ListShardsResponse { shards: protos }))
    }
}
