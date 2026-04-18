use std::collections::BTreeSet;
use std::sync::Arc;

use ggap_consensus::{OpenRaftNode, ShardRouter, SplitCoordinator};
use ggap_proto::v1::{
    admin_service_server::AdminService, AddLearnerRequest, AddLearnerResponse,
    ChangeMembershipRequest, ChangeMembershipResponse, ClusterStatusRequest, ClusterStatusResponse,
    ListShardsRequest, ListShardsResponse, NodeInfo, ShardInfoProto, SplitShardRequest,
    SplitShardResponse,
};
use ggap_storage::ShardMap;
use tonic::{Request, Response, Status};

use crate::convert::ggap_to_status;

pub struct AdminServiceImpl {
    router: Arc<ShardRouter>,
    split_coordinator: Arc<SplitCoordinator>,
    shard_map: Arc<ShardMap>,
}

impl AdminServiceImpl {
    pub fn new(
        router: Arc<ShardRouter>,
        split_coordinator: Arc<SplitCoordinator>,
        shard_map: Arc<ShardMap>,
    ) -> Self {
        AdminServiceImpl {
            router,
            split_coordinator,
            shard_map,
        }
    }

    /// Look up the `OpenRaftNode` for shard 0 (the default shard in Phases 1–6).
    ///
    /// Admin operations target shard 0 because multi-raft (Phase 7) is deferred.
    async fn shard0_node(&self) -> Result<Arc<OpenRaftNode>, Status> {
        self.router
            .get_node(0)
            .await
            .ok_or_else(|| Status::internal("shard 0 not found in router"))
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let node = self.shard0_node().await?;
        let status = node.cluster_status();

        let to_node_info = |(node_id, cluster_addr): (u64, String)| NodeInfo {
            node_id,
            client_addr: String::new(),
            cluster_addr,
        };

        Ok(Response::new(ClusterStatusResponse {
            nodes: status.voters.into_iter().map(to_node_info).collect(),
            leader_id: status.leader_id,
            term: status.term,
            last_applied: status.last_applied,
            learners: status.learners.into_iter().map(to_node_info).collect(),
        }))
    }

    async fn add_learner(
        &self,
        request: Request<AddLearnerRequest>,
    ) -> Result<Response<AddLearnerResponse>, Status> {
        let req = request.into_inner();
        let node_info = req
            .node
            .ok_or_else(|| Status::invalid_argument("node must be provided"))?;
        if node_info.node_id == 0 {
            return Err(Status::invalid_argument("node_id must be > 0"));
        }
        if node_info.cluster_addr.is_empty() {
            return Err(Status::invalid_argument("cluster_addr must not be empty"));
        }

        let raft_node = self.shard0_node().await?;
        match raft_node
            .add_learner(node_info.node_id, node_info.cluster_addr)
            .await
        {
            Ok(()) => Ok(Response::new(AddLearnerResponse {
                ok: true,
                error: String::new(),
            })),
            Err(e) => {
                // NotLeader errors carry metadata; surface them as gRPC status.
                if matches!(&e, ggap_types::GgapError::NotLeader { .. }) {
                    return Err(ggap_to_status(e));
                }
                Ok(Response::new(AddLearnerResponse {
                    ok: false,
                    error: e.to_string(),
                }))
            }
        }
    }

    async fn change_membership(
        &self,
        request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        let req = request.into_inner();
        if req.node_ids.is_empty() {
            return Err(Status::invalid_argument(
                "node_ids must contain at least one voter",
            ));
        }

        let node_ids: BTreeSet<u64> = req.node_ids.into_iter().collect();
        let raft_node = self.shard0_node().await?;
        match raft_node.change_membership(node_ids).await {
            Ok(()) => Ok(Response::new(ChangeMembershipResponse {
                ok: true,
                error: String::new(),
            })),
            Err(e) => {
                if matches!(&e, ggap_types::GgapError::NotLeader { .. }) {
                    return Err(ggap_to_status(e));
                }
                Ok(Response::new(ChangeMembershipResponse {
                    ok: false,
                    error: e.to_string(),
                }))
            }
        }
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
            Err(e) => {
                if matches!(&e, ggap_types::GgapError::NotLeader { .. }) {
                    return Err(ggap_to_status(e));
                }
                Ok(Response::new(SplitShardResponse {
                    ok: false,
                    new_shard_id: 0,
                    error: e.to_string(),
                }))
            }
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
