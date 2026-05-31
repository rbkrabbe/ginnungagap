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
use crate::metrics::record;

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

    async fn node_for_shard(&self, shard_id: u64) -> Result<Arc<OpenRaftNode>, Status> {
        self.router
            .get_node(shard_id)
            .await
            .ok_or_else(|| Status::not_found(format!("shard {shard_id} not found in router")))
    }

    async fn do_cluster_status(
        &self,
        req: ClusterStatusRequest,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let shard_id = req.shard_id.unwrap_or(0);
        let node = self.node_for_shard(shard_id).await?;
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

    async fn do_add_learner(
        &self,
        req: AddLearnerRequest,
    ) -> Result<Response<AddLearnerResponse>, Status> {
        let node_info = req
            .node
            .ok_or_else(|| Status::invalid_argument("node must be provided"))?;
        if node_info.node_id == 0 {
            return Err(Status::invalid_argument("node_id must be > 0"));
        }
        if node_info.cluster_addr.is_empty() {
            return Err(Status::invalid_argument("cluster_addr must not be empty"));
        }

        let raft_node = self.node_for_shard(0).await?;
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

    async fn do_change_membership(
        &self,
        req: ChangeMembershipRequest,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        if req.node_ids.is_empty() {
            return Err(Status::invalid_argument(
                "node_ids must contain at least one voter",
            ));
        }

        let node_ids: BTreeSet<u64> = req.node_ids.into_iter().collect();
        let raft_node = self.node_for_shard(0).await?;
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

    async fn do_split_shard(
        &self,
        req: SplitShardRequest,
    ) -> Result<Response<SplitShardResponse>, Status> {
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

    async fn do_list_shards(
        &self,
        _req: ListShardsRequest,
    ) -> Result<Response<ListShardsResponse>, Status> {
        let shards = self.shard_map.all_shards().await;
        let mut protos = Vec::with_capacity(shards.len());
        for s in shards {
            let consensus = self
                .router
                .get_node(s.shard_id)
                .await
                .map(|n| n.cluster_status());
            let (leader_id, voters, learners, term, last_applied) = match consensus {
                Some(cs) => (
                    cs.leader_id,
                    cs.voters.into_iter().map(|(id, _)| id).collect(),
                    cs.learners.into_iter().map(|(id, _)| id).collect(),
                    cs.term,
                    cs.last_applied,
                ),
                None => (None, vec![], vec![], 0, 0),
            };
            protos.push(ShardInfoProto {
                shard_id: s.shard_id,
                range_start: s.range.start,
                range_end: s.range.end,
                state: format!("{:?}", s.state),
                leader_id,
                voters,
                learners,
                term,
                last_applied,
            });
        }
        Ok(Response::new(ListShardsResponse { shards: protos }))
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn cluster_status(
        &self,
        request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_cluster_status(request.into_inner()).await;
        record(
            "ginnungagap.v1.AdminService/ClusterStatus",
            &result,
            start.elapsed(),
        );
        result
    }

    async fn add_learner(
        &self,
        request: Request<AddLearnerRequest>,
    ) -> Result<Response<AddLearnerResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_add_learner(request.into_inner()).await;
        record(
            "ginnungagap.v1.AdminService/AddLearner",
            &result,
            start.elapsed(),
        );
        result
    }

    async fn change_membership(
        &self,
        request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_change_membership(request.into_inner()).await;
        record(
            "ginnungagap.v1.AdminService/ChangeMembership",
            &result,
            start.elapsed(),
        );
        result
    }

    async fn split_shard(
        &self,
        request: Request<SplitShardRequest>,
    ) -> Result<Response<SplitShardResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_split_shard(request.into_inner()).await;
        record(
            "ginnungagap.v1.AdminService/SplitShard",
            &result,
            start.elapsed(),
        );
        result
    }

    async fn list_shards(
        &self,
        request: Request<ListShardsRequest>,
    ) -> Result<Response<ListShardsResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_list_shards(request.into_inner()).await;
        record(
            "ginnungagap.v1.AdminService/ListShards",
            &result,
            start.elapsed(),
        );
        result
    }
}
