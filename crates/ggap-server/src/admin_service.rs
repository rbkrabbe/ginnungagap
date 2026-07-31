use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ggap_consensus::{OpenRaftNode, ShardEntry, ShardRegistry, ShardRouter, SplitCoordinator};
use ggap_proto::v1::{
    admin_service_server::AdminService, AddLearnerRequest, AddLearnerResponse,
    ChangeMembershipRequest, ChangeMembershipResponse, ClusterStatusRequest, ClusterStatusResponse,
    ListShardsRequest, ListShardsResponse, NodeInfo, ShardInfoProto, SplitShardRequest,
    SplitShardResponse,
};
use ggap_storage::ShardMap;
use ggap_types::NodeAddrs;
use tonic::{Request, Response, Status};

use crate::convert::ggap_to_status;
use crate::metrics::record;

pub struct AdminServiceImpl {
    router: Arc<ShardRouter>,
    split_coordinator: Arc<SplitCoordinator>,
    shard_map: Arc<ShardMap>,
    /// Cluster-wide gossiped view, used to answer status for shards this node
    /// does not host locally.
    registry: Arc<ShardRegistry>,
}

impl AdminServiceImpl {
    pub fn new(
        router: Arc<ShardRouter>,
        split_coordinator: Arc<SplitCoordinator>,
        shard_map: Arc<ShardMap>,
        registry: Arc<ShardRegistry>,
    ) -> Self {
        AdminServiceImpl {
            router,
            split_coordinator,
            shard_map,
            registry,
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

        // Membership carries both addresses, so a locally hosted shard answers
        // in full without gossip having run at all.
        let to_node_info = |(node_id, addrs): (u64, NodeAddrs)| NodeInfo {
            node_id,
            client_addr: addrs.client_addr,
            cluster_addr: addrs.cluster_addr,
        };

        // Prefer fresh local openraft metrics when we host the shard.
        if let Some(node) = self.router.get_node(shard_id).await {
            let status = node.cluster_status();
            return Ok(Response::new(ClusterStatusResponse {
                nodes: status.voters.into_iter().map(to_node_info).collect(),
                leader_id: status.leader_id,
                term: status.term,
                last_applied: status.last_applied,
                learners: status.learners.into_iter().map(to_node_info).collect(),
                last_observed_age_ms: Some(0),
            }));
        }

        // Otherwise answer from the gossiped registry. A miss is not an error:
        // return zeroed consensus with `last_observed_age_ms = None` so any
        // shard is answerable from any node and consumers can tell "no snapshot"
        // from a real leaderless state.
        match self.registry.lookup(shard_id).await {
            Some(entry) => {
                let age = entry.age_ms();
                Ok(Response::new(ClusterStatusResponse {
                    nodes: self.ids_to_node_infos(&entry.voters).await,
                    leader_id: entry.leader_id,
                    term: entry.term,
                    last_applied: entry.last_applied,
                    learners: self.ids_to_node_infos(&entry.learners).await,
                    last_observed_age_ms: Some(age),
                }))
            }
            None => Ok(Response::new(ClusterStatusResponse {
                nodes: vec![],
                leader_id: None,
                term: 0,
                last_applied: 0,
                learners: vec![],
                last_observed_age_ms: None,
            })),
        }
    }

    /// Resolve node ids to `NodeInfo`, filling both addresses from the gossiped
    /// directory where known (empty otherwise).
    ///
    /// Only used for shards this node does not host; a hosted shard reads
    /// membership directly and needs no directory lookup.
    async fn ids_to_node_infos(&self, ids: &[u64]) -> Vec<NodeInfo> {
        let mut out = Vec::with_capacity(ids.len());
        for &node_id in ids {
            out.push(NodeInfo {
                node_id,
                client_addr: self.registry.client_addr(node_id).await.unwrap_or_default(),
                cluster_addr: self
                    .registry
                    .directory_addr(node_id)
                    .await
                    .unwrap_or_default(),
            });
        }
        out
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
        // Rejected rather than accepted-as-empty: membership is now the source of
        // truth for the client address, so a learner joining without one is a node
        // nothing in the cluster can ever forward a client request to, and nothing
        // later fills the gap in. Failing the join is the only loud moment
        // available — silently admitting it defers the surprise to whoever tries
        // to forward months later.
        if node_info.client_addr.is_empty() {
            return Err(Status::invalid_argument("client_addr must not be empty"));
        }

        let raft_node = self.node_for_shard(req.shard_id.unwrap_or(0)).await?;
        match raft_node
            .add_learner(
                node_info.node_id,
                NodeAddrs::new(node_info.cluster_addr, node_info.client_addr),
            )
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

        let shard_id = req.shard_id.unwrap_or(0);
        let node_ids: BTreeSet<u64> = req.node_ids.into_iter().collect();
        let raft_node = self.node_for_shard(shard_id).await?;
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
        // Authoritative shard list = union of the local shard map and shards we
        // only know about via gossip. Local shard-map metadata (range/state)
        // wins where both are present.
        let mut meta: BTreeMap<u64, (String, String, String)> = BTreeMap::new();
        for e in self.registry.all().await {
            meta.entry(e.shard_id)
                .or_insert((e.range_start, e.range_end, e.state));
        }
        for s in self.shard_map.all_shards().await {
            meta.insert(
                s.shard_id,
                (s.range.start, s.range.end, format!("{:?}", s.state)),
            );
        }

        let mut protos = Vec::with_capacity(meta.len());
        for (shard_id, (range_start, range_end, state)) in meta {
            // Prefer fresh local openraft metrics; fall back to gossip; else
            // honest zeros with `last_observed_age_ms = None`.
            let proto = if let Some(node) = self.router.get_node(shard_id).await {
                let cs = node.cluster_status();
                ShardInfoProto {
                    shard_id,
                    range_start,
                    range_end,
                    state,
                    leader_id: cs.leader_id,
                    voters: cs.voters.into_iter().map(|(id, _)| id).collect(),
                    learners: cs.learners.into_iter().map(|(id, _)| id).collect(),
                    term: cs.term,
                    last_applied: cs.last_applied,
                    last_observed_age_ms: Some(0),
                }
            } else if let Some(entry) = self.registry.lookup(shard_id).await {
                entry_to_shard_info(shard_id, range_start, range_end, state, entry)
            } else {
                ShardInfoProto {
                    shard_id,
                    range_start,
                    range_end,
                    state,
                    leader_id: None,
                    voters: vec![],
                    learners: vec![],
                    term: 0,
                    last_applied: 0,
                    last_observed_age_ms: None,
                }
            };
            protos.push(proto);
        }
        Ok(Response::new(ListShardsResponse { shards: protos }))
    }
}

/// Build a `ShardInfoProto` from a gossiped registry entry, stamping the
/// snapshot age so consumers can judge freshness.
fn entry_to_shard_info(
    shard_id: u64,
    range_start: String,
    range_end: String,
    state: String,
    entry: ShardEntry,
) -> ShardInfoProto {
    let age = entry.age_ms();
    ShardInfoProto {
        shard_id,
        range_start,
        range_end,
        state,
        leader_id: entry.leader_id,
        voters: entry.voters,
        learners: entry.learners,
        term: entry.term,
        last_applied: entry.last_applied,
        last_observed_age_ms: Some(age),
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
