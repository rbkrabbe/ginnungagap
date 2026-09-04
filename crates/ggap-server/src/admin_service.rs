use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ggap_consensus::gossip::broadcast_removal;
use ggap_consensus::{OpenRaftNode, ShardEntry, ShardRegistry, ShardRouter, SplitCoordinator};
use ggap_proto::v1::{
    admin_service_client::AdminServiceClient, admin_service_server::AdminService,
    AddLearnerRequest, AddLearnerResponse, ChangeMembershipRequest, ChangeMembershipResponse,
    ClusterStatusRequest, ClusterStatusResponse, ListShardsRequest, ListShardsResponse, NodeInfo,
    RemoveNodeRequest, RemoveNodeResponse, ShardInfoProto, SplitShardRequest, SplitShardResponse,
};
use ggap_storage::ShardMap;
use ggap_types::NodeAddrs;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
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
    /// Cancelled when this node has retired itself, so the binary can shut the
    /// process down. `None` in harnesses that have no process to end — they
    /// observe the tombstone in the registry instead.
    retired: Option<CancellationToken>,
}

/// How long `RemoveNode` waits on the node it is retiring, and on the peers it
/// hands the tombstone to. An operator RPC, so a couple of seconds of patience
/// costs nothing and a hang costs the call.
const REMOVE_RPC_TIMEOUT: Duration = Duration::from_secs(2);

// These helpers feed the tonic trait methods directly, so their error type is
// `tonic::Status` whether we like it or not — 176 bytes against the lint's
// 128-byte threshold.
#[allow(clippy::result_large_err)]
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
            retired: None,
        }
    }

    /// Give this service a token to cancel once the node has retired itself.
    pub fn with_retire_token(mut self, retired: CancellationToken) -> Self {
        self.retired = Some(retired);
        self
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

        // Prefer fresh local openraft metrics when we host the shard. Consensus
        // state is local and current — hence `last_observed_age_ms = 0` — while
        // the ids it names are resolved through the directory, exactly as they
        // are for a shard we only know by gossip.
        if let Some(node) = self.router.get_node(shard_id).await {
            let status = node.cluster_status();
            return Ok(Response::new(ClusterStatusResponse {
                nodes: self.ids_to_node_infos(&status.voters).await,
                leader_id: status.leader_id,
                term: status.term,
                last_applied: status.last_applied,
                learners: self.ids_to_node_infos(&status.learners).await,
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

    /// Resolve node ids to `NodeInfo`, filling both addresses from the
    /// directory where known (empty otherwise).
    ///
    /// The directory is the only source of addresses, so hosted and non-hosted
    /// shards resolve the same way; only where the *ids* come from differs.
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
        // A learner without a client address is one nothing can forward a client
        // request to, and only another membership change could fix it.
        if node_info.client_addr.is_empty() {
            return Err(Status::invalid_argument("client_addr must not be empty"));
        }

        // A retired id can never be resolved again, so a learner added under one
        // would be in the membership and unreachable forever.
        if self.registry.is_retired(node_info.node_id).await {
            return Err(Status::failed_precondition(format!(
                "node {} was removed from the cluster; a retired id is never reused",
                node_info.node_id
            )));
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

    /// Retire a node from the directory, cluster-wide and for good.
    ///
    /// Whoever is asked either *is* the target or forwards to it: only the
    /// target knows which shards it still belongs to, and only the target can
    /// author its own descriptor. A target that cannot be reached is the sole
    /// exception, and the only way to retire hardware that is already gone.
    async fn do_remove_node(
        &self,
        req: RemoveNodeRequest,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        if req.node_id == 0 {
            return Err(Status::invalid_argument("node_id must be > 0"));
        }
        if req.node_id == self.registry.self_node_id() {
            return self.retire_self().await;
        }
        if self.registry.is_retired(req.node_id).await {
            // Already done, and a removal is never undone — so say so rather
            // than dialling an address the tombstone has already erased.
            return Ok(Response::new(RemoveNodeResponse {
                ok: true,
                error: String::new(),
                confirmed_by_node: false,
            }));
        }

        // An entry that maps another id to this node's own address is the
        // corruption tk-8d80's join check exists to prevent. Forwarding to it
        // would dial ourselves and recurse until the timeouts collapse it, and
        // tombstoning "on its behalf" would retire a stranger on the strength of
        // a bad entry. Neither is a removal, so refuse and say what is wrong.
        let self_addr = self
            .registry
            .directory_addr(self.registry.self_node_id())
            .await;
        if let (Some(target), Some(mine)) = (
            self.registry.directory_addr(req.node_id).await,
            self_addr.as_ref(),
        ) {
            if target == *mine {
                return Err(Status::failed_precondition(format!(
                    "node {}'s directory entry names this node's own address {target}; \
                     the entry is wrong, and removing it would retire the wrong node",
                    req.node_id
                )));
            }
        }

        match self.forward_removal(req.node_id).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => {
                // Unreachable. Tombstone on its behalf: this is what a
                // decommissioned node's entry needs, and there is nobody left
                // to write it. tk-ad1d gates this on the node being demonstrably
                // dead rather than merely silent right now.
                tracing::warn!(
                    node_id = req.node_id,
                    error = %e,
                    "removal target did not answer; tombstoning it on its behalf"
                );
                self.registry.retire(req.node_id).await;
                Ok(Response::new(RemoveNodeResponse {
                    ok: true,
                    error: String::new(),
                    confirmed_by_node: false,
                }))
            }
        }
    }

    /// Ask the target to retire itself. `Err` means it could not be asked, not
    /// that it refused — a refusal comes back as an `ok: false` response.
    async fn forward_removal(&self, node_id: u64) -> Result<RemoveNodeResponse, String> {
        let addr = self
            .registry
            .directory_addr(node_id)
            .await
            .ok_or_else(|| format!("no directory entry for node {node_id}"))?;

        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| e.to_string())?;
        let channel = tokio::time::timeout(REMOVE_RPC_TIMEOUT, endpoint.connect())
            .await
            .map_err(|_| "connect timed out".to_string())?
            .map_err(|e| e.to_string())?;

        let resp = tokio::time::timeout(
            REMOVE_RPC_TIMEOUT,
            AdminServiceClient::new(channel).remove_node(RemoveNodeRequest { node_id }),
        )
        .await
        .map_err(|_| "remove_node timed out".to_string())?
        .map_err(|e| e.to_string())?
        .into_inner();

        Ok(resp)
    }

    /// The authoritative half: this node names the shards it still belongs to
    /// from its own Raft membership, refuses while there are any, and otherwise
    /// tombstones itself and pushes that out before it stops.
    async fn retire_self(&self) -> Result<Response<RemoveNodeResponse>, Status> {
        let self_id = self.registry.self_node_id();

        let mut still_in: Vec<u64> = Vec::new();
        for shard_id in self.router.local_shard_ids().await {
            let Some(node) = self.router.get_node(shard_id).await else {
                continue;
            };
            let status = node.cluster_status();
            if status.voters.contains(&self_id) || status.learners.contains(&self_id) {
                still_in.push(shard_id);
            }
        }
        if !still_in.is_empty() {
            still_in.sort_unstable();
            let shards = still_in
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            return Ok(Response::new(RemoveNodeResponse {
                ok: false,
                error: format!(
                    "node {self_id} is still a member of shard(s) {shards}; \
                     change membership to drop it before removing it"
                ),
                confirmed_by_node: true,
            }));
        }

        // A tombstone nobody else has heard dies with this process, and this
        // process is about to end — so hand it over before recording it. A
        // removal cannot be taken back, so a node that reached nobody must not
        // be left retired in its own directory and running.
        //
        // Delivery is counted by the response, so a peer that merged the
        // tombstone and then failed to answer is counted as a miss: the node
        // stays up and reports failure while the cluster has already retired it.
        // Erring this way is deliberate — the operator retries and the node
        // exits, where the opposite error leaves a node retired that nobody
        // else believes is gone.
        let peers = self.registry.peers_excluding_self().await.len();
        let delivered =
            broadcast_removal(&self.registry, self_id, self_id, REMOVE_RPC_TIMEOUT).await;
        if delivered == 0 && peers > 0 {
            return Ok(Response::new(RemoveNodeResponse {
                ok: false,
                error: format!(
                    "node {self_id} reached none of its {peers} peer(s), so the removal \
                     would be lost when it stops; it stays in the cluster"
                ),
                confirmed_by_node: true,
            }));
        }
        self.registry.retire(self_id).await;

        tracing::info!(
            node_id = self_id,
            delivered,
            "retired from the directory; shutting down"
        );
        if let Some(retired) = &self.retired {
            retired.cancel();
        }
        Ok(Response::new(RemoveNodeResponse {
            ok: true,
            error: String::new(),
            confirmed_by_node: true,
        }))
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
                    voters: cs.voters,
                    learners: cs.learners,
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

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_remove_node(request.into_inner()).await;
        record(
            "ginnungagap.v1.AdminService/RemoveNode",
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
