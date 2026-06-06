use std::sync::Arc;

use ggap_consensus::gossip::entry_to_proto;
use ggap_consensus::{merge_gossip_state, ShardRegistry};
use ggap_proto::v1::{gossip_service_server::GossipService, GossipNode, GossipState};
use tonic::{Request, Response, Status};

use crate::metrics::record;

/// Serves the push-pull anti-entropy `Exchange` RPC: merge the caller's view
/// into our [`ShardRegistry`], then return our own full view back so both sides
/// converge. Served on the cluster port alongside RaftService and AdminService.
pub struct GossipServiceImpl {
    registry: Arc<ShardRegistry>,
}

impl GossipServiceImpl {
    pub fn new(registry: Arc<ShardRegistry>) -> Self {
        GossipServiceImpl { registry }
    }

    async fn do_exchange(&self, incoming: GossipState) -> Result<Response<GossipState>, Status> {
        merge_gossip_state(&self.registry, incoming).await;

        let (dir, shards) = self.registry.snapshot_for_gossip().await;
        Ok(Response::new(GossipState {
            sender_node_id: self.registry.self_node_id(),
            directory: dir
                .into_iter()
                .map(|(node_id, cluster_addr)| GossipNode {
                    node_id,
                    cluster_addr,
                })
                .collect(),
            shards: shards.into_iter().map(entry_to_proto).collect(),
        }))
    }
}

#[tonic::async_trait]
impl GossipService for GossipServiceImpl {
    async fn exchange(
        &self,
        request: Request<GossipState>,
    ) -> Result<Response<GossipState>, Status> {
        let start = tokio::time::Instant::now();
        let result = self.do_exchange(request.into_inner()).await;
        record(
            "ginnungagap.v1.GossipService/Exchange",
            &result,
            start.elapsed(),
        );
        result
    }
}
