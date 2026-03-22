use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use ggap_storage::ShardMap;
use ggap_types::{GgapError, ShardId, ShardState};

use crate::node::{OpenRaftCluster, OpenRaftNode};

/// Routes KV requests to the correct shard's `RaftNode` based on key ranges,
/// and routes inbound Raft RPCs to the correct shard's `ClusterNode`.
pub struct ShardRouter {
    shard_map: Arc<ShardMap>,
    nodes: RwLock<HashMap<ShardId, Arc<OpenRaftNode>>>,
    clusters: RwLock<HashMap<ShardId, Arc<OpenRaftCluster>>>,
}

impl ShardRouter {
    pub fn new(shard_map: Arc<ShardMap>) -> Self {
        ShardRouter {
            shard_map,
            nodes: RwLock::new(HashMap::new()),
            clusters: RwLock::new(HashMap::new()),
        }
    }

    pub fn shard_map(&self) -> &Arc<ShardMap> {
        &self.shard_map
    }

    /// Register a shard's RaftNode and ClusterNode.
    pub async fn add_shard(
        &self,
        shard_id: ShardId,
        node: Arc<OpenRaftNode>,
        cluster: Arc<OpenRaftCluster>,
    ) {
        self.nodes.write().await.insert(shard_id, node);
        self.clusters.write().await.insert(shard_id, cluster);
    }

    /// Remove a shard's registration.
    pub async fn remove_shard(&self, shard_id: ShardId) {
        self.nodes.write().await.remove(&shard_id);
        self.clusters.write().await.remove(&shard_id);
    }

    /// Look up the RaftNode for a read operation on the given key.
    pub async fn route_read(&self, key: &str) -> Result<Arc<OpenRaftNode>, GgapError> {
        let info = self
            .shard_map
            .lookup_shard(key)
            .await
            .ok_or_else(|| GgapError::InvalidArgument(format!("no shard found for key '{key}'")))?;

        let nodes = self.nodes.read().await;
        nodes
            .get(&info.shard_id)
            .cloned()
            .ok_or(GgapError::WrongShard {
                shard_id: info.shard_id,
                range: info.range,
            })
    }

    /// Look up the RaftNode for a write operation on the given key.
    /// Returns `ShardSplitting` if the shard is currently being split.
    pub async fn route_write(&self, key: &str) -> Result<Arc<OpenRaftNode>, GgapError> {
        let info = self
            .shard_map
            .lookup_shard(key)
            .await
            .ok_or_else(|| GgapError::InvalidArgument(format!("no shard found for key '{key}'")))?;

        if info.state == ShardState::Splitting {
            return Err(GgapError::ShardSplitting);
        }

        let nodes = self.nodes.read().await;
        nodes
            .get(&info.shard_id)
            .cloned()
            .ok_or(GgapError::WrongShard {
                shard_id: info.shard_id,
                range: info.range,
            })
    }

    /// Look up the RaftNode for a scan operation.
    /// For correctness, the entire scan range must be within a single shard.
    pub async fn route_scan(
        &self,
        start_key: &str,
        end_key: &str,
    ) -> Result<Arc<OpenRaftNode>, GgapError> {
        let info = self.shard_map.lookup_shard(start_key).await.ok_or_else(|| {
            GgapError::InvalidArgument(format!("no shard found for key '{start_key}'"))
        })?;

        // If end_key is specified and falls outside this shard's range, reject
        if !end_key.is_empty() && !info.range.contains(end_key) {
            // Check that end_key is at most the shard boundary
            // (end_key == shard.range.end is acceptable as an exclusive bound)
            if !info.range.end.is_empty() && end_key > info.range.end.as_str() {
                return Err(GgapError::InvalidArgument(
                    "scan range spans multiple shards".into(),
                ));
            }
        }

        let nodes = self.nodes.read().await;
        nodes
            .get(&info.shard_id)
            .cloned()
            .ok_or(GgapError::WrongShard {
                shard_id: info.shard_id,
                range: info.range,
            })
    }

    /// Get the ClusterNode for a specific shard (for inbound Raft RPCs).
    pub async fn get_cluster(&self, shard_id: ShardId) -> Option<Arc<OpenRaftCluster>> {
        self.clusters.read().await.get(&shard_id).cloned()
    }

    /// Get the RaftNode for a specific shard by id.
    pub async fn get_node(&self, shard_id: ShardId) -> Option<Arc<OpenRaftNode>> {
        self.nodes.read().await.get(&shard_id).cloned()
    }
}
