use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, ServerState};

use ggap_storage::fjall::{FjallStateMachine, FjallStore};
use ggap_storage::ShardMap;
use ggap_types::{GgapError, KeyRange, ShardId, ShardInfo, ShardState};

use crate::config::build_raft_config;
use crate::log_store::GgapLogStorage;
use crate::network::GgapNetworkFactory;
use crate::node::{GgapRaft, OpenRaftCluster, OpenRaftNode};
use crate::router::ShardRouter;
use crate::state_machine::GgapStateMachine;

/// Parameters needed to construct a `SplitCoordinator`.
pub struct SplitCoordinatorConfig {
    pub router: Arc<ShardRouter>,
    pub shard_map: Arc<ShardMap>,
    pub store: Arc<FjallStore>,
    pub fsm: Arc<FjallStateMachine>,
    pub node_id: u64,
    pub cluster_addr: String,
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

/// Coordinates the split of a shard's key range into two shards.
///
/// The split protocol blocks writes on the source shard during the split
/// to ensure consistency. The steps are:
///
/// 1. Validate the split request
/// 2. Mark source shard as Splitting (blocks writes via router)
/// 3. Write barrier: propose a no-op and wait for commit
/// 4. Build partial snapshot of keys >= split_key
/// 5. Bootstrap new Raft group with new ShardId
/// 6. Install partial snapshot on new shard
/// 7. Delete transferred keys from source shard
/// 8. Update ShardMap with new ranges
/// 9. Resume writes on source shard (state → Active)
pub struct SplitCoordinator {
    router: Arc<ShardRouter>,
    shard_map: Arc<ShardMap>,
    store: Arc<FjallStore>,
    fsm: Arc<FjallStateMachine>,
    node_id: u64,
    cluster_addr: String,
    heartbeat_ms: u64,
    election_min_ms: u64,
    election_max_ms: u64,
}

impl SplitCoordinator {
    pub fn new(cfg: SplitCoordinatorConfig) -> Self {
        SplitCoordinator {
            router: cfg.router,
            shard_map: cfg.shard_map,
            store: cfg.store,
            fsm: cfg.fsm,
            node_id: cfg.node_id,
            cluster_addr: cfg.cluster_addr,
            heartbeat_ms: cfg.heartbeat_ms,
            election_min_ms: cfg.election_min_ms,
            election_max_ms: cfg.election_max_ms,
        }
    }

    /// Execute a shard split at the given key.
    ///
    /// Returns the new shard id on success.
    pub async fn split(&self, shard_id: ShardId, split_key: &str) -> Result<ShardId, GgapError> {
        // 1. Validate
        let source_info = self
            .shard_map
            .get_shard(shard_id)
            .await
            .ok_or(GgapError::ShardNotFound(shard_id))?;

        if source_info.state != ShardState::Active {
            return Err(GgapError::InvalidArgument(
                "shard is not in Active state".into(),
            ));
        }

        if !source_info.range.contains(split_key) {
            return Err(GgapError::InvalidArgument(format!(
                "split_key '{}' is not within shard {}'s range [{}, {})",
                split_key, shard_id, source_info.range.start, source_info.range.end
            )));
        }

        if split_key == source_info.range.start {
            return Err(GgapError::InvalidArgument(
                "split_key cannot be the shard's start key".into(),
            ));
        }

        // 2. Mark source shard as Splitting
        let splitting_info = ShardInfo {
            shard_id,
            range: source_info.range.clone(),
            state: ShardState::Splitting,
        };
        self.shard_map.put_shard(splitting_info).await?;

        // From here on, if we fail, we need to restore the source shard to Active.
        let result = self.do_split(shard_id, split_key, &source_info).await;

        match result {
            Ok(new_shard_id) => Ok(new_shard_id),
            Err(e) => {
                // Restore source shard to Active on failure
                tracing::error!(
                    shard_id,
                    split_key,
                    error = %e,
                    "split failed, restoring source shard to Active"
                );
                let restored = ShardInfo {
                    shard_id,
                    range: source_info.range.clone(),
                    state: ShardState::Active,
                };
                let _ = self.shard_map.put_shard(restored).await;
                Err(e)
            }
        }
    }

    async fn do_split(
        &self,
        shard_id: ShardId,
        split_key: &str,
        source_info: &ShardInfo,
    ) -> Result<ShardId, GgapError> {
        // 3. Write barrier: propose a no-op through the source shard's Raft
        let source_node = self
            .router
            .get_node(shard_id)
            .await
            .ok_or(GgapError::ShardNotFound(shard_id))?;

        // Use a Put with empty key as a barrier — just ensure linearizability
        source_node
            .raft()
            .ensure_linearizable()
            .await
            .map_err(|e| GgapError::Consensus(format!("write barrier failed: {e}")))?;

        // 4. Build partial snapshot of keys >= split_key
        let contents = self.fsm.build_partial_snapshot(shard_id, split_key).await?;

        // 5. Allocate new shard id
        let new_shard_id = self.shard_map.next_shard_id().await;

        // 6. Install partial snapshot on new shard
        self.fsm
            .install_partial_snapshot(new_shard_id, &contents)
            .await?;

        // 7. Delete transferred keys from source shard
        self.fsm.delete_range_from(shard_id, split_key).await?;

        // 8. Update ShardMap: narrow source, add new shard
        let updated_source = ShardInfo {
            shard_id,
            range: KeyRange {
                start: source_info.range.start.clone(),
                end: split_key.to_string(),
            },
            state: ShardState::Active,
        };
        let new_shard = ShardInfo {
            shard_id: new_shard_id,
            range: KeyRange {
                start: split_key.to_string(),
                end: source_info.range.end.clone(),
            },
            state: ShardState::Active,
        };
        self.shard_map.put_shard(updated_source).await?;
        self.shard_map.put_shard(new_shard).await?;

        // 9. Bootstrap new Raft group for the new shard
        let log_store = GgapLogStorage::new(self.store.clone(), new_shard_id);
        let sm = GgapStateMachine::new(self.fsm.clone(), new_shard_id);
        let net = GgapNetworkFactory::new(new_shard_id);
        let cfg = build_raft_config(
            self.heartbeat_ms,
            self.election_min_ms,
            self.election_max_ms,
        );

        let raft = Arc::new(
            GgapRaft::new(self.node_id, cfg, net, log_store, sm)
                .await
                .map_err(|e| {
                    GgapError::Consensus(format!("failed to create Raft for new shard: {e}"))
                })?,
        );

        // Initialize as single-node cluster
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: self.cluster_addr.clone(),
            },
        );
        raft.initialize(members).await.map_err(|e| {
            GgapError::Consensus(format!("failed to initialize new shard Raft: {e}"))
        })?;

        // Wait for the new shard to become leader
        raft.wait(Some(Duration::from_secs(5)))
            .state(ServerState::Leader, "new shard become leader")
            .await
            .map_err(|e| GgapError::Consensus(format!("new shard did not become leader: {e}")))?;

        // 10. Register in router
        let new_node = Arc::new(OpenRaftNode::new(
            raft.clone(),
            self.fsm.clone(),
            new_shard_id,
            self.node_id,
        ));
        let new_cluster = Arc::new(OpenRaftCluster::new(raft));
        self.router
            .add_shard(new_shard_id, new_node, new_cluster)
            .await;

        tracing::info!(
            shard_id,
            new_shard_id,
            split_key,
            "shard split completed successfully"
        );

        Ok(new_shard_id)
    }
}
