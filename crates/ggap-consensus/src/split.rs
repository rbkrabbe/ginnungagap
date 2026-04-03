use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, ServerState};

use ggap_storage::fjall::{FjallLogStorage, FjallStateMachine, FjallStore};
use ggap_storage::SplitApplied;
use ggap_storage::ShardMap;
use ggap_types::{GgapError, KvCommand, KvResponse, ShardId, ShardInfo, ShardState};

use crate::log_store::GgapLogStorage;
use crate::network::GgapNetworkFactory;
use crate::node::{GgapRaft, OpenRaftCluster, OpenRaftNode};
use crate::router::ShardRouter;
use crate::state_machine::GgapStateMachine;

/// Parameters needed to construct a `SplitCoordinator`.
pub struct SplitCoordinatorConfig {
    pub router: Arc<ShardRouter>,
    pub shard_map: Arc<ShardMap>,
}

/// Coordinates the split of a shard's key range into two shards.
///
/// The split is driven via Raft: a `KvCommand::Split` is proposed through the
/// source shard's log, so every node applies the data movement deterministically.
/// The new shard's Raft group is then bootstrapped with the source membership.
pub struct SplitCoordinator {
    router: Arc<ShardRouter>,
    shard_map: Arc<ShardMap>,
}

impl SplitCoordinator {
    pub fn new(cfg: SplitCoordinatorConfig) -> Self {
        SplitCoordinator {
            router: cfg.router,
            shard_map: cfg.shard_map,
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

        // 2. Mark source shard as Splitting (blocks writes via router).
        let splitting_info = ShardInfo {
            shard_id,
            range: source_info.range.clone(),
            state: ShardState::Splitting,
        };
        self.shard_map.put_shard(splitting_info).await?;

        // 3. Execute the split.
        // do_split() returns Err only if the KvCommand::Split was NOT committed to
        // the Raft log (pre-commit failures). Once the command is committed the
        // ShardMap is already updated by apply() on every node, so on post-commit
        // failures do_split() logs a warning and returns Ok (the split is done).
        let result = self.do_split(shard_id, split_key, &source_info).await;

        if let Err(ref e) = result {
            // The Raft propose failed — the split was not applied. Restore source.
            tracing::error!(
                shard_id,
                split_key,
                error = %e,
                "split failed before commit, restoring source shard to Active"
            );
            let restored = ShardInfo {
                shard_id,
                range: source_info.range.clone(),
                state: ShardState::Active,
            };
            let _ = self.shard_map.put_shard(restored).await;
        }

        result
    }

    async fn do_split(
        &self,
        shard_id: ShardId,
        split_key: &str,
        source_info: &ShardInfo,
    ) -> Result<ShardId, GgapError> {
        // 1. Get source node's Raft handle.
        let source_node = self
            .router
            .get_node(shard_id)
            .await
            .ok_or(GgapError::ShardNotFound(shard_id))?;

        // 2. Read source shard membership from Raft metrics.
        let source_members: BTreeMap<u64, BasicNode> = {
            let metrics = source_node.raft().metrics().borrow().clone();
            metrics
                .membership_config
                .membership()
                .nodes()
                .map(|(id, node)| (*id, node.clone()))
                .collect()
        };

        // 3. Allocate new shard id.
        let new_shard_id = self.shard_map.next_shard_id().await;

        // 4. Propose KvCommand::Split through the source shard's Raft log.
        //    This is the write barrier: ordered after all prior writes.
        //    Every node's apply() will:
        //      a) Move keys >= split_key to new_shard_id
        //      b) Delete those keys from source shard
        //      c) Update ShardMap (narrow source, add new shard)
        //      d) Signal the background split handler via SplitApplied channel
        let cmd = KvCommand::Split {
            split_key: split_key.to_string(),
            new_shard_id,
            source_range: source_info.range.clone(),
            source_members: source_members
                .iter()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect(),
        };
        let write_result = source_node
            .raft()
            .client_write(cmd)
            .await
            .map_err(|e| {
                if let Some(fwd) = e.forward_to_leader() {
                    let leader_addr =
                        fwd.leader_node.as_ref().map(|n: &BasicNode| n.addr.clone());
                    return GgapError::NotLeader {
                        leader: leader_addr,
                    };
                }
                GgapError::Consensus(format!("Split propose failed: {e}"))
            })?;

        // Validate the response.
        match write_result.data {
            KvResponse::SplitComplete { new_shard_id: id } if id == new_shard_id => {}
            ref other => {
                return Err(GgapError::Consensus(format!(
                    "unexpected response from Split command: {other:?}"
                )));
            }
        }

        // ── The Split command is now committed. The ShardMap has been updated on
        //    every node that has applied the entry. From here, post-commit failures
        //    are logged as warnings but do not cause a rollback (the data is split). ──

        // 5. Wait for the new shard's Raft instance to appear in the router.
        //    The background split handler creates it after receiving SplitApplied.
        let new_node = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(node) = self.router.get_node(new_shard_id).await {
                    break node;
                }
                if tokio::time::Instant::now() >= deadline {
                    tracing::warn!(
                        shard_id,
                        new_shard_id,
                        "timed out waiting for new shard Raft instance in router"
                    );
                    return Ok(new_shard_id);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };

        // 6. Initialize new shard's Raft with source membership.
        //    openraft retries will handle followers whose background handler hasn't
        //    created the Raft instance yet.
        if let Err(e) = new_node.raft().initialize(source_members).await {
            tracing::warn!(
                shard_id,
                new_shard_id,
                error = %e,
                "failed to initialize new shard Raft (will retry on next split attempt)"
            );
            return Ok(new_shard_id);
        }

        // 7. Wait for new shard leader election.
        if let Err(e) = new_node
            .raft()
            .wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "new shard become leader")
            .await
        {
            tracing::warn!(
                shard_id,
                new_shard_id,
                error = %e,
                "new shard did not elect a leader within timeout"
            );
        }

        tracing::info!(
            shard_id,
            new_shard_id,
            split_key,
            "shard split completed successfully"
        );

        Ok(new_shard_id)
    }
}

// ---------------------------------------------------------------------------
// Background split handler — spawned on every node
// ---------------------------------------------------------------------------

/// Background task that runs on every node. When a `KvCommand::Split` is applied,
/// `FjallStateMachine::apply()` sends a `SplitApplied` event through the channel.
/// This task receives the event and bootstraps the new shard's Raft group,
/// then registers it in the router.
pub async fn run_split_handler(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<SplitApplied>,
    store: Arc<FjallStore>,
    fsm: Arc<FjallStateMachine>,
    router: Arc<ShardRouter>,
    node_id: u64,
    raft_config: Arc<openraft::Config>,
) {
    while let Some(event) = rx.recv().await {
        let new_shard_id = event.new_shard_id;
        tracing::info!(new_shard_id, "background split handler: bootstrapping new shard");

        let log_store = GgapLogStorage::new(FjallLogStorage(store.clone()), new_shard_id);
        let sm = GgapStateMachine::new(fsm.clone(), new_shard_id);
        let net = GgapNetworkFactory::new(new_shard_id);

        let raft = match GgapRaft::new(node_id, raft_config.clone(), net, log_store, sm).await {
            Ok(r) => Arc::new(r),
            Err(e) => {
                tracing::error!(
                    new_shard_id,
                    error = %e,
                    "failed to create Raft for new shard in background handler"
                );
                continue;
            }
        };

        let node = Arc::new(OpenRaftNode::new(
            raft.clone(),
            fsm.clone(),
            new_shard_id,
            node_id,
            // Lease duration of zero — the coordinator will initialize membership.
            tokio::time::Duration::from_secs(0),
        ));
        let cluster = Arc::new(OpenRaftCluster::new(raft));
        router.add_shard(new_shard_id, node, cluster).await;

        tracing::info!(
            new_shard_id,
            "background split handler: new shard registered in router"
        );
    }
}
