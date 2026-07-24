use std::collections::BTreeSet;
use std::sync::Arc;

use ggap_storage::fjall::FjallStateMachine;
use ggap_storage::traits::StateMachineStore;
use ggap_types::{GgapError, KvCommand, KvEntry, KvResponse, ReadMode, ShardId, WriteMode};
use openraft::{
    raft::{AppendEntriesRequest, VoteRequest},
    BasicNode, ChangeMembers, Raft,
};

use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::config::GgapTypeConfig;
use crate::convert::{decode, encode};
use crate::RaftNode;

pub type GgapRaft = Raft<GgapTypeConfig>;

// ---------------------------------------------------------------------------
// ClusterNode trait (bytes in / bytes out)
// ---------------------------------------------------------------------------

/// Inbound cluster RPC handler — bytes in, bytes out.
///
/// Keeps openraft types out of `ggap-server`'s dep tree.
pub trait ClusterNode: Send + Sync + 'static {
    fn append_entries(
        &self,
        payload: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, GgapError>> + Send;

    fn vote(
        &self,
        payload: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, GgapError>> + Send;

    fn install_snapshot(
        &self,
        payload: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, GgapError>> + Send;
}

// ---------------------------------------------------------------------------
// ClusterStatus — return type for OpenRaftNode::cluster_status()
// ---------------------------------------------------------------------------

pub struct ClusterStatus {
    pub term: u64,
    pub leader_id: Option<u64>,
    pub last_applied: u64,
    pub voters: Vec<(u64, String)>,
    pub learners: Vec<(u64, String)>,
}

// ---------------------------------------------------------------------------
// OpenRaftNode
// ---------------------------------------------------------------------------

pub struct OpenRaftNode {
    raft: Arc<GgapRaft>,
    fsm: Arc<FjallStateMachine>,
    shard_id: ShardId,
    #[allow(dead_code)]
    node_id: u64,
    lease: tokio::sync::Mutex<LeaseManager>,
}

impl OpenRaftNode {
    pub fn new(
        raft: Arc<GgapRaft>,
        fsm: Arc<FjallStateMachine>,
        shard_id: ShardId,
        node_id: u64,
        lease_duration: tokio::time::Duration,
    ) -> Self {
        OpenRaftNode {
            raft,
            fsm,
            shard_id,
            node_id,
            lease: tokio::sync::Mutex::new(LeaseManager::new(lease_duration)),
        }
    }

    /// Access the underlying Raft instance (e.g. for ensure_linearizable).
    pub fn raft(&self) -> &Arc<GgapRaft> {
        &self.raft
    }

    /// Run a linearizable read, using the lease shortcut when valid.
    ///
    /// If this node is the current leader and the lease is still within its
    /// validity window, skip the ReadIndex round-trip and serve from the
    /// local FSM directly. Otherwise fall back to `ensure_linearizable()` and
    /// renew the lease on success.
    async fn ensure_linearizable_or_lease(&self) -> Result<(), GgapError> {
        let metrics = self.raft.metrics().borrow().clone();
        let is_leader = metrics.state == openraft::ServerState::Leader;
        if is_leader && self.lease.lock().await.is_valid(metrics.current_term) {
            return Ok(());
        }
        self.raft.ensure_linearizable().await.map_err(|e| {
            if let Some(fwd) = e.forward_to_leader() {
                let leader_addr = fwd.leader_node.as_ref().map(|n: &BasicNode| n.addr.clone());
                return GgapError::NotLeader {
                    leader: leader_addr,
                };
            }
            GgapError::Consensus(e.to_string())
        })?;
        let term = self.raft.metrics().borrow().current_term;
        self.lease.lock().await.renew(term);
        Ok(())
    }

    // -----------------------------------------------------------------
    // Admin operations — cluster management via openraft APIs
    // -----------------------------------------------------------------

    pub fn cluster_status(&self) -> ClusterStatus {
        let m = self.raft.metrics().borrow().clone();
        let last_applied = m.last_applied.map(|log_id| log_id.index).unwrap_or(0);

        let membership = m.membership_config.membership();
        let voter_ids: BTreeSet<u64> = membership.voter_ids().collect();

        let mut voters = Vec::new();
        let mut learners = Vec::new();
        for (nid, node) in membership.nodes() {
            let pair = (*nid, node.addr.clone());
            if voter_ids.contains(nid) {
                voters.push(pair);
            } else {
                learners.push(pair);
            }
        }

        ClusterStatus {
            term: m.current_term,
            leader_id: m.current_leader,
            last_applied,
            voters,
            learners,
        }
    }

    /// Add a node as a non-voting learner to the Raft group.
    pub async fn add_learner(&self, node_id: u64, addr: String) -> Result<(), GgapError> {
        self.raft
            .add_learner(node_id, BasicNode { addr }, false)
            .await
            .map_err(|e| {
                if let Some(fwd) = e.forward_to_leader() {
                    let leader_addr = fwd.leader_node.as_ref().map(|n: &BasicNode| n.addr.clone());
                    return GgapError::NotLeader {
                        leader: leader_addr,
                    };
                }
                GgapError::Consensus(e.to_string())
            })?;
        Ok(())
    }

    /// Replace the voter set with the given node IDs.
    ///
    /// All supplied `node_ids` must already be learners or voters.
    /// This triggers a joint-consensus membership change that commits
    /// in two phases through Raft.
    ///
    /// Voters removed from the set are demoted to learners (`retain = true`)
    /// rather than ejected from the cluster, so the operation is reversible
    /// and cannot accidentally cause permanent quorum loss.
    pub async fn change_membership(&self, node_ids: BTreeSet<u64>) -> Result<(), GgapError> {
        self.raft
            .change_membership(
                ChangeMembers::<u64, BasicNode>::ReplaceAllVoters(node_ids),
                true,
            )
            .await
            .map_err(|e| {
                if let Some(fwd) = e.forward_to_leader() {
                    let leader_addr = fwd.leader_node.as_ref().map(|n: &BasicNode| n.addr.clone());
                    return GgapError::NotLeader {
                        leader: leader_addr,
                    };
                }
                GgapError::Consensus(e.to_string())
            })?;
        Ok(())
    }
}

impl RaftNode for OpenRaftNode {
    fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    async fn propose(&self, cmd: KvCommand, _mode: WriteMode) -> Result<KvResponse, GgapError> {
        // Create an apply.entry span anchored to the current rpc.server span.
        // openraft's state machine apply runs inside client_write() on an
        // internal task that doesn't carry the caller's trace context; creating
        // the span here (before the await) bridges the trace gap without
        // modifying KvCommand or the Raft log.
        let span = tracing::info_span!(
            "apply.entry",
            otel.kind = "internal",
            shard_id = self.shard_id,
        );
        span.set_parent(tracing::Span::current().context());

        self.raft
            .client_write(cmd)
            .instrument(span)
            .await
            .map(|r| r.data)
            .map_err(|e| {
                if let Some(fwd) = e.forward_to_leader() {
                    let leader_addr = fwd.leader_node.as_ref().map(|n: &BasicNode| n.addr.clone());
                    return GgapError::NotLeader {
                        leader: leader_addr,
                    };
                }
                GgapError::Consensus(e.to_string())
            })
    }

    async fn read(
        &self,
        key: &str,
        at_version: u64,
        mode: ReadMode,
    ) -> Result<Option<KvEntry>, GgapError> {
        if mode == ReadMode::Linearizable {
            self.ensure_linearizable_or_lease().await?;
        }
        self.fsm.get(self.shard_id, key, at_version).await
    }

    async fn scan(
        &self,
        start_key: &str,
        end_key: &str,
        limit: u32,
        mode: ReadMode,
    ) -> Result<(Vec<KvEntry>, Option<String>), GgapError> {
        if mode == ReadMode::Linearizable {
            self.ensure_linearizable_or_lease().await?;
        }
        self.fsm
            .scan(self.shard_id, start_key, end_key, limit)
            .await
    }
}

// ---------------------------------------------------------------------------
// OpenRaftCluster
// ---------------------------------------------------------------------------

pub struct OpenRaftCluster {
    raft: Arc<GgapRaft>,
}

impl OpenRaftCluster {
    pub fn new(raft: Arc<GgapRaft>) -> Self {
        OpenRaftCluster { raft }
    }
}

impl ClusterNode for OpenRaftCluster {
    async fn append_entries(&self, payload: Vec<u8>) -> Result<Vec<u8>, GgapError> {
        let req = decode::<AppendEntriesRequest<GgapTypeConfig>>(&payload)?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| GgapError::Consensus(e.to_string()))?;
        encode(&resp)
    }

    async fn vote(&self, payload: Vec<u8>) -> Result<Vec<u8>, GgapError> {
        let req = decode::<VoteRequest<u64>>(&payload)?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| GgapError::Consensus(e.to_string()))?;
        encode(&resp)
    }

    async fn install_snapshot(&self, payload: Vec<u8>) -> Result<Vec<u8>, GgapError> {
        let req = decode::<openraft::raft::InstallSnapshotRequest<GgapTypeConfig>>(&payload)?;
        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| GgapError::Consensus(e.to_string()))?;
        encode(&resp)
    }
}

// ---------------------------------------------------------------------------
// LeaseManager
// ---------------------------------------------------------------------------

pub struct LeaseManager {
    acquired_at: Option<tokio::time::Instant>,
    acquired_at_term: u64,
    duration: tokio::time::Duration,
}

impl LeaseManager {
    pub fn new(duration: tokio::time::Duration) -> Self {
        LeaseManager {
            acquired_at: None,
            acquired_at_term: 0,
            duration,
        }
    }

    /// Returns `true` if the lease was acquired in `current_term` and has not
    /// yet expired. A term change invalidates the lease even if the time
    /// window hasn't elapsed — prevents stale reads across leadership changes.
    pub fn is_valid(&self, current_term: u64) -> bool {
        if self.acquired_at_term != current_term {
            return false;
        }
        self.acquired_at
            .map(|t| tokio::time::Instant::now() < t + self.duration)
            .unwrap_or(false)
    }

    pub fn renew(&mut self, term: u64) {
        self.acquired_at = Some(tokio::time::Instant::now());
        self.acquired_at_term = term;
    }
}
