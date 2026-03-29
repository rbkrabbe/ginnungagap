use std::sync::Arc;

use ggap_storage::fjall::FjallStateMachine;
use ggap_storage::traits::StateMachineStore;
use ggap_types::{GgapError, KvCommand, KvEntry, KvResponse, ReadMode, ShardId, WriteMode};
use openraft::{
    raft::{AppendEntriesRequest, VoteRequest},
    BasicNode, Raft,
};

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
        let is_leader = self.raft.metrics().borrow().state == openraft::ServerState::Leader;
        if is_leader && self.lease.lock().await.is_valid() {
            return Ok(());
        }
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|e| GgapError::Consensus(e.to_string()))?;
        self.lease.lock().await.renew();
        Ok(())
    }
}

impl RaftNode for OpenRaftNode {
    fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    async fn propose(&self, cmd: KvCommand, _mode: WriteMode) -> Result<KvResponse, GgapError> {
        self.raft
            .client_write(cmd)
            .await
            .map(|r| r.data)
            .map_err(|e| {
                // Check if it's a ForwardToLeader error.
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
// LeaseManager (Phase 5 stub)
// ---------------------------------------------------------------------------

pub struct LeaseManager {
    acquired_at: Option<tokio::time::Instant>,
    duration: tokio::time::Duration,
}

impl LeaseManager {
    pub fn new(duration: tokio::time::Duration) -> Self {
        LeaseManager {
            acquired_at: None,
            duration,
        }
    }

    /// Returns `true` if the lease was acquired and has not yet expired.
    pub fn is_valid(&self) -> bool {
        self.acquired_at
            .map(|t| tokio::time::Instant::now() < t + self.duration)
            .unwrap_or(false)
    }

    pub fn renew(&mut self) {
        self.acquired_at = Some(tokio::time::Instant::now());
    }
}
