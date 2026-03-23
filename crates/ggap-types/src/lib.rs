use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub type NodeId = u64;
pub type ShardId = u64;

/// Injectable clock for wall-clock timestamps (nanoseconds since Unix epoch).
/// Use the default `system_now_fn()` in production. In deterministic simulation
/// tests (Phase 6), inject a mock that returns controlled time.
pub type NowFn = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Returns a `NowFn` backed by `SystemTime::now()`.
pub fn system_now_fn() -> NowFn {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64
    })
}

/// Key range owned by a shard: [start, end). Empty end means unbounded.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct KeyRange {
    /// Inclusive lower bound. Empty string means the minimum possible key.
    pub start: String,
    /// Exclusive upper bound. Empty string means unbounded (no upper limit).
    pub end: String,
}

impl KeyRange {
    /// Returns `true` if `key` falls within this range.
    pub fn contains(&self, key: &str) -> bool {
        let after_start = self.start.is_empty() || key >= self.start.as_str();
        let before_end = self.end.is_empty() || key < self.end.as_str();
        after_start && before_end
    }
}

/// State of a shard in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShardState {
    Active,
    Splitting,
}

/// Metadata for a single shard.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub range: KeyRange,
    pub state: ShardState,
}

/// Stored in last_applied metadata
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogId {
    pub term: u64,
    pub leader_id: u64,
    pub index: u64,
}

/// Stored per key (current value and history entries)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KvEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub version: u64,
    pub created_at_ns: i64,
    pub modified_at_ns: i64,
    pub expires_at_ns: Option<i64>,
}

/// Commands proposed through Raft (implements openraft::AppData in ggap-consensus)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum KvCommand {
    Put {
        key: String,
        value: Vec<u8>,
        ttl_ns: Option<i64>,
        expect_version: u64,
    },
    Delete {
        key: String,
    },
    Cas {
        key: String,
        expected: Vec<u8>,
        new_value: Vec<u8>,
        ttl_ns: Option<i64>,
    },
}

/// Responses returned from state machine apply
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum KvResponse {
    Written {
        version: u64,
    },
    Deleted {
        found: bool,
    },
    CasResult {
        success: bool,
        current: Option<KvEntry>,
    },
    /// Conditional Put whose expect_version didn't match. Returned to the
    /// client as Status::aborted; never a fatal storage error.
    Conflict {
        expected: u64,
        actual: u64,
    },
    /// Returned for Raft-internal entries (Blank, Membership).
    /// Never sent to clients; guarded by unreachable!() in ggap-server.
    NoOp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    Linearizable,
    Sequential,
    Eventual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Majority,
    All,
}

#[derive(thiserror::Error, Debug)]
pub enum GgapError {
    #[error("key not found")]
    NotFound,
    #[error("not the leader; hint: {leader:?}")]
    NotLeader { leader: Option<String> },
    #[error("version conflict: expected {expected}, got {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("operation timed out")]
    Timeout,
    #[error("storage error: {0}")]
    Storage(String),
    #[error("consensus error: {0}")]
    Consensus(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("wrong shard for key; try shard {shard_id}")]
    WrongShard { shard_id: ShardId, range: KeyRange },
    #[error("shard is splitting, retry later")]
    ShardSplitting,
    #[error("shard not found: {0}")]
    ShardNotFound(ShardId),
}
