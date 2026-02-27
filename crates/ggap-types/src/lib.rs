pub type NodeId = u64;
pub type ShardId = u64; // always 0 in phases 1-6

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
#[derive(Debug, Clone)]
pub enum KvResponse {
    Written { version: u64 },
    Deleted { found: bool },
    CasResult { success: bool, current: Option<KvEntry> },
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
}
