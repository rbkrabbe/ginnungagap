use ggap_types::KvCommand;

/// A single entry in the Raft log.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub payload: LogPayload,
}

/// The payload carried by a `LogEntry`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum LogPayload {
    /// No-op / heartbeat entry.
    Blank,
    /// A key-value command to apply to the state machine.
    Normal(KvCommand),
    /// Raw bytes for membership changes and other openraft internals.
    /// Phase 4 owns the deserialization of this variant.
    Raw(Vec<u8>),
}

/// Persisted vote for a single shard, written before granting a vote in an
/// election.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Vote {
    pub term: u64,
    /// `None` means the node has not voted yet in this term.
    pub voted_for: Option<u64>, // NodeId
    /// `true` once the leader for this term has been committed (quorum formed).
    pub committed: bool,
}

/// Summary of the current log extent for a shard.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogState {
    /// Smallest index currently in the log (`None` if log is empty).
    pub first_index: Option<u64>,
    /// Largest index currently in the log (`None` if log is empty).
    pub last_index: Option<u64>,
    /// Largest index that has been purged (compacted into a snapshot).
    pub last_purged_index: Option<u64>,
}

/// Metadata identifying a snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMeta {
    pub last_log_index: u64,
    pub last_log_term: u64,
    /// UUID v4 string identifying this snapshot.
    pub snapshot_id: String,
}

/// A full state-machine snapshot for a shard.
///
/// `data` is a `bincode`-serialized `Vec<(String, KvEntry)>` — a complete
/// dump of the `data` partition for the shard at the time of the snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}
