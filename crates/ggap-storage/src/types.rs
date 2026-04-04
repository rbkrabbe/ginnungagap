use ggap_types::{KvCommand, KvEntry, LogId};

/// A single entry in the Raft log.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub leader_id: u64,
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
    /// Full `LogId` of the last entry in the log, or `None` if the log is empty.
    pub last_log_id: Option<LogId>,
    /// Full `LogId` of the last purged entry (compacted into a snapshot).
    pub last_purged_log_id: Option<LogId>,
}

/// Metadata identifying a snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMeta {
    pub last_log_id: Option<LogId>,
    pub membership_bytes: Vec<u8>,
    /// UUID v4 string identifying this snapshot.
    pub snapshot_id: String,
}

/// Internal serialized format stored in [`Snapshot::data`].
///
/// Both the current-value (`data`) partition and the MVCC history (`history`)
/// partition are captured so that `at_version` reads survive a snapshot
/// round-trip on the node that built it, and remain available on a follower
/// that receives and installs the snapshot.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotContents {
    /// Current value per key (mirrors the `data` partition).
    pub data: Vec<(String, KvEntry)>,
    /// All retained history entries (mirrors the `history` partition),
    /// keyed by `(user_key, version)`.
    pub history: Vec<((String, u64), KvEntry)>,
}

/// A full state-machine snapshot for a shard.
///
/// `data` is a `bincode`-serialized [`SnapshotContents`] — a complete dump of
/// both the `data` and `history` partitions for the shard at the time of the
/// snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}
