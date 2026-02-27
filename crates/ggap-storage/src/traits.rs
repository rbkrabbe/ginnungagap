use std::future::Future;

use ggap_types::{GgapError, KvCommand, KvEntry, KvResponse, ShardId};

use crate::types::{LogEntry, LogState, Snapshot, Vote};

/// Persistent storage for the Raft log of a single shard.
///
/// Methods use RPITIT (`-> impl Future + Send`) matching the `RaftNode` trait
/// style in `ggap-consensus`. This avoids any `async-trait` dependency.
///
/// `ggap-storage` deliberately does **not** depend on `openraft`. The
/// openraft adapter shim lives in `ggap-consensus` (Phase 4).
pub trait LogStorage: Send + Sync + 'static {
    /// Return first index, last index, and last purged index for the shard.
    fn log_state(&self, shard_id: ShardId)
        -> impl Future<Output = Result<LogState, GgapError>> + Send;

    /// Return the entry at `index`, or `None` if it has been purged or does
    /// not exist.
    fn get_entry(&self, shard_id: ShardId, index: u64)
        -> impl Future<Output = Result<Option<LogEntry>, GgapError>> + Send;

    /// Return all entries in the inclusive range `[from, to_inclusive]`.
    fn get_entries(&self, shard_id: ShardId, from: u64, to_inclusive: u64)
        -> impl Future<Output = Result<Vec<LogEntry>, GgapError>> + Send;

    /// Append entries to the log, overwriting any existing entry at the same
    /// index (used when the leader sends a corrective AppendEntries).
    fn append(&self, shard_id: ShardId, entries: Vec<LogEntry>)
        -> impl Future<Output = Result<(), GgapError>> + Send;

    /// Delete all entries with `index >= from_index` (conflict resolution).
    fn truncate(&self, shard_id: ShardId, from_index: u64)
        -> impl Future<Output = Result<(), GgapError>> + Send;

    /// Delete all entries with `index <= up_to_index` (post-snapshot GC).
    /// Updates `last_purged_index`.
    fn purge(&self, shard_id: ShardId, up_to_index: u64)
        -> impl Future<Output = Result<(), GgapError>> + Send;

    /// Durably persist the vote for the shard (called before granting a vote).
    fn save_vote(&self, shard_id: ShardId, vote: Vote)
        -> impl Future<Output = Result<(), GgapError>> + Send;

    /// Retrieve the last persisted vote for the shard.
    fn read_vote(&self, shard_id: ShardId)
        -> impl Future<Output = Result<Option<Vote>, GgapError>> + Send;
}

/// Persistent key-value state machine for a single shard.
///
/// Maintains the current data map, full MVCC history, and TTL metadata.
pub trait StateMachineStore: Send + Sync + 'static {
    /// Return the Raft log index of the last applied entry, or `None` if the
    /// state machine is empty.
    fn last_applied(&self, shard_id: ShardId)
        -> impl Future<Output = Result<Option<u64>, GgapError>> + Send;

    /// Apply a committed `KvCommand` at the given Raft log `index`.
    ///
    /// `index` is used as both the MVCC version and the `last_applied` cursor.
    fn apply(&self, shard_id: ShardId, index: u64, cmd: KvCommand)
        -> impl Future<Output = Result<KvResponse, GgapError>> + Send;

    /// Read a key.
    /// * `at_version == 0` → current value (from the `data` partition).
    /// * `at_version > 0`  → historical value at exactly that version.
    fn get(&self, shard_id: ShardId, key: &str, at_version: u64)
        -> impl Future<Output = Result<Option<KvEntry>, GgapError>> + Send;

    /// Paginated prefix/range scan over the `data` partition.
    ///
    /// * `end_key` empty means unbounded.
    /// * `limit == 0` means server default (100).
    ///
    /// Returns `(entries, continuation_key)` where `Some(key)` indicates
    /// more results exist past the returned page.
    fn scan(&self, shard_id: ShardId, start_key: &str, end_key: &str, limit: u32)
        -> impl Future<Output = Result<(Vec<KvEntry>, Option<String>), GgapError>> + Send;

    /// Serialize the entire `data` partition for the shard into a `Snapshot`.
    fn build_snapshot(&self, shard_id: ShardId)
        -> impl Future<Output = Result<Snapshot, GgapError>> + Send;

    /// Replace the state machine contents with a snapshot received from the
    /// leader. Clears `history` and `ttl_index` for the shard.
    fn install_snapshot(&self, shard_id: ShardId, snapshot: Snapshot)
        -> impl Future<Output = Result<(), GgapError>> + Send;
}
