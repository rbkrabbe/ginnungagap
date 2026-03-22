use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use ggap_storage::fjall::FjallStore;
use ggap_storage::keys::{meta_key, raft_log_key};
use ggap_types::ShardId;
use openraft::storage::RaftLogStorage;
use openraft::AnyError;
use openraft::{
    ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader, RaftTypeConfig,
    StorageError, StorageIOError, Vote,
};

use crate::config::GgapTypeConfig;
use crate::convert::{decode, encode};

fn sto_err(msg: impl Into<String>) -> StorageError<u64> {
    let io = StorageIOError::new(
        ErrorSubject::Store,
        ErrorVerb::Write,
        AnyError::error(msg.into()),
    );
    StorageError::IO { source: io }
}

// ---------------------------------------------------------------------------
// GgapLogStorage
// ---------------------------------------------------------------------------

/// openraft `RaftLogStorage` adapter backed by `FjallStore`.
///
/// Stores serialized `Entry<GgapTypeConfig>` in the `raft_log` partition,
/// keyed by `raft_log_key(shard_id, index)`. Vote and metadata are stored
/// in the `meta` partition.
pub struct GgapLogStorage {
    pub(crate) store: Arc<FjallStore>,
    pub(crate) shard_id: ShardId,
}

impl GgapLogStorage {
    pub fn new(store: Arc<FjallStore>, shard_id: ShardId) -> Self {
        GgapLogStorage { store, shard_id }
    }
}

impl Clone for GgapLogStorage {
    fn clone(&self) -> Self {
        GgapLogStorage {
            store: self.store.clone(),
            shard_id: self.shard_id,
        }
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader
// ---------------------------------------------------------------------------

impl RaftLogReader<GgapTypeConfig> for GgapLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<GgapTypeConfig as RaftTypeConfig>::Entry>, StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;

        // Resolve bounds to concrete u64 values.
        let start = match range.start_bound() {
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&e) => e,
            std::ops::Bound::Excluded(&e) => e.saturating_sub(1),
            std::ops::Bound::Unbounded => u64::MAX,
        };

        if start > end {
            return Ok(vec![]);
        }

        tokio::task::spawn_blocking(move || {
            let key_start = raft_log_key(shard_id, start).to_vec();
            let key_end = raft_log_key(shard_id, end).to_vec();
            store
                .raft_log
                .range(key_start..=key_end)
                .map(|g| {
                    g.into_inner()
                        .map_err(|e| sto_err(e.to_string()))
                        .and_then(|(_, v)| {
                            decode::<<GgapTypeConfig as RaftTypeConfig>::Entry>(&v)
                                .map_err(|e| sto_err(e.to_string()))
                        })
                })
                .collect()
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }
}

// ---------------------------------------------------------------------------
// RaftLogStorage
// ---------------------------------------------------------------------------

impl RaftLogStorage<GgapTypeConfig> for GgapLogStorage {
    type LogReader = GgapLogStorage;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;
        let bytes = encode(vote).map_err(|e| sto_err(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            store
                .meta
                .insert(meta_key(shard_id, "or_vote"), bytes)
                .map_err(|e| sto_err(e.to_string()))
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;
        tokio::task::spawn_blocking(move || {
            match store
                .meta
                .get(meta_key(shard_id, "or_vote"))
                .map_err(|e| sto_err(e.to_string()))?
            {
                None => Ok(None),
                Some(b) => decode::<Vote<u64>>(&b)
                    .map(Some)
                    .map_err(|e| sto_err(e.to_string())),
            }
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }

    async fn get_log_state(&mut self) -> Result<LogState<GgapTypeConfig>, StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;
        tokio::task::spawn_blocking(move || {
            // Read last_purged_log_id from meta.
            let last_purged_log_id: Option<LogId<u64>> = match store
                .meta
                .get(meta_key(shard_id, "or_last_purged"))
                .map_err(|e| sto_err(e.to_string()))?
            {
                None => None,
                Some(b) => Some(decode::<LogId<u64>>(&b).map_err(|e| sto_err(e.to_string()))?),
            };

            // Read only the last log entry (iterator is DoubleEndedIterator).
            let start_key = raft_log_key(shard_id, 0).to_vec();
            let end_key = raft_log_key(shard_id, u64::MAX).to_vec();

            let last_log_id = store
                .raft_log
                .range(start_key..=end_key)
                .next_back()
                .map(|g| {
                    g.into_inner()
                        .map_err(|e| sto_err(e.to_string()))
                        .and_then(|(_, v)| {
                            decode::<<GgapTypeConfig as RaftTypeConfig>::Entry>(&v)
                                .map_err(|e| sto_err(e.to_string()))
                                .map(|entry| entry.log_id)
                        })
                })
                .transpose()?;

            // If the log is empty, last_log_id falls back to last_purged_log_id.
            let last_log_id = last_log_id.or(last_purged_log_id);

            Ok(LogState {
                last_purged_log_id,
                last_log_id,
            })
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;
        let bytes = encode(&committed).map_err(|e| sto_err(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            store
                .meta
                .insert(meta_key(shard_id, "or_committed"), bytes)
                .map_err(|e| sto_err(e.to_string()))
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;
        tokio::task::spawn_blocking(move || {
            match store
                .meta
                .get(meta_key(shard_id, "or_committed"))
                .map_err(|e| sto_err(e.to_string()))?
            {
                None => Ok(None),
                Some(b) => decode::<Option<LogId<u64>>>(&b).map_err(|e| sto_err(e.to_string())),
            }
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<GgapTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = <GgapTypeConfig as RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let store = self.store.clone();
        let shard_id = self.shard_id;
        let entries: Vec<_> = entries.into_iter().collect();

        let result: Result<(), StorageError<u64>> = tokio::task::spawn_blocking(move || {
            let mut batch = store.db.batch();
            for entry in &entries {
                let index = entry.log_id.index;
                let key = raft_log_key(shard_id, index).to_vec();
                let val = encode(entry).map_err(|e| sto_err(e.to_string()))?;
                batch.insert(&store.raft_log, key, val);
            }
            batch.commit().map_err(|e| sto_err(e.to_string()))
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?;

        // Report flush completion after disk write.
        callback.log_io_completed(result.map_err(|e| io::Error::other(e.to_string())));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;
        tokio::task::spawn_blocking(move || {
            let start = raft_log_key(shard_id, log_id.index).to_vec();
            let end = raft_log_key(shard_id, u64::MAX).to_vec();

            let keys: Vec<Vec<u8>> = store
                .raft_log
                .range(start..=end)
                .map(|g| {
                    g.into_inner()
                        .map(|(k, _)| k.to_vec())
                        .map_err(|e| sto_err(e.to_string()))
                })
                .collect::<Result<_, _>>()?;

            if !keys.is_empty() {
                let mut batch = store.db.batch();
                for k in keys {
                    batch.remove(&store.raft_log, k);
                }
                batch.commit().map_err(|e| sto_err(e.to_string()))?;
            }
            Ok(())
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;
        tokio::task::spawn_blocking(move || {
            let start = raft_log_key(shard_id, 0).to_vec();
            let end = raft_log_key(shard_id, log_id.index).to_vec();

            let keys: Vec<Vec<u8>> = store
                .raft_log
                .range(start..=end)
                .map(|g| {
                    g.into_inner()
                        .map(|(k, _)| k.to_vec())
                        .map_err(|e| sto_err(e.to_string()))
                })
                .collect::<Result<_, _>>()?;

            let purged_bytes = encode(&log_id).map_err(|e| sto_err(e.to_string()))?;
            let mut batch = store.db.batch();
            for k in keys {
                batch.remove(&store.raft_log, k);
            }
            batch.insert(
                &store.meta,
                meta_key(shard_id, "or_last_purged"),
                purged_bytes,
            );
            batch.commit().map_err(|e| sto_err(e.to_string()))
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }
}

// The Sealed trait is implemented for all types when storage-v2 is enabled.
// We just need to make sure the trait bounds are satisfied.
#[allow(dead_code)]
fn _assert_log_storage_impls() {
    fn assert_log_storage<T: RaftLogStorage<GgapTypeConfig>>() {}
    // This function is never called; it's just a compile-time check.
}
