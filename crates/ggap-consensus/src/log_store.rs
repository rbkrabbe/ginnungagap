use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;

use ggap_storage::traits::LogStorage;
use ggap_types::ShardId;
use openraft::storage::RaftLogStorage;
use openraft::AnyError;
use openraft::{
    ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader, RaftTypeConfig,
    StorageError, StorageIOError, Vote,
};

use crate::config::GgapTypeConfig;
use crate::convert;

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

/// openraft `RaftLogStorage` adapter backed by any `LogStorage` implementation.
///
/// Converts between openraft types and domain types. All storage I/O is
/// delegated to the underlying `S: LogStorage`; this adapter contains no
/// fjall-specific code.
pub struct GgapLogStorage<S: LogStorage> {
    pub(crate) store: S,
    pub(crate) shard_id: ShardId,
}

impl<S: LogStorage> GgapLogStorage<S> {
    pub fn new(store: S, shard_id: ShardId) -> Self {
        GgapLogStorage { store, shard_id }
    }
}

impl<S: LogStorage + Clone> Clone for GgapLogStorage<S> {
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

#[allow(clippy::result_large_err)]
impl<S: LogStorage + Clone> RaftLogReader<GgapTypeConfig> for GgapLogStorage<S> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<GgapTypeConfig as RaftTypeConfig>::Entry>, StorageError<u64>> {
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

        let entries = self
            .store
            .get_entries(shard_id, start, end)
            .await
            .map_err(|e| sto_err(e.to_string()))?;

        entries
            .iter()
            .map(|e| convert::log_entry_to_or_entry(e).map_err(|e| sto_err(e.to_string())))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// RaftLogStorage
// ---------------------------------------------------------------------------

#[allow(clippy::result_large_err)]
impl<S: LogStorage + Clone> RaftLogStorage<GgapTypeConfig> for GgapLogStorage<S> {
    type LogReader = GgapLogStorage<S>;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let domain_vote = convert::or_vote_to_vote(vote);
        self.store
            .save_vote(self.shard_id, domain_vote)
            .await
            .map_err(|e| sto_err(e.to_string()))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let vote = self
            .store
            .read_vote(self.shard_id)
            .await
            .map_err(|e| sto_err(e.to_string()))?;
        Ok(vote.map(|v| convert::vote_to_or_vote(&v)))
    }

    async fn get_log_state(&mut self) -> Result<LogState<GgapTypeConfig>, StorageError<u64>> {
        let state = self
            .store
            .log_state(self.shard_id)
            .await
            .map_err(|e| sto_err(e.to_string()))?;
        Ok(convert::log_state_to_or_log_state(&state))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let domain_committed = committed.map(convert::or_log_id_to_log_id);
        self.store
            .save_committed(self.shard_id, domain_committed)
            .await
            .map_err(|e| sto_err(e.to_string()))
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let committed = self
            .store
            .read_committed(self.shard_id)
            .await
            .map_err(|e| sto_err(e.to_string()))?;
        Ok(committed.map(convert::log_id_to_or_log_id))
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
        let domain_entries: Vec<_> = entries
            .into_iter()
            .map(|e| convert::or_entry_to_log_entry(&e))
            .collect::<Result<_, _>>()
            .map_err(|e| sto_err(e.to_string()))?;

        let result = self
            .store
            .append(self.shard_id, domain_entries)
            .await
            .map_err(|e| sto_err(e.to_string()));

        // Report flush completion after storage write.
        callback.log_io_completed(result.map_err(|e| io::Error::other(e.to_string())));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.store
            .truncate(self.shard_id, log_id.index)
            .await
            .map_err(|e| sto_err(e.to_string()))
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let domain_log_id = convert::or_log_id_to_log_id(log_id);
        self.store
            .purge(self.shard_id, domain_log_id)
            .await
            .map_err(|e| sto_err(e.to_string()))
    }
}

// The Sealed trait is implemented for all types when storage-v2 is enabled.
// We just need to make sure the trait bounds are satisfied.
#[allow(dead_code)]
fn _assert_log_storage_impls() {
    fn assert_log_storage<T: RaftLogStorage<GgapTypeConfig>>() {}
    // This function is never called; it's just a compile-time check.
}
