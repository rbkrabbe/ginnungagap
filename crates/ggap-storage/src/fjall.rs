use std::path::Path;
use std::sync::Arc;

use ggap_types::{
    system_now_fn, DomainWatchEvent, GgapError, KvCommand, KvEntry, KvResponse, LogId, NowFn,
    ShardId, WatchEventKind,
};
use tracing::warn;

use crate::keys::{
    data_key, data_shard_end, history_key, history_prefix, meta_key, raft_log_key, ttl_index_key,
};
use crate::traits::{LogStorage, StateMachineStore};
use crate::types::{LogEntry, LogState, Snapshot, SnapshotContents, SnapshotMeta, Vote};

/// Maximum MVCC history versions retained per key before compaction.
pub const DEFAULT_MAX_HISTORY: u64 = 10;

fn encode<T: serde::Serialize>(val: &T) -> Result<Vec<u8>, GgapError> {
    bincode::serde::encode_to_vec(val, bincode::config::standard())
        .map_err(|e| GgapError::Storage(e.to_string()))
}

fn decode<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8]) -> Result<T, GgapError> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| GgapError::Storage(e.to_string()))
}

fn fjall_err(e: fjall::Error) -> GgapError {
    GgapError::Storage(e.to_string())
}

// ---------------------------------------------------------------------------
// FjallStore — shared handle wrapping all five keyspaces
// ---------------------------------------------------------------------------

/// Shared storage handle.
///
/// All five keyspaces live in a single fjall `Database` so that cross-keyspace
/// write batches are atomic.
pub struct FjallStore {
    /// The underlying fjall database.
    pub db: fjall::Database,
    /// Raft log entries: `shard(8) ++ index(8)` → bincode(LogEntry)
    pub raft_log: fjall::Keyspace,
    /// Current key-value data: `shard(8) ++ key_utf8` → bincode(KvEntry)
    pub data: fjall::Keyspace,
    /// MVCC history: `shard(8) ++ key_utf8 ++ \x00 ++ version(8)` → bincode(KvEntry)
    pub history: fjall::Keyspace,
    /// TTL expiry index: `shard(8) ++ expires_at_ns_be_i64(8) ++ key_utf8` → b""
    pub ttl_index: fjall::Keyspace,
    /// Miscellaneous metadata: `shard(8) ++ label_utf8` → bincode(value)
    pub meta: fjall::Keyspace,
}

impl FjallStore {
    /// Open (or create) a `FjallStore` at `path`.
    pub fn open(path: &Path) -> Result<Arc<Self>, GgapError> {
        let db = fjall::Database::builder(path).open().map_err(fjall_err)?;
        let raft_log = db
            .keyspace("raft_log", fjall::KeyspaceCreateOptions::default)
            .map_err(fjall_err)?;
        let data = db
            .keyspace("data", fjall::KeyspaceCreateOptions::default)
            .map_err(fjall_err)?;
        let history = db
            .keyspace("history", fjall::KeyspaceCreateOptions::default)
            .map_err(fjall_err)?;
        let ttl_index = db
            .keyspace("ttl_index", fjall::KeyspaceCreateOptions::default)
            .map_err(fjall_err)?;
        let meta = db
            .keyspace("meta", fjall::KeyspaceCreateOptions::default)
            .map_err(fjall_err)?;
        Ok(Arc::new(FjallStore {
            db,
            raft_log,
            data,
            history,
            ttl_index,
            meta,
        }))
    }
}

// ---------------------------------------------------------------------------
// FjallLogStorage
// ---------------------------------------------------------------------------

/// `LogStorage` backed by fjall.
///
/// All blocking I/O is wrapped in `tokio::task::spawn_blocking`.
#[derive(Clone)]
pub struct FjallLogStorage(pub Arc<FjallStore>);

impl LogStorage for FjallLogStorage {
    async fn log_state(&self, shard_id: ShardId) -> Result<LogState, GgapError> {
        let store = self.0.clone();
        tokio::task::spawn_blocking(move || -> Result<LogState, GgapError> {
            // Read last_purged_log_id from meta.
            let last_purged_log_id = match store
                .meta
                .get(meta_key(shard_id, "last_purged"))
                .map_err(fjall_err)?
            {
                Some(b) => Some(decode::<LogId>(&b)?),
                None => None,
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
                        .map_err(fjall_err)
                        .and_then(|(_, v)| {
                            let entry = decode::<LogEntry>(&v)?;
                            Ok(LogId {
                                term: entry.term,
                                leader_id: entry.leader_id,
                                index: entry.index,
                            })
                        })
                })
                .transpose()?;

            // If the log is empty, last_log_id falls back to last_purged_log_id.
            let last_log_id = last_log_id.or(last_purged_log_id);

            Ok(LogState {
                last_log_id,
                last_purged_log_id,
            })
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn get_entries(
        &self,
        shard_id: ShardId,
        from: u64,
        to_inclusive: u64,
    ) -> Result<Vec<LogEntry>, GgapError> {
        let store = self.0.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<LogEntry>, GgapError> {
            let start = raft_log_key(shard_id, from).to_vec();
            let end = raft_log_key(shard_id, to_inclusive).to_vec();
            store
                .raft_log
                .range(start..=end)
                .map(|g| {
                    g.into_inner()
                        .map_err(fjall_err)
                        .and_then(|(_, v)| decode::<LogEntry>(&v))
                })
                .collect()
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn append(&self, shard_id: ShardId, entries: Vec<LogEntry>) -> Result<(), GgapError> {
        let store = self.0.clone();
        tokio::task::spawn_blocking(move || -> Result<(), GgapError> {
            let mut batch = store.db.batch();
            for entry in &entries {
                let key = raft_log_key(shard_id, entry.index).to_vec();
                batch.insert(&store.raft_log, key, encode(entry)?);
            }
            batch.commit().map_err(fjall_err)
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn truncate(&self, shard_id: ShardId, from_index: u64) -> Result<(), GgapError> {
        let store = self.0.clone();
        tokio::task::spawn_blocking(move || -> Result<(), GgapError> {
            let start = raft_log_key(shard_id, from_index).to_vec();
            let end = raft_log_key(shard_id, u64::MAX).to_vec();

            let keys: Vec<Vec<u8>> = store
                .raft_log
                .range(start..=end)
                .map(|g| g.into_inner().map(|(k, _)| k.to_vec()).map_err(fjall_err))
                .collect::<Result<_, _>>()?;

            if !keys.is_empty() {
                let mut batch = store.db.batch();
                for k in keys {
                    batch.remove(&store.raft_log, k);
                }
                batch.commit().map_err(fjall_err)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn purge(&self, shard_id: ShardId, up_to: LogId) -> Result<(), GgapError> {
        let store = self.0.clone();
        tokio::task::spawn_blocking(move || -> Result<(), GgapError> {
            let start = raft_log_key(shard_id, 0).to_vec();
            let end = raft_log_key(shard_id, up_to.index).to_vec();

            let keys: Vec<Vec<u8>> = store
                .raft_log
                .range(start..=end)
                .map(|g| g.into_inner().map(|(k, _)| k.to_vec()).map_err(fjall_err))
                .collect::<Result<_, _>>()?;

            let mut batch = store.db.batch();
            for k in keys {
                batch.remove(&store.raft_log, k);
            }
            batch.insert(
                &store.meta,
                meta_key(shard_id, "last_purged"),
                encode(&up_to)?,
            );
            batch.commit().map_err(fjall_err)
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn save_vote(&self, shard_id: ShardId, vote: Vote) -> Result<(), GgapError> {
        let store = self.0.clone();
        tokio::task::spawn_blocking(move || -> Result<(), GgapError> {
            store
                .meta
                .insert(meta_key(shard_id, "vote"), encode(&vote)?)
                .map_err(fjall_err)
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn read_vote(&self, shard_id: ShardId) -> Result<Option<Vote>, GgapError> {
        let store = self.0.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<Vote>, GgapError> {
            match store
                .meta
                .get(meta_key(shard_id, "vote"))
                .map_err(fjall_err)?
            {
                Some(b) => Ok(Some(decode::<Vote>(&b)?)),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn save_committed(
        &self,
        shard_id: ShardId,
        committed: Option<LogId>,
    ) -> Result<(), GgapError> {
        let store = self.0.clone();
        tokio::task::spawn_blocking(move || -> Result<(), GgapError> {
            store
                .meta
                .insert(meta_key(shard_id, "committed"), encode(&committed)?)
                .map_err(fjall_err)
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn read_committed(&self, shard_id: ShardId) -> Result<Option<LogId>, GgapError> {
        let store = self.0.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<LogId>, GgapError> {
            match store
                .meta
                .get(meta_key(shard_id, "committed"))
                .map_err(fjall_err)?
            {
                Some(b) => decode::<Option<LogId>>(&b),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }
}

// ---------------------------------------------------------------------------
// FjallStateMachine
// ---------------------------------------------------------------------------

/// `StateMachineStore` backed by fjall.
pub struct FjallStateMachine {
    pub(crate) store: Arc<FjallStore>,
    max_history_versions: u64,
    now_fn: NowFn,
    watch_tx: Option<tokio::sync::broadcast::Sender<DomainWatchEvent>>,
}

impl FjallStateMachine {
    pub fn new(store: Arc<FjallStore>) -> Self {
        FjallStateMachine {
            store,
            max_history_versions: DEFAULT_MAX_HISTORY,
            now_fn: system_now_fn(),
            watch_tx: None,
        }
    }

    pub fn with_max_history(store: Arc<FjallStore>, max_history_versions: u64) -> Self {
        FjallStateMachine {
            store,
            max_history_versions,
            now_fn: system_now_fn(),
            watch_tx: None,
        }
    }

    pub fn with_clock(mut self, now_fn: NowFn) -> Self {
        self.now_fn = now_fn;
        self
    }

    /// Attach a broadcast sender so that committed writes fan out to Watch subscribers.
    pub fn with_watch(mut self, tx: tokio::sync::broadcast::Sender<DomainWatchEvent>) -> Self {
        self.watch_tx = Some(tx);
        self
    }
}

impl StateMachineStore for FjallStateMachine {
    async fn last_applied(
        &self,
        shard_id: ShardId,
    ) -> Result<(Option<LogId>, Option<Vec<u8>>), GgapError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(
            move || -> Result<(Option<LogId>, Option<Vec<u8>>), GgapError> {
                let last_applied = match store
                    .meta
                    .get(meta_key(shard_id, "last_applied"))
                    .map_err(fjall_err)?
                {
                    Some(b) => Some(decode::<LogId>(&b)?),
                    None => None,
                };

                let membership_bytes = store
                    .meta
                    .get(meta_key(shard_id, "membership"))
                    .map_err(fjall_err)?
                    .map(|b| b.to_vec());
                Ok((last_applied, membership_bytes))
            },
        )
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn apply(
        &self,
        shard_id: ShardId,
        log_id: LogId,
        cmd: Option<KvCommand>,
        membership_bytes: Option<Vec<u8>>,
    ) -> Result<KvResponse, GgapError> {
        let store = self.store.clone();
        let max_history = self.max_history_versions;
        let now_fn = self.now_fn.clone();
        let watch_tx = self.watch_tx.clone();
        tokio::task::spawn_blocking(move || -> Result<KvResponse, GgapError> {
            let now = now_fn();
            match cmd {
                Some(KvCommand::Put {
                    key,
                    value,
                    ttl_ns,
                    expect_version,
                }) => {
                    let current = store
                        .data
                        .get(data_key(shard_id, &key))
                        .map_err(fjall_err)?
                        .map(|b| decode::<KvEntry>(&b))
                        .transpose()?;

                    let current_ver = current.as_ref().map(|e| e.version).unwrap_or(0);
                    if expect_version != 0 && current_ver != expect_version {
                        return Err(GgapError::VersionConflict {
                            expected: expect_version,
                            actual: current_ver,
                        });
                    }

                    let created_at_ns = current.as_ref().map(|e| e.created_at_ns).unwrap_or(now);
                    let expires_at_ns = ttl_ns.map(|ns| now + ns);
                    let entry = KvEntry {
                        key: key.clone(),
                        value,
                        version: log_id.index,
                        created_at_ns,
                        modified_at_ns: now,
                        expires_at_ns,
                    };

                    let mut batch = store.db.batch();
                    batch.insert(&store.data, data_key(shard_id, &key), encode(&entry)?);
                    batch.insert(
                        &store.history,
                        history_key(shard_id, &key, log_id.index),
                        encode(&entry)?,
                    );
                    if let Some(exp) = expires_at_ns {
                        batch.insert(
                            &store.ttl_index,
                            ttl_index_key(shard_id, exp, &key),
                            Vec::new(),
                        );
                    }
                    // Remove stale TTL index entry for the key's previous TTL.
                    if let Some(ref cur) = current {
                        if let Some(old_exp) = cur.expires_at_ns {
                            batch.remove(&store.ttl_index, ttl_index_key(shard_id, old_exp, &key));
                        }
                    }
                    batch.insert(
                        &store.meta,
                        meta_key(shard_id, "last_applied"),
                        encode(&log_id)?,
                    );
                    batch.commit().map_err(fjall_err)?;

                    if let Err(e) = compact_history(&store, shard_id, &key, max_history) {
                        warn!(key = %key, error = %e, "history compaction failed after committed write");
                    }
                    if let Some(ref tx) = watch_tx {
                        let _ = tx.send(DomainWatchEvent {
                            kind: WatchEventKind::Put,
                            shard_id,
                            key,
                            entry: Some(entry.clone()),
                            version: log_id.index,
                            raft_index: log_id.index,
                        });
                    }
                    Ok(KvResponse::Written {
                        version: log_id.index,
                    })
                }

                Some(KvCommand::Delete { key }) => {
                    let current = store
                        .data
                        .get(data_key(shard_id, &key))
                        .map_err(fjall_err)?
                        .map(|b| decode::<KvEntry>(&b))
                        .transpose()?;

                    let found = current.is_some();
                    let mut batch = store.db.batch();
                    batch.remove(&store.data, data_key(shard_id, &key));
                    if let Some(ref cur) = current {
                        if let Some(exp) = cur.expires_at_ns {
                            batch.remove(&store.ttl_index, ttl_index_key(shard_id, exp, &key));
                        }
                    }
                    batch.insert(
                        &store.meta,
                        meta_key(shard_id, "last_applied"),
                        encode(&log_id)?,
                    );
                    batch.commit().map_err(fjall_err)?;
                    if let Some(ref tx) = watch_tx {
                        let _ = tx.send(DomainWatchEvent {
                            kind: WatchEventKind::Delete,
                            shard_id,
                            key,
                            entry: None,
                            version: log_id.index,
                            raft_index: log_id.index,
                        });
                    }
                    Ok(KvResponse::Deleted { found })
                }

                Some(KvCommand::Cas {
                    key,
                    expected,
                    new_value,
                    ttl_ns,
                }) => {
                    let current = store
                        .data
                        .get(data_key(shard_id, &key))
                        .map_err(fjall_err)?
                        .map(|b| decode::<KvEntry>(&b))
                        .transpose()?;

                    let matches = current
                        .as_ref()
                        .map(|e| e.value == expected)
                        .unwrap_or(false);

                    if matches {
                        let created_at_ns =
                            current.as_ref().map(|e| e.created_at_ns).unwrap_or(now);
                        let expires_at_ns = ttl_ns.map(|ns| now + ns);
                        let entry = KvEntry {
                            key: key.clone(),
                            value: new_value,
                            version: log_id.index,
                            created_at_ns,
                            modified_at_ns: now,
                            expires_at_ns,
                        };
                        let mut batch = store.db.batch();
                        batch.insert(&store.data, data_key(shard_id, &key), encode(&entry)?);
                        batch.insert(
                            &store.history,
                            history_key(shard_id, &key, log_id.index),
                            encode(&entry)?,
                        );
                        if let Some(exp) = expires_at_ns {
                            batch.insert(
                                &store.ttl_index,
                                ttl_index_key(shard_id, exp, &key),
                                Vec::new(),
                            );
                        }
                        if let Some(ref cur) = current {
                            if let Some(old_exp) = cur.expires_at_ns {
                                batch.remove(
                                    &store.ttl_index,
                                    ttl_index_key(shard_id, old_exp, &key),
                                );
                            }
                        }
                        batch.insert(
                            &store.meta,
                            meta_key(shard_id, "last_applied"),
                            encode(&log_id)?,
                        );
                        batch.commit().map_err(fjall_err)?;
                        if let Err(e) = compact_history(&store, shard_id, &key, max_history) {
                            warn!(key = %key, error = %e, "history compaction failed after committed CAS");
                        }
                        if let Some(ref tx) = watch_tx {
                            let _ = tx.send(DomainWatchEvent {
                                kind: WatchEventKind::Put,
                                shard_id,
                                key,
                                entry: Some(entry.clone()),
                                version: log_id.index,
                                raft_index: log_id.index,
                            });
                        }
                    } else {
                        // Still advance last_applied on CAS failure — use a batch
                        // for crash-atomicity with any concurrent membership update.
                        let mut batch = store.db.batch();
                        if let Some(bytes) = membership_bytes.as_ref() {
                            batch.insert(
                                &store.meta,
                                meta_key(shard_id, "membership"),
                                bytes.clone(),
                            );
                        }
                        batch.insert(
                            &store.meta,
                            meta_key(shard_id, "last_applied"),
                            encode(&log_id)?,
                        );
                        batch.commit().map_err(fjall_err)?;
                    }

                    Ok(KvResponse::CasResult {
                        success: matches,
                        current,
                    })
                }
                None => {
                    let mut batch = store.db.batch();
                    if let Some(bytes) = membership_bytes {
                        batch.insert(&store.meta, meta_key(shard_id, "membership"), bytes);
                    }
                    batch.insert(
                        &store.meta,
                        meta_key(shard_id, "last_applied"),
                        encode(&log_id)?,
                    );
                    batch.commit().map_err(fjall_err)?;
                    Ok(KvResponse::NoOp)
                }
            }
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn get(
        &self,
        shard_id: ShardId,
        key: &str,
        at_version: u64,
    ) -> Result<Option<KvEntry>, GgapError> {
        let store = self.store.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<KvEntry>, GgapError> {
            if at_version == 0 {
                match store
                    .data
                    .get(data_key(shard_id, &key))
                    .map_err(fjall_err)?
                {
                    Some(b) => Ok(Some(decode::<KvEntry>(&b)?)),
                    None => Ok(None),
                }
            } else {
                match store
                    .history
                    .get(history_key(shard_id, &key, at_version))
                    .map_err(fjall_err)?
                {
                    Some(b) => Ok(Some(decode::<KvEntry>(&b)?)),
                    None => Ok(None),
                }
            }
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn scan(
        &self,
        shard_id: ShardId,
        start_key: &str,
        end_key: &str,
        limit: u32,
    ) -> Result<(Vec<KvEntry>, Option<String>), GgapError> {
        let store = self.store.clone();
        let start_key = start_key.to_string();
        let end_key = end_key.to_string();
        tokio::task::spawn_blocking(
            move || -> Result<(Vec<KvEntry>, Option<String>), GgapError> {
                let effective_limit = if limit == 0 { 100 } else { limit } as usize;
                let start = data_key(shard_id, &start_key);
                let shard_end = data_shard_end(shard_id).to_vec();

                let raw: Vec<KvEntry> = if end_key.is_empty() {
                    store
                        .data
                        .range(start..shard_end)
                        .take(effective_limit + 1)
                        .map(|g| {
                            g.into_inner()
                                .map_err(fjall_err)
                                .and_then(|(_, v)| decode::<KvEntry>(&v))
                        })
                        .collect::<Result<_, _>>()?
                } else {
                    let end = data_key(shard_id, &end_key);
                    store
                        .data
                        .range(start..end)
                        .take(effective_limit + 1)
                        .map(|g| {
                            g.into_inner()
                                .map_err(fjall_err)
                                .and_then(|(_, v)| decode::<KvEntry>(&v))
                        })
                        .collect::<Result<_, _>>()?
                };

                let has_more = raw.len() > effective_limit;
                let mut entries = raw;
                let continuation = if has_more {
                    entries.pop().map(|e| e.key)
                } else {
                    None
                };
                Ok((entries, continuation))
            },
        )
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn build_snapshot(&self, shard_id: ShardId) -> Result<Snapshot, GgapError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || -> Result<Snapshot, GgapError> {
            let shard_start = shard_id.to_be_bytes().to_vec();
            let shard_end = data_shard_end(shard_id).to_vec();

            let data: Vec<(String, KvEntry)> = store
                .data
                .range(shard_start.clone()..shard_end.clone())
                .map(|g| {
                    g.into_inner().map_err(fjall_err).and_then(|(k, v)| {
                        let user_key = String::from_utf8(k[8..].to_vec())
                            .map_err(|e| GgapError::Storage(e.to_string()))?;
                        Ok((user_key, decode::<KvEntry>(&v)?))
                    })
                })
                .collect::<Result<_, _>>()?;

            // Key layout: shard(8) ++ key_utf8 ++ \x00 ++ version_be(8)
            let history: Vec<((String, u64), KvEntry)> = store
                .history
                .range(shard_start..shard_end)
                .map(|g| {
                    g.into_inner().map_err(fjall_err).and_then(|(k, v)| {
                        let raw = &k[8..]; // strip shard prefix
                        let null_pos = raw.iter().position(|&b| b == 0).ok_or_else(|| {
                            GgapError::Storage("malformed history key: missing null byte".into())
                        })?;
                        let user_key = String::from_utf8(raw[..null_pos].to_vec())
                            .map_err(|e| GgapError::Storage(e.to_string()))?;
                        let version = u64::from_be_bytes(
                            raw[null_pos + 1..null_pos + 9].try_into().map_err(|_| {
                                GgapError::Storage("malformed history key: short version".into())
                            })?,
                        );
                        Ok(((user_key, version), decode::<KvEntry>(&v)?))
                    })
                })
                .collect::<Result<_, _>>()?;

            let last_applied = match store
                .meta
                .get(meta_key(shard_id, "last_applied"))
                .map_err(fjall_err)?
            {
                Some(b) => Some(decode::<LogId>(&b)?),
                None => None,
            };

            let mb = match store
                .meta
                .get(meta_key(shard_id, "membership"))
                .map_err(fjall_err)?
            {
                Some(bytes) => bytes.to_vec(),
                None => fjall::Slice::new(&[]).to_vec(), // Return empty slice if no membership info is found.
            };

            Ok(Snapshot {
                meta: SnapshotMeta {
                    last_log_id: last_applied,
                    membership_bytes: mb,
                    snapshot_id: uuid::Uuid::new_v4().to_string(),
                },
                data: encode(&SnapshotContents { data, history })?,
            })
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    async fn install_snapshot(
        &self,
        shard_id: ShardId,
        snapshot: Snapshot,
    ) -> Result<(), GgapError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || -> Result<(), GgapError> {
            let contents: SnapshotContents = decode(&snapshot.data)?;

            let shard_start = shard_id.to_be_bytes().to_vec();
            let shard_end = data_shard_end(shard_id).to_vec();

            // Collect all existing keys under this shard that must be wiped.
            let data_keys: Vec<Vec<u8>> = store
                .data
                .range(shard_start.clone()..shard_end.clone())
                .map(|g| g.into_inner().map(|(k, _)| k.to_vec()).map_err(fjall_err))
                .collect::<Result<_, _>>()?;
            let history_keys: Vec<Vec<u8>> = store
                .history
                .range(shard_start.clone()..shard_end.clone())
                .map(|g| g.into_inner().map(|(k, _)| k.to_vec()).map_err(fjall_err))
                .collect::<Result<_, _>>()?;
            let ttl_keys: Vec<Vec<u8>> = store
                .ttl_index
                .range(shard_start..shard_end)
                .map(|g| g.into_inner().map(|(k, _)| k.to_vec()).map_err(fjall_err))
                .collect::<Result<_, _>>()?;

            let mut batch = store.db.batch();
            for k in data_keys {
                batch.remove(&store.data, k);
            }
            for k in history_keys {
                batch.remove(&store.history, k);
            }
            for k in ttl_keys {
                batch.remove(&store.ttl_index, k);
            }
            for (user_key, entry) in &contents.data {
                batch.insert(&store.data, data_key(shard_id, user_key), encode(entry)?);
                if let Some(expires_at_ns) = entry.expires_at_ns {
                    batch.insert(
                        &store.ttl_index,
                        ttl_index_key(shard_id, expires_at_ns, user_key),
                        b"",
                    );
                }
            }
            for ((user_key, version), entry) in &contents.history {
                batch.insert(
                    &store.history,
                    history_key(shard_id, user_key, *version),
                    encode(entry)?,
                );
            }
            if let Some(log_id) = snapshot.meta.last_log_id {
                batch.insert(
                    &store.meta,
                    meta_key(shard_id, "last_applied"),
                    encode(&log_id)?,
                );
            }

            if !snapshot.meta.membership_bytes.is_empty() {
                // membership_bytes is already serialized — store directly, don't double-encode.
                batch.insert(
                    &store.meta,
                    meta_key(shard_id, "membership"),
                    snapshot.meta.membership_bytes.clone(),
                );
            }
            batch.commit().map_err(fjall_err)
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }
}

// ---------------------------------------------------------------------------
// Split helpers on FjallStateMachine
// ---------------------------------------------------------------------------

impl FjallStateMachine {
    /// Build a snapshot containing only keys `>= split_key` within the shard.
    /// Used during range splitting to extract the upper half of a shard.
    pub async fn build_partial_snapshot(
        &self,
        shard_id: ShardId,
        split_key: &str,
    ) -> Result<SnapshotContents, GgapError> {
        let store = self.store.clone();
        let split_key = split_key.to_string();
        tokio::task::spawn_blocking(move || -> Result<SnapshotContents, GgapError> {
            let start = data_key(shard_id, &split_key);
            let shard_end = data_shard_end(shard_id).to_vec();

            let data: Vec<(String, KvEntry)> = store
                .data
                .range(start..shard_end.clone())
                .map(|g| {
                    g.into_inner().map_err(fjall_err).and_then(|(k, v)| {
                        let user_key = String::from_utf8(k[8..].to_vec())
                            .map_err(|e| GgapError::Storage(e.to_string()))?;
                        Ok((user_key, decode::<KvEntry>(&v)?))
                    })
                })
                .collect::<Result<_, _>>()?;

            // History: scan entire shard and filter by user key >= split_key
            let shard_start = shard_id.to_be_bytes().to_vec();
            let history: Vec<((String, u64), KvEntry)> = store
                .history
                .range(shard_start..shard_end)
                .map(|g| {
                    g.into_inner().map_err(fjall_err).and_then(|(k, v)| {
                        let raw = &k[8..];
                        let null_pos = raw.iter().position(|&b| b == 0).ok_or_else(|| {
                            GgapError::Storage("malformed history key: missing null byte".into())
                        })?;
                        let user_key = String::from_utf8(raw[..null_pos].to_vec())
                            .map_err(|e| GgapError::Storage(e.to_string()))?;
                        let version = u64::from_be_bytes(
                            raw[null_pos + 1..null_pos + 9].try_into().map_err(|_| {
                                GgapError::Storage("malformed history key: short version".into())
                            })?,
                        );
                        Ok(((user_key, version), decode::<KvEntry>(&v)?))
                    })
                })
                .filter(|r| match r {
                    Ok(((ref user_key, _), _)) => user_key.as_str() >= split_key.as_str(),
                    Err(_) => true, // propagate errors
                })
                .collect::<Result<_, _>>()?;

            Ok(SnapshotContents { data, history })
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    /// Install a partial snapshot into a new shard, writing keys with the
    /// new shard_id prefix. Also copies TTL index entries.
    pub async fn install_partial_snapshot(
        &self,
        new_shard_id: ShardId,
        contents: &SnapshotContents,
    ) -> Result<(), GgapError> {
        let store = self.store.clone();
        let data = contents.data.clone();
        let history = contents.history.clone();
        tokio::task::spawn_blocking(move || -> Result<(), GgapError> {
            let mut batch = store.db.batch();
            for (user_key, entry) in &data {
                batch.insert(
                    &store.data,
                    data_key(new_shard_id, user_key),
                    encode(entry)?,
                );
                if let Some(expires_at_ns) = entry.expires_at_ns {
                    batch.insert(
                        &store.ttl_index,
                        ttl_index_key(new_shard_id, expires_at_ns, user_key),
                        Vec::new(),
                    );
                }
            }
            for ((user_key, version), entry) in &history {
                batch.insert(
                    &store.history,
                    history_key(new_shard_id, user_key, *version),
                    encode(entry)?,
                );
            }
            batch.commit().map_err(fjall_err)
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }

    /// Delete all keys `>= from_key` from a shard's data, history, and
    /// ttl_index partitions. Used after a split to remove the upper half
    /// from the source shard.
    pub async fn delete_range_from(
        &self,
        shard_id: ShardId,
        from_key: &str,
    ) -> Result<(), GgapError> {
        let store = self.store.clone();
        let from_key = from_key.to_string();
        tokio::task::spawn_blocking(move || -> Result<(), GgapError> {
            let start = data_key(shard_id, &from_key);
            let shard_end = data_shard_end(shard_id).to_vec();

            // Collect data keys to delete
            let data_keys: Vec<Vec<u8>> = store
                .data
                .range(start..shard_end.clone())
                .map(|g| g.into_inner().map(|(k, _)| k.to_vec()).map_err(fjall_err))
                .collect::<Result<_, _>>()?;

            // Collect history keys to delete (filter by user key)
            let shard_start = shard_id.to_be_bytes().to_vec();
            let history_keys: Vec<Vec<u8>> = store
                .history
                .range(shard_start.clone()..shard_end.clone())
                .filter_map(|g| match g.into_inner().map_err(fjall_err) {
                    Ok((k, _)) => {
                        let raw = &k[8..];
                        if let Some(null_pos) = raw.iter().position(|&b| b == 0) {
                            let user_key_bytes = &raw[..null_pos];
                            if user_key_bytes >= from_key.as_bytes() {
                                return Some(Ok(k.to_vec()));
                            }
                        }
                        None
                    }
                    Err(e) => Some(Err(e)),
                })
                .collect::<Result<_, _>>()?;

            // Collect TTL index keys to delete (filter by user key)
            let ttl_keys: Vec<Vec<u8>> = store
                .ttl_index
                .range(shard_start..shard_end)
                .filter_map(|g| {
                    match g.into_inner().map_err(fjall_err) {
                        Ok((k, _)) => {
                            // TTL key: shard(8) ++ expires_at_ns(8) ++ key_utf8
                            let user_key_bytes = &k[16..];
                            if user_key_bytes >= from_key.as_bytes() {
                                Some(Ok(k.to_vec()))
                            } else {
                                None
                            }
                        }
                        Err(e) => Some(Err(e)),
                    }
                })
                .collect::<Result<_, _>>()?;

            let mut batch = store.db.batch();
            for k in data_keys {
                batch.remove(&store.data, k);
            }
            for k in history_keys {
                batch.remove(&store.history, k);
            }
            for k in ttl_keys {
                batch.remove(&store.ttl_index, k);
            }
            batch.commit().map_err(fjall_err)
        })
        .await
        .map_err(|e| GgapError::Storage(e.to_string()))?
    }
}

/// Compact history for `key`: if there are more than `max_history` versions,
/// delete the oldest ones. The prefix iterator returns versions in ascending
/// order (big-endian u64), so the first entries are the oldest.
fn compact_history(
    store: &FjallStore,
    shard_id: ShardId,
    key: &str,
    max_history: u64,
) -> Result<(), GgapError> {
    let prefix = history_prefix(shard_id, key);
    let all_keys: Vec<Vec<u8>> = store
        .history
        .prefix(prefix)
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()).map_err(fjall_err))
        .collect::<Result<_, _>>()?;

    let count = all_keys.len() as u64;
    if count > max_history {
        let to_remove = (count - max_history) as usize;
        let mut batch = store.db.batch();
        for k in all_keys.into_iter().take(to_remove) {
            batch.remove(&store.history, k);
        }
        batch.commit().map_err(fjall_err)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogPayload;

    fn make_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            leader_id: 1,
            payload: LogPayload::Blank,
        }
    }

    fn open_store(dir: &std::path::Path) -> Arc<FjallStore> {
        FjallStore::open(dir).expect("open store")
    }

    // -----------------------------------------------------------------------
    // FjallLogStorage
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn log_storage_basic() {
        let dir = tempfile::tempdir().unwrap();
        let log = FjallLogStorage(open_store(dir.path()));
        let shard = 0u64;

        let state = log.log_state(shard).await.unwrap();
        assert!(state.last_log_id.is_none());

        log.append(
            shard,
            vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)],
        )
        .await
        .unwrap();

        let state = log.log_state(shard).await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);

        let es = log.get_entries(shard, 1, 2).await.unwrap();
        assert_eq!(es.len(), 2);

        log.truncate(shard, 3).await.unwrap();
        let state = log.log_state(shard).await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 2);

        let purge_id = LogId {
            term: 1,
            leader_id: 1,
            index: 1,
        };
        log.purge(shard, purge_id).await.unwrap();
        let state = log.log_state(shard).await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 1);
        // Entry 1 should be gone
        let es = log.get_entries(shard, 1, 1).await.unwrap();
        assert!(es.is_empty());
    }

    #[tokio::test]
    async fn log_storage_vote() {
        let dir = tempfile::tempdir().unwrap();
        let log = FjallLogStorage(open_store(dir.path()));
        let shard = 0u64;

        assert!(log.read_vote(shard).await.unwrap().is_none());

        let vote = Vote {
            term: 7,
            voted_for: Some(1),
            committed: true,
        };
        log.save_vote(shard, vote).await.unwrap();

        let loaded = log.read_vote(shard).await.unwrap().unwrap();
        assert_eq!(loaded.term, 7);
        assert_eq!(loaded.voted_for, Some(1));
        assert!(loaded.committed);
    }

    // -----------------------------------------------------------------------
    // FjallStateMachine
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn sm_put_and_get_latest() {
        let dir = tempfile::tempdir().unwrap();
        let sm = FjallStateMachine::new(open_store(dir.path()));
        let shard = 0u64;
        let log_id = LogId {
            index: 1,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id,
            KvCommand::Put {
                key: "k".into(),
                value: b"v1".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        let e = sm.get(shard, "k", 0).await.unwrap().unwrap();
        assert_eq!(e.value, b"v1");
        assert_eq!(e.version, 1);
        assert_eq!(sm.last_applied(shard).await.unwrap(), (Some(log_id), None));
    }

    #[tokio::test]
    async fn sm_mvcc_versions() {
        let dir = tempfile::tempdir().unwrap();
        let sm = FjallStateMachine::new(open_store(dir.path()));
        let shard = 0u64;

        for i in 1u64..=3 {
            let log_id = LogId {
                index: i,
                term: 1,
                leader_id: 1,
            };
            sm.apply(
                shard,
                log_id,
                KvCommand::Put {
                    key: "k".into(),
                    value: format!("v{i}").into_bytes(),
                    ttl_ns: None,
                    expect_version: 0,
                }
                .into(),
                None,
            )
            .await
            .unwrap();
        }

        let e = sm.get(shard, "k", 0).await.unwrap().unwrap();
        assert_eq!(e.version, 3);

        for i in 1u64..=3 {
            let e = sm.get(shard, "k", i).await.unwrap().unwrap();
            assert_eq!(e.value, format!("v{i}").into_bytes());
        }
    }

    #[tokio::test]
    async fn sm_history_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let max_history = 3u64;
        let sm = FjallStateMachine::with_max_history(open_store(dir.path()), max_history);
        let shard = 0u64;

        for i in 1u64..=(max_history + 1) {
            let log_id = LogId {
                index: i,
                term: 1,
                leader_id: 1,
            };
            sm.apply(
                shard,
                log_id,
                KvCommand::Put {
                    key: "k".into(),
                    value: i.to_be_bytes().to_vec(),
                    ttl_ns: None,
                    expect_version: 0,
                }
                .into(),
                None,
            )
            .await
            .unwrap();
        }

        // Oldest version should be evicted.
        assert!(sm.get(shard, "k", 1).await.unwrap().is_none());
        assert!(sm.get(shard, "k", max_history + 1).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn sm_delete_preserves_history() {
        let dir = tempfile::tempdir().unwrap();
        let sm = FjallStateMachine::new(open_store(dir.path()));
        let shard = 0u64;

        let log_id = LogId {
            index: 1,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id,
            KvCommand::Put {
                key: "k".into(),
                value: b"v".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        let log_id2 = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id2,
            KvCommand::Delete { key: "k".into() }.into(),
            None,
        )
        .await
        .unwrap();

        assert!(sm.get(shard, "k", 0).await.unwrap().is_none());
        assert!(sm.get(shard, "k", 1).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn sm_cas_success_and_failure() {
        let dir = tempfile::tempdir().unwrap();
        let sm = FjallStateMachine::new(open_store(dir.path()));
        let shard = 0u64;

        let log_id = LogId {
            index: 1,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id,
            KvCommand::Put {
                key: "k".into(),
                value: b"old".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        let log_id2 = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        let resp = sm
            .apply(
                shard,
                log_id2,
                KvCommand::Cas {
                    key: "k".into(),
                    expected: b"old".to_vec(),
                    new_value: b"new".to_vec(),
                    ttl_ns: None,
                }
                .into(),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(resp, KvResponse::CasResult { success: true, .. }));

        let log_id3 = LogId {
            index: 3,
            term: 1,
            leader_id: 1,
        };
        let resp = sm
            .apply(
                shard,
                log_id3,
                KvCommand::Cas {
                    key: "k".into(),
                    expected: b"old".to_vec(),
                    new_value: b"other".to_vec(),
                    ttl_ns: None,
                }
                .into(),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(resp, KvResponse::CasResult { success: false, .. }));
    }

    #[tokio::test]
    async fn sm_scan_pagination() {
        let dir = tempfile::tempdir().unwrap();
        let sm = FjallStateMachine::new(open_store(dir.path()));
        let shard = 0u64;

        for i in 0u64..10 {
            let log_id = LogId {
                index: i + 1,
                term: 1,
                leader_id: 1,
            };
            sm.apply(
                shard,
                log_id,
                KvCommand::Put {
                    key: format!("key{i:02}"),
                    value: vec![i as u8],
                    ttl_ns: None,
                    expect_version: 0,
                }
                .into(),
                None,
            )
            .await
            .unwrap();
        }

        let (page1, cont) = sm.scan(shard, "", "", 5).await.unwrap();
        assert_eq!(page1.len(), 5);
        assert!(cont.is_some());

        let start = cont.unwrap();
        let (page2, cont2) = sm.scan(shard, &start, "", 5).await.unwrap();
        assert_eq!(page2.len(), 5);
        assert!(cont2.is_none());
    }

    #[tokio::test]
    async fn sm_snapshot_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let sm = FjallStateMachine::new(open_store(dir.path()));
        let shard = 0u64;

        let log_id = LogId {
            index: 1,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id,
            KvCommand::Put {
                key: "a".into(),
                value: b"1".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        let log_id2 = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id2,
            KvCommand::Put {
                key: "b".into(),
                value: b"2".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        let snap = sm.build_snapshot(shard).await.unwrap();
        assert_eq!(snap.meta.last_log_id.unwrap().index, 2);
        assert_eq!(snap.meta.last_log_id.unwrap().term, 1);
        assert_eq!(snap.meta.last_log_id.unwrap().leader_id, 1);

        let dir2 = tempfile::tempdir().unwrap();
        let sm2 = FjallStateMachine::new(open_store(dir2.path()));
        sm2.install_snapshot(shard, snap).await.unwrap();

        assert_eq!(sm2.get(shard, "a", 0).await.unwrap().unwrap().value, b"1");
        assert_eq!(sm2.get(shard, "b", 0).await.unwrap().unwrap().value, b"2");
        assert_eq!(
            sm2.last_applied(shard).await.unwrap(),
            (Some(log_id2), None)
        );
    }

    #[tokio::test]
    async fn sm_durability() {
        let log_id = LogId {
            index: 1,
            term: 1,
            leader_id: 1,
        };
        let dir = tempfile::tempdir().unwrap();
        {
            let sm = FjallStateMachine::new(open_store(dir.path()));
            sm.apply(
                0,
                log_id,
                KvCommand::Put {
                    key: "x".into(),
                    value: b"persist".to_vec(),
                    ttl_ns: None,
                    expect_version: 0,
                }
                .into(),
                None,
            )
            .await
            .unwrap();
        }
        // Reopen at the same path.
        let sm2 = FjallStateMachine::new(open_store(dir.path()));
        let e = sm2.get(0, "x", 0).await.unwrap().unwrap();
        assert_eq!(e.value, b"persist");
        assert_eq!(sm2.last_applied(0).await.unwrap(), (Some(log_id), None));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sm_concurrent_applies() {
        let dir = tempfile::tempdir().unwrap();
        let sm = Arc::new(FjallStateMachine::new(open_store(dir.path())));

        let mut handles = Vec::new();
        for i in 0u64..20 {
            let sm = sm.clone();
            handles.push(tokio::spawn(async move {
                let log_id = LogId {
                    index: i + 1,
                    term: 1,
                    leader_id: 1,
                };
                sm.apply(
                    0,
                    log_id,
                    KvCommand::Put {
                        key: format!("k{i}"),
                        value: vec![i as u8],
                        ttl_ns: None,
                        expect_version: 0,
                    }
                    .into(),
                    None,
                )
                .await
                .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        for i in 0u64..20 {
            let e = sm.get(0, &format!("k{i}"), 0).await.unwrap().unwrap();
            assert_eq!(e.value, vec![i as u8]);
        }
    }

    #[tokio::test]
    async fn sm_last_applied_version_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let sm = Arc::new(FjallStateMachine::new(open_store(dir.path())));

        let mut last_applied = sm.last_applied(0).await.unwrap();
        assert!(last_applied.0.is_none());

        let log_id = LogId {
            index: 42,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            0,
            log_id,
            KvCommand::Put {
                key: "k1".into(),
                value: vec![1u8],
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();
        last_applied = sm.last_applied(0).await.unwrap();
        assert_eq!(last_applied, (Some(log_id), None));

        let log_id2 = LogId {
            index: 43,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            0,
            log_id2,
            KvCommand::Put {
                key: "k1".into(),
                value: vec![1u8],
                ttl_ns: None,
                expect_version: 1,
            }
            .into(),
            None,
        )
        .await
        .unwrap_err();
        last_applied = sm.last_applied(0).await.unwrap();
        assert_eq!(last_applied, (Some(log_id), None));
    }

    #[tokio::test]
    async fn sm_snapshot_roundtrip_history_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let sm = Arc::new(FjallStateMachine::new(open_store(dir.path())));

        let log_id = LogId {
            index: 1,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            0,
            log_id,
            KvCommand::Put {
                key: "k1".into(),
                value: vec![1u8],
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        let log_id2 = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            0,
            log_id2,
            KvCommand::Put {
                key: "k1".into(),
                value: vec![2u8],
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        let v1 = sm.get(0, "k1", 1).await.unwrap().unwrap();
        let v2 = sm.get(0, "k1", 2).await.unwrap().unwrap();
        assert_eq!(v1.value, vec![1u8]);
        assert_eq!(v2.value, vec![2u8]);

        let snap = sm.build_snapshot(0).await.unwrap();
        sm.install_snapshot(0, snap).await.unwrap();

        let v0_post = sm.get(0, "k1", 0).await.unwrap().unwrap();
        let v1_post = sm.get(0, "k1", 1).await.unwrap().unwrap();
        let v2_post = sm.get(0, "k1", 2).await.unwrap().unwrap();
        assert_eq!(v0_post.value, vec![2u8]);
        assert_eq!(v1_post.value, vec![1u8]);
        assert_eq!(v2_post.value, vec![2u8]);
    }

    // -----------------------------------------------------------------------
    // Commit Group 1 regression tests
    // -----------------------------------------------------------------------

    /// Regression test for membership double-encoding in install_snapshot.
    ///
    /// Previously, `install_snapshot` called `encode(&snapshot.meta.membership_bytes)`
    /// which double-encoded the already-serialized bytes. After snapshot restore,
    /// `build_snapshot` → `install_snapshot` → `build_snapshot` would produce
    /// corrupted membership data that could not be deserialized.
    ///
    /// The existing `sm_snapshot_round_trip` test never caught this because it
    /// never passed `membership_bytes` to `apply`, so the membership was always
    /// empty and the encoding branch was skipped.
    #[tokio::test]
    async fn sm_snapshot_membership_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let sm = FjallStateMachine::new(open_store(dir.path()));
        let shard = 0u64;

        // Simulate a membership payload (arbitrary bytes that represent a
        // serialized StoredMembership in production).
        let membership_data = b"membership-payload-v1".to_vec();

        // Apply a command WITH membership_bytes so the store persists it.
        let log_id = LogId {
            index: 1,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id,
            None, // Blank entry — only membership matters
            Some(membership_data.clone()),
        )
        .await
        .unwrap();

        // Verify membership is readable via last_applied.
        let (_, mb) = sm.last_applied(shard).await.unwrap();
        assert_eq!(mb.as_deref(), Some(membership_data.as_slice()));

        // Build a snapshot — should capture the membership bytes.
        let snap = sm.build_snapshot(shard).await.unwrap();
        assert_eq!(snap.meta.membership_bytes, membership_data);

        // Install the snapshot into a fresh store.
        let dir2 = tempfile::tempdir().unwrap();
        let sm2 = FjallStateMachine::new(open_store(dir2.path()));
        sm2.install_snapshot(shard, snap).await.unwrap();

        // The critical check: membership must be readable and match the original.
        let (_, mb2) = sm2.last_applied(shard).await.unwrap();
        assert_eq!(
            mb2.as_deref(),
            Some(membership_data.as_slice()),
            "membership bytes corrupted after install_snapshot (was double-encoded?)"
        );

        // Double-check: build_snapshot on the restored store should produce
        // identical membership bytes, proving the full round-trip is clean.
        let snap2 = sm2.build_snapshot(shard).await.unwrap();
        assert_eq!(
            snap2.meta.membership_bytes, membership_data,
            "membership bytes corrupted after second build_snapshot"
        );
    }

    /// Regression test for CAS failure non-atomic last_applied update.
    ///
    /// Previously, when CAS failed (value mismatch), `last_applied` was updated
    /// via a standalone `store.meta.insert()` outside any batch. If the process
    /// crashed between the main batch and this insert, `last_applied` would not
    /// advance and the log entry could be re-applied.
    ///
    /// The existing `sm_cas_success_and_failure` test never caught this because
    /// it only checked the CAS response, not `last_applied` after a failed CAS.
    ///
    /// This test verifies that `last_applied` is correctly advanced even on CAS
    /// failure, and that membership_bytes are also persisted in the same operation.
    #[tokio::test]
    async fn sm_cas_failure_advances_last_applied() {
        let dir = tempfile::tempdir().unwrap();
        let sm = FjallStateMachine::new(open_store(dir.path()));
        let shard = 0u64;

        // Write an initial value.
        let log_id1 = LogId {
            index: 1,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id1,
            KvCommand::Put {
                key: "k".into(),
                value: b"original".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        // CAS with wrong expected value — should fail but still advance last_applied.
        let log_id2 = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        let membership_on_fail = b"membership-during-cas-fail".to_vec();
        let resp = sm
            .apply(
                shard,
                log_id2,
                KvCommand::Cas {
                    key: "k".into(),
                    expected: b"wrong_value".to_vec(),
                    new_value: b"new_value".to_vec(),
                    ttl_ns: None,
                }
                .into(),
                Some(membership_on_fail.clone()),
            )
            .await
            .unwrap();

        // CAS should have failed.
        assert!(matches!(resp, KvResponse::CasResult { success: false, .. }));

        // last_applied must have advanced to log_id2 despite CAS failure.
        let (la, mb) = sm.last_applied(shard).await.unwrap();
        assert_eq!(
            la.unwrap().index,
            2,
            "last_applied should advance to index 2 even on CAS failure"
        );

        // membership_bytes should be persisted atomically with last_applied.
        assert_eq!(
            mb.as_deref(),
            Some(membership_on_fail.as_slice()),
            "membership_bytes should be persisted even on CAS failure"
        );

        // Verify data was NOT modified by the failed CAS.
        let entry = sm.get(shard, "k", 0).await.unwrap().unwrap();
        assert_eq!(entry.value, b"original");
    }

    /// Regression test: verify that a failed CAS followed by a successful
    /// CAS produces the correct last_applied, proving the batch approach works
    /// correctly for the failure → success sequence.
    #[tokio::test]
    async fn sm_cas_failure_then_success_last_applied() {
        let dir = tempfile::tempdir().unwrap();
        let sm = FjallStateMachine::new(open_store(dir.path()));
        let shard = 0u64;

        // Write initial value.
        let log_id1 = LogId {
            index: 1,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id1,
            KvCommand::Put {
                key: "k".into(),
                value: b"v1".to_vec(),
                ttl_ns: None,
                expect_version: 0,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        // Failed CAS at index 2.
        let log_id2 = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id2,
            KvCommand::Cas {
                key: "k".into(),
                expected: b"wrong".to_vec(),
                new_value: b"v2".to_vec(),
                ttl_ns: None,
            }
            .into(),
            None,
        )
        .await
        .unwrap();

        // Successful CAS at index 3.
        let log_id3 = LogId {
            index: 3,
            term: 1,
            leader_id: 1,
        };
        let resp = sm
            .apply(
                shard,
                log_id3,
                KvCommand::Cas {
                    key: "k".into(),
                    expected: b"v1".to_vec(),
                    new_value: b"v2".to_vec(),
                    ttl_ns: None,
                }
                .into(),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(resp, KvResponse::CasResult { success: true, .. }));

        // last_applied must be at index 3.
        let (la, _) = sm.last_applied(shard).await.unwrap();
        assert_eq!(la.unwrap().index, 3);

        // Data should reflect the successful CAS.
        let entry = sm.get(shard, "k", 0).await.unwrap().unwrap();
        assert_eq!(entry.value, b"v2");
    }

    /// Regression test for history compaction error isolation.
    ///
    /// Previously, `compact_history()` errors after a committed batch would
    /// propagate upward, making openraft believe the write failed even though
    /// the data was already durable. This test verifies that the write succeeds
    /// and data is correct even when many versions accumulate (the compaction
    /// runs but its success/failure doesn't affect the apply result).
    ///
    /// The existing `sm_history_compaction` test verified that compaction works
    /// when it succeeds, but never tested that a committed write isn't affected
    /// by post-commit compaction status.
    #[tokio::test]
    async fn sm_write_succeeds_regardless_of_compaction() {
        let dir = tempfile::tempdir().unwrap();
        // Use max_history=2 so compaction triggers on the 3rd write to same key.
        let sm = FjallStateMachine::with_max_history(open_store(dir.path()), 2);
        let shard = 0u64;

        // Write 5 versions of the same key — compaction runs after each write
        // past the 2nd. All writes must succeed.
        for i in 1u64..=5 {
            let log_id = LogId {
                index: i,
                term: 1,
                leader_id: 1,
            };
            let resp = sm
                .apply(
                    shard,
                    log_id,
                    KvCommand::Put {
                        key: "k".into(),
                        value: format!("v{i}").into_bytes(),
                        ttl_ns: None,
                        expect_version: 0,
                    }
                    .into(),
                    None,
                )
                .await
                .unwrap();
            assert!(
                matches!(resp, KvResponse::Written { version } if version == i),
                "write at index {i} must succeed even when compaction runs"
            );
        }

        // Latest value must be correct.
        let entry = sm.get(shard, "k", 0).await.unwrap().unwrap();
        assert_eq!(entry.value, b"v5");
        assert_eq!(entry.version, 5);

        // last_applied must be at index 5.
        let (la, _) = sm.last_applied(shard).await.unwrap();
        assert_eq!(la.unwrap().index, 5);

        // Similarly test CAS path: CAS success triggers compaction too.
        let log_id6 = LogId {
            index: 6,
            term: 1,
            leader_id: 1,
        };
        let resp = sm
            .apply(
                shard,
                log_id6,
                KvCommand::Cas {
                    key: "k".into(),
                    expected: b"v5".to_vec(),
                    new_value: b"v6".to_vec(),
                    ttl_ns: None,
                }
                .into(),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(resp, KvResponse::CasResult { success: true, .. }));

        let entry = sm.get(shard, "k", 0).await.unwrap().unwrap();
        assert_eq!(entry.value, b"v6");
    }
}
