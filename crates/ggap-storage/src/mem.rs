use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tokio::sync::RwLock;

use ggap_types::{system_now_fn, GgapError, KvCommand, KvEntry, KvResponse, LogId, NowFn, ShardId};

use crate::traits::{LogStorage, StateMachineStore};
use crate::types::{LogEntry, LogState, Snapshot, SnapshotContents, SnapshotMeta, Vote};

/// Maximum MVCC history versions retained per key before compaction.
const MAX_HISTORY: usize = 10;

fn encode<T: serde::Serialize>(val: &T) -> Result<Vec<u8>, GgapError> {
    bincode::serde::encode_to_vec(val, bincode::config::standard())
        .map_err(|e| GgapError::Storage(e.to_string()))
}

fn decode<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8]) -> Result<T, GgapError> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| GgapError::Storage(e.to_string()))
}

// ---------------------------------------------------------------------------
// MemLogStorage
// ---------------------------------------------------------------------------

struct MemLogInner {
    entries: BTreeMap<u64, LogEntry>, // index → entry (single shard, Phase 3)
    last_purged: Option<LogId>,
    votes: HashMap<u64, Vote>,              // shard_id → vote
    committed: HashMap<u64, Option<LogId>>, // shard_id → committed log id
}

/// In-memory `LogStorage` backed by a `BTreeMap`.
///
/// Intended for unit tests; not persisted across restarts.
#[derive(Clone)]
pub struct MemLogStorage {
    inner: Arc<RwLock<MemLogInner>>,
}

impl MemLogStorage {
    pub fn new() -> Self {
        MemLogStorage {
            inner: Arc::new(RwLock::new(MemLogInner {
                entries: BTreeMap::new(),
                last_purged: None,
                votes: HashMap::new(),
                committed: HashMap::new(),
            })),
        }
    }
}

impl Default for MemLogStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStorage for MemLogStorage {
    async fn log_state(&self, _shard_id: ShardId) -> Result<LogState, GgapError> {
        let g = self.inner.read().await;
        let last_log_id = g
            .entries
            .values()
            .next_back()
            .map(|e| LogId {
                term: e.term,
                leader_id: e.leader_id,
                index: e.index,
            })
            .or(g.last_purged);
        Ok(LogState {
            last_log_id,
            last_purged_log_id: g.last_purged,
        })
    }

    async fn get_entries(
        &self,
        _shard_id: ShardId,
        from: u64,
        to_inclusive: u64,
    ) -> Result<Vec<LogEntry>, GgapError> {
        let g = self.inner.read().await;
        Ok(g.entries
            .range(from..=to_inclusive)
            .map(|(_, e)| e.clone())
            .collect())
    }

    async fn append(&self, _shard_id: ShardId, entries: Vec<LogEntry>) -> Result<(), GgapError> {
        let mut g = self.inner.write().await;
        for entry in entries {
            g.entries.insert(entry.index, entry);
        }
        Ok(())
    }

    async fn truncate(&self, _shard_id: ShardId, from_index: u64) -> Result<(), GgapError> {
        let mut g = self.inner.write().await;
        g.entries.retain(|&idx, _| idx < from_index);
        Ok(())
    }

    async fn purge(&self, _shard_id: ShardId, up_to: LogId) -> Result<(), GgapError> {
        let mut g = self.inner.write().await;
        g.entries.retain(|&idx, _| idx > up_to.index);
        g.last_purged = Some(up_to);
        Ok(())
    }

    async fn save_vote(&self, shard_id: ShardId, vote: Vote) -> Result<(), GgapError> {
        self.inner.write().await.votes.insert(shard_id, vote);
        Ok(())
    }

    async fn read_vote(&self, shard_id: ShardId) -> Result<Option<Vote>, GgapError> {
        Ok(self.inner.read().await.votes.get(&shard_id).cloned())
    }

    async fn save_committed(
        &self,
        shard_id: ShardId,
        committed: Option<LogId>,
    ) -> Result<(), GgapError> {
        self.inner
            .write()
            .await
            .committed
            .insert(shard_id, committed);
        Ok(())
    }

    async fn read_committed(&self, shard_id: ShardId) -> Result<Option<LogId>, GgapError> {
        Ok(self
            .inner
            .read()
            .await
            .committed
            .get(&shard_id)
            .cloned()
            .flatten())
    }
}

// ---------------------------------------------------------------------------
// MemStateMachine
// ---------------------------------------------------------------------------

struct MemSmInner {
    data: BTreeMap<String, KvEntry>,
    /// (key, version) → entry for MVCC point reads.
    history: BTreeMap<(String, u64), KvEntry>,
    last_applied: Option<LogId>,
}

/// In-memory `StateMachineStore` backed by `BTreeMap` with MVCC history.
///
/// Intended for unit tests; not persisted across restarts.
pub struct MemStateMachine {
    inner: Arc<RwLock<MemSmInner>>,
    now_fn: NowFn,
}

impl MemStateMachine {
    pub fn new() -> Self {
        MemStateMachine {
            inner: Arc::new(RwLock::new(MemSmInner {
                data: BTreeMap::new(),
                history: BTreeMap::new(),
                last_applied: None,
            })),
            now_fn: system_now_fn(),
        }
    }

    pub fn with_clock(mut self, now_fn: NowFn) -> Self {
        self.now_fn = now_fn;
        self
    }
}

impl Default for MemStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachineStore for MemStateMachine {
    async fn last_applied(
        &self,
        _shard_id: ShardId,
    ) -> Result<(Option<LogId>, Option<Vec<u8>>), GgapError> {
        Ok((self.inner.read().await.last_applied, None))
    }

    async fn apply(
        &self,
        _shard_id: ShardId,
        log_id: LogId,
        cmd: Option<KvCommand>,
        membership_bytes: Option<Vec<u8>>,
    ) -> Result<KvResponse, GgapError> {
        let mut g = self.inner.write().await;
        let now = (self.now_fn)();

        let response = match cmd {
            Some(KvCommand::Put {
                key,
                value,
                ttl_ns,
                expect_version,
            }) => {
                let current_ver = g.data.get(&key).map(|e| e.version).unwrap_or(0);
                if expect_version != 0 && current_ver != expect_version {
                    return Err(GgapError::VersionConflict {
                        expected: expect_version,
                        actual: current_ver,
                    });
                }
                let created_at_ns = g.data.get(&key).map(|e| e.created_at_ns).unwrap_or(now);
                let entry = KvEntry {
                    key: key.clone(),
                    value,
                    version: log_id.index,
                    created_at_ns,
                    modified_at_ns: now,
                    expires_at_ns: ttl_ns.map(|ns| now + ns),
                };
                g.data.insert(key.clone(), entry.clone());
                g.history.insert((key.clone(), log_id.index), entry);

                // Compact history: keep only the MAX_HISTORY most recent versions.
                let count = g
                    .history
                    .range((key.clone(), 0u64)..=(key.clone(), u64::MAX))
                    .count();
                if count > MAX_HISTORY {
                    let to_remove = count - MAX_HISTORY;
                    let old_keys: Vec<(String, u64)> = g
                        .history
                        .range((key.clone(), 0u64)..=(key.clone(), u64::MAX))
                        .take(to_remove)
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in old_keys {
                        g.history.remove(&k);
                    }
                }

                KvResponse::Written {
                    version: log_id.index,
                }
            }

            Some(KvCommand::Delete { key }) => {
                let found = g.data.remove(&key).is_some();
                // History entries are preserved so MVCC reads at old versions still work.
                KvResponse::Deleted { found }
            }

            Some(KvCommand::Cas {
                key,
                expected,
                new_value,
                ttl_ns,
            }) => {
                let current = g.data.get(&key).cloned();
                let matches = current
                    .as_ref()
                    .map(|e| e.value == expected)
                    .unwrap_or(false);
                if matches {
                    let created_at_ns = current.as_ref().unwrap().created_at_ns;
                    let entry = KvEntry {
                        key: key.clone(),
                        value: new_value,
                        version: log_id.index,
                        created_at_ns,
                        modified_at_ns: now,
                        expires_at_ns: ttl_ns.map(|ns| now + ns),
                    };
                    g.data.insert(key.clone(), entry.clone());
                    g.history.insert((key.clone(), log_id.index), entry);

                    let count = g
                        .history
                        .range((key.clone(), 0u64)..=(key.clone(), u64::MAX))
                        .count();
                    if count > MAX_HISTORY {
                        let to_remove = count - MAX_HISTORY;
                        let old_keys: Vec<(String, u64)> = g
                            .history
                            .range((key.clone(), 0u64)..=(key.clone(), u64::MAX))
                            .take(to_remove)
                            .map(|(k, _)| k.clone())
                            .collect();
                        for k in old_keys {
                            g.history.remove(&k);
                        }
                    }
                }
                KvResponse::CasResult {
                    success: matches,
                    current,
                }
            }
            None => {
                // Raft-internal entry (Blank, Membership). No state change; return NoOp.
                KvResponse::NoOp
            }
        };

        if let Some(bytes) = membership_bytes {
            // For simplicity, we ignore membership_bytes in this in-memory implementation.
            // In a real implementation, you would deserialize and apply the membership change here.
            let _ = bytes;
        }

        g.last_applied = Some(log_id);
        Ok(response)
    }

    async fn get(
        &self,
        _shard_id: ShardId,
        key: &str,
        at_version: u64,
    ) -> Result<Option<KvEntry>, GgapError> {
        let g = self.inner.read().await;
        if at_version == 0 {
            Ok(g.data.get(key).cloned())
        } else {
            Ok(g.history.get(&(key.to_string(), at_version)).cloned())
        }
    }

    async fn scan(
        &self,
        _shard_id: ShardId,
        start_key: &str,
        end_key: &str,
        limit: u32,
    ) -> Result<(Vec<KvEntry>, Option<String>), GgapError> {
        let effective_limit = if limit == 0 { 100 } else { limit } as usize;
        let g = self.inner.read().await;

        // Fetch limit+1 to detect whether more pages exist.
        let raw: Vec<KvEntry> = if end_key.is_empty() {
            g.data
                .range(start_key.to_string()..)
                .map(|(_, v)| v.clone())
                .take(effective_limit + 1)
                .collect()
        } else {
            g.data
                .range(start_key.to_string()..end_key.to_string())
                .map(|(_, v)| v.clone())
                .take(effective_limit + 1)
                .collect()
        };

        let has_more = raw.len() > effective_limit;
        let mut entries = raw;
        let continuation = if has_more {
            entries.pop().map(|e| e.key)
        } else {
            None
        };
        Ok((entries, continuation))
    }

    async fn build_snapshot(&self, _shard_id: ShardId) -> Result<Snapshot, GgapError> {
        let g = self.inner.read().await;
        let contents = SnapshotContents {
            data: g.data.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            history: g
                .history
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };

        let last_applied = g.last_applied;

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                membership_bytes: vec![], // For simplicity, we don't capture membership changes in this in-memory implementation.
                snapshot_id: uuid::Uuid::new_v4().to_string(),
            },
            data: encode(&contents)?,
        })
    }

    async fn install_snapshot(
        &self,
        _shard_id: ShardId,
        snapshot: Snapshot,
    ) -> Result<(), GgapError> {
        let contents: SnapshotContents = decode(&snapshot.data)?;
        let mut g = self.inner.write().await;
        g.data = contents.data.into_iter().collect();
        g.history = contents.history.into_iter().collect();
        g.last_applied = snapshot.meta.last_log_id;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogPayload;

    // -----------------------------------------------------------------------
    // MemLogStorage tests
    // -----------------------------------------------------------------------

    fn make_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            leader_id: 1,
            payload: LogPayload::Blank,
        }
    }

    #[tokio::test]
    async fn log_storage_basic() {
        let store = MemLogStorage::new();
        let shard = 0;

        // Empty state
        let state = store.log_state(shard).await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());

        // Append entries 1..=3
        store
            .append(
                shard,
                vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)],
            )
            .await
            .unwrap();

        let state = store.log_state(shard).await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);

        // Get range
        let entries = store.get_entries(shard, 1, 2).await.unwrap();
        assert_eq!(entries.len(), 2);

        // Truncate from index 3 (removes index 3)
        store.truncate(shard, 3).await.unwrap();
        let state = store.log_state(shard).await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 2);

        // Purge up to index 1
        let purge_id = LogId {
            term: 1,
            leader_id: 1,
            index: 1,
        };
        store.purge(shard, purge_id).await.unwrap();
        let state = store.log_state(shard).await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 1);
    }

    #[tokio::test]
    async fn log_storage_vote() {
        let store = MemLogStorage::new();
        let shard = 0;

        assert!(store.read_vote(shard).await.unwrap().is_none());

        let vote = Vote {
            term: 5,
            voted_for: Some(3),
            committed: false,
        };
        store.save_vote(shard, vote.clone()).await.unwrap();

        let loaded = store.read_vote(shard).await.unwrap().unwrap();
        assert_eq!(loaded.term, 5);
        assert_eq!(loaded.voted_for, Some(3));
    }

    // -----------------------------------------------------------------------
    // MemStateMachine tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn sm_put_and_get_latest() {
        let sm = MemStateMachine::new();
        let shard = 0;

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

        let entry = sm.get(shard, "k", 0).await.unwrap().unwrap();
        assert_eq!(entry.value, b"v1");
        assert_eq!(entry.version, 1);
    }

    #[tokio::test]
    async fn sm_mvcc_versions() {
        let sm = MemStateMachine::new();
        let shard = 0;

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

        // Latest
        let e = sm.get(shard, "k", 0).await.unwrap().unwrap();
        assert_eq!(e.version, 3);

        // Historical
        for i in 1u64..=3 {
            let e = sm.get(shard, "k", i).await.unwrap().unwrap();
            assert_eq!(e.value, format!("v{i}").into_bytes());
        }
    }

    #[tokio::test]
    async fn sm_history_compaction() {
        let sm = MemStateMachine::new();
        let shard = 0;

        // Write MAX_HISTORY + 1 versions — oldest should be evicted.
        for i in 1u64..=(MAX_HISTORY as u64 + 1) {
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

        // Version 1 should be gone (compacted).
        assert!(sm.get(shard, "k", 1).await.unwrap().is_none());
        // Version MAX_HISTORY + 1 should still be there.
        assert!(sm
            .get(shard, "k", MAX_HISTORY as u64 + 1)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn sm_delete_preserves_history() {
        let sm = MemStateMachine::new();
        let shard = 0;

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

        let log_id = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id,
            KvCommand::Delete { key: "k".into() }.into(),
            None,
        )
        .await
        .unwrap();

        // Latest read returns None (deleted).
        assert!(sm.get(shard, "k", 0).await.unwrap().is_none());
        // MVCC read at version 1 still works.
        assert!(sm.get(shard, "k", 1).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn sm_cas_success_and_failure() {
        let sm = MemStateMachine::new();
        let shard = 0;

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

        // CAS success
        let log_id = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        let resp = sm
            .apply(
                shard,
                log_id,
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

        // CAS failure (wrong expected value)
        let log_id = LogId {
            index: 3,
            term: 1,
            leader_id: 1,
        };
        let resp = sm
            .apply(
                shard,
                log_id,
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
        let sm = MemStateMachine::new();
        let shard = 0;

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

        // First page: limit=5
        let (page1, cont) = sm.scan(shard, "", "", 5).await.unwrap();
        assert_eq!(page1.len(), 5);
        assert!(cont.is_some());

        // Second page using continuation key
        let start = cont.unwrap();
        let (page2, cont2) = sm.scan(shard, &start, "", 5).await.unwrap();
        assert_eq!(page2.len(), 5);
        assert!(cont2.is_none());
    }

    #[tokio::test]
    async fn sm_snapshot_round_trip() {
        let sm = MemStateMachine::new();
        let shard = 0;

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

        let log_id = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        sm.apply(
            shard,
            log_id,
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

        let snapshot = sm.build_snapshot(shard).await.unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(log_id));

        // Install into a fresh state machine.
        let sm2 = MemStateMachine::new();
        sm2.install_snapshot(shard, snapshot).await.unwrap();

        assert_eq!(sm2.get(shard, "a", 0).await.unwrap().unwrap().value, b"1");
        assert_eq!(sm2.get(shard, "b", 0).await.unwrap().unwrap().value, b"2");
        assert_eq!(sm2.last_applied(shard).await.unwrap(), (Some(log_id), None));
    }

    #[tokio::test]
    async fn sm_version_conflict() {
        let sm = MemStateMachine::new();
        let shard = 0;

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

        let log_id = LogId {
            index: 2,
            term: 1,
            leader_id: 1,
        };
        let err = sm
            .apply(
                shard,
                log_id,
                KvCommand::Put {
                    key: "k".into(),
                    value: b"v2".to_vec(),
                    ttl_ns: None,
                    expect_version: 999, // wrong version
                }
                .into(),
                None,
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            GgapError::VersionConflict {
                expected: 999,
                actual: 1
            }
        ));
    }
}
