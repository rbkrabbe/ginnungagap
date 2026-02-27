use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use ggap_types::{GgapError, KvCommand, KvEntry, KvResponse, ReadMode, ShardId, WriteMode};

// ---------------------------------------------------------------------------
// RaftNode trait
// Uses RPITIT (Return Position Impl Trait In Trait, stable since Rust 1.75).
// The explicit `+ Send` bound on each future ensures generic callers can
// `.await` across thread boundaries.
// ---------------------------------------------------------------------------

pub trait RaftNode: Send + Sync + 'static {
    fn shard_id(&self) -> ShardId;

    fn propose(
        &self,
        cmd: KvCommand,
        mode: WriteMode,
    ) -> impl std::future::Future<Output = Result<KvResponse, GgapError>> + Send;

    /// `at_version == 0` means latest. Stub ignores both `at_version` and `mode`.
    fn read(
        &self,
        key: &str,
        at_version: u64,
        mode: ReadMode,
    ) -> impl std::future::Future<Output = Result<Option<KvEntry>, GgapError>> + Send;

    /// `end_key` empty means unbounded. `limit == 0` means server default (100).
    /// Returns `(entries, continuation_key)` — `Some(key)` when more results exist.
    fn scan(
        &self,
        start_key: &str,
        end_key: &str,
        limit: u32,
        mode: ReadMode,
    ) -> impl std::future::Future<Output = Result<(Vec<KvEntry>, Option<String>), GgapError>> + Send;
}

// ---------------------------------------------------------------------------
// StubRaftNode — in-memory, BTreeMap-backed, no real Raft
// ---------------------------------------------------------------------------

struct StubInner {
    data: BTreeMap<String, KvEntry>,
    next_version: u64,
}

pub struct StubRaftNode {
    inner: Arc<RwLock<StubInner>>,
}

impl StubRaftNode {
    pub fn new() -> Self {
        StubRaftNode {
            inner: Arc::new(RwLock::new(StubInner {
                data: BTreeMap::new(),
                next_version: 1,
            })),
        }
    }
}

impl Default for StubRaftNode {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNode for StubRaftNode {
    fn shard_id(&self) -> ShardId {
        0
    }

    async fn propose(&self, cmd: KvCommand, _mode: WriteMode) -> Result<KvResponse, GgapError> {
        let mut g = self.inner.write().await;
        let now = now_ns();
        match cmd {
            KvCommand::Put { key, value, ttl_ns, expect_version } => {
                let current_ver = g.data.get(&key).map(|e| e.version).unwrap_or(0);
                if expect_version != 0 && current_ver != expect_version {
                    return Err(GgapError::VersionConflict {
                        expected: expect_version,
                        actual: current_ver,
                    });
                }
                let version = g.next_version;
                g.next_version += 1;
                let created_at_ns = g.data.get(&key).map(|e| e.created_at_ns).unwrap_or(now);
                g.data.insert(
                    key.clone(),
                    KvEntry {
                        key,
                        value,
                        version,
                        created_at_ns,
                        modified_at_ns: now,
                        expires_at_ns: ttl_ns.map(|ns| now + ns),
                    },
                );
                Ok(KvResponse::Written { version })
            }
            KvCommand::Delete { key } => {
                Ok(KvResponse::Deleted { found: g.data.remove(&key).is_some() })
            }
            KvCommand::Cas { key, expected, new_value, ttl_ns } => {
                let current = g.data.get(&key).cloned();
                let matches = current.as_ref().map(|e| e.value == expected).unwrap_or(false);
                if matches {
                    let version = g.next_version;
                    g.next_version += 1;
                    let created_at_ns = current.as_ref().unwrap().created_at_ns;
                    g.data.insert(
                        key.clone(),
                        KvEntry {
                            key,
                            value: new_value,
                            version,
                            created_at_ns,
                            modified_at_ns: now,
                            expires_at_ns: ttl_ns.map(|ns| now + ns),
                        },
                    );
                }
                Ok(KvResponse::CasResult { success: matches, current })
            }
        }
    }

    async fn read(
        &self,
        key: &str,
        _at_version: u64,
        _mode: ReadMode,
    ) -> Result<Option<KvEntry>, GgapError> {
        Ok(self.inner.read().await.data.get(key).cloned())
    }

    async fn scan(
        &self,
        start_key: &str,
        end_key: &str,
        limit: u32,
        _mode: ReadMode,
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
        let continuation = if has_more { entries.pop().map(|e| e.key) } else { None };
        Ok((entries, continuation))
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use super::*;

    #[tokio::test]
    async fn test_stub_raft_node() {
        let node = StubRaftNode::new();
        assert_eq!(node.shard_id(), 0);

        // Test Put
        let resp = node.propose(KvCommand::Put { key: "foo".into(), value: "bar".into(), ttl_ns: None, expect_version: 0 }, WriteMode::Majority).await.unwrap();
        assert!(matches!(resp, KvResponse::Written { version: 1 }));

        // Test Read
        let entry = node.read("foo", 0, ReadMode::Linearizable).await.unwrap().unwrap();
        assert_eq!(entry.key, "foo");
        assert_eq!(entry.value, vec![b'b', b'a', b'r']);
        assert_eq!(entry.version, 1);

        // Test Cas success
        let cas_resp = node.propose(KvCommand::Cas { key: "foo".into(), expected: "bar".into(), new_value: "baz".into(), ttl_ns: None }, WriteMode::Majority).await.unwrap();
        assert!(matches!(cas_resp, KvResponse::CasResult { success: true, .. }));

        // Test Cas failure
        let cas_fail_resp = node.propose(KvCommand::Cas { key: "foo".into(), expected: "bar".into(), new_value: "qux".into(), ttl_ns: None }, WriteMode::Majority).await.unwrap();
        assert!(matches!(cas_fail_resp, KvResponse::CasResult { success: false, .. }));

        // Test Delete
        let del_resp = node.propose(KvCommand::Delete { key: "foo".into() }, WriteMode::Majority).await.unwrap();
        assert!(matches!(del_resp, KvResponse::Deleted { found: true }));

        // Test Read after Delete
        let read_after_del = node.read("foo", 0, ReadMode::Linearizable).await.unwrap();
        assert!(read_after_del.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stub_raft_node_parallel_propose() {
        let node = Arc::new(StubRaftNode::new());
        assert_eq!(node.shard_id(), 0);

        let (send, mut recv) = mpsc::unbounded_channel();
        for _n in 1..1001 {
            let send_clone = send.clone();
            let node_clone = node.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(rand::random::<u64>() % 100)).await;
                let resp = node_clone.propose(KvCommand::Put { key: "foo".into(), value: "bar".into(), ttl_ns: None, expect_version: 0 }, WriteMode::Majority).await.unwrap();
                let _ = send_clone.send(resp);
            });
        }

        drop(send);
        let mut versions = Vec::new();
        while let Some(answer) = recv.recv().await {
            if let KvResponse::Written { version } = answer {
                versions.push(version);
            }
        }
        versions.sort_unstable();
        assert_eq!(versions, (1..1001).collect::<Vec<u64>>());
    }
}
