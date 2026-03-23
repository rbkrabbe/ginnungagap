use std::sync::Arc;
use std::time::Duration;

use ggap_types::{system_now_fn, KvCommand, NowFn, ShardId};
use tokio_util::sync::CancellationToken;

use crate::fjall::FjallStateMachine;
use crate::keys::ttl_shard_prefix;

/// GC interval used when no expiring key is found.
const GC_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Scans the `ttl_index` keyspace for keys that have expired and emits
/// `KvCommand::Delete` through `cmd_tx`.
///
/// **Phase 3 skeleton** — `cmd_tx` is wired to `openraft::Raft::client_write`
/// in Phase 4. Until then the task is not spawned.
pub struct TtlGcTask {
    store: Arc<FjallStateMachine>,
    shard_id: ShardId,
    /// Consumer wires this to the Raft client_write path in Phase 4.
    cmd_tx: tokio::sync::mpsc::Sender<KvCommand>,
    cancel: CancellationToken,
    now_fn: NowFn,
}

impl TtlGcTask {
    pub fn new(
        store: Arc<FjallStateMachine>,
        shard_id: ShardId,
        cmd_tx: tokio::sync::mpsc::Sender<KvCommand>,
        cancel: CancellationToken,
    ) -> Self {
        TtlGcTask {
            store,
            shard_id,
            cmd_tx,
            cancel,
            now_fn: system_now_fn(),
        }
    }

    pub fn with_clock(mut self, now_fn: NowFn) -> Self {
        self.now_fn = now_fn;
        self
    }

    /// Run the GC loop until the channel is closed or the task is cancelled.
    ///
    /// Algorithm:
    /// 1. Scan `ttl_index` from the shard prefix forward; take the first entry.
    /// 2. If none: sleep `GC_POLL_INTERVAL` and retry.
    /// 3. If `expires_at_ns <= now`: send `KvCommand::Delete` through Raft.
    ///    The state machine apply path removes the TTL index entry atomically.
    /// 4. Else: sleep until `expires_at_ns`, then send.
    pub async fn run(self) {
        let shard_id = self.shard_id;
        loop {
            if self.cancel.is_cancelled() {
                tracing::info!(shard_id, "TTL GC task shutting down");
                return;
            }
            let store = self.store.store.clone();
            let prefix = ttl_shard_prefix(shard_id);

            // Find the next entry to expire.
            let next = tokio::task::spawn_blocking(
                move || -> Result<Option<(i64, String, Vec<u8>)>, String> {
                    let mut iter = store.ttl_index.prefix(prefix);
                    match iter.next() {
                        None => Ok(None),
                        Some(guard) => match guard.into_inner() {
                            Err(e) => Err(e.to_string()),
                            Ok((k, _)) => {
                                // ttl_index key: shard(8) ++ expires_at_ns(8) ++ key_utf8
                                if k.len() < 16 {
                                    return Ok(None);
                                }
                                let expires_at_ns =
                                    i64::from_be_bytes(k[8..16].try_into().expect("16 byte key"));
                                let user_key = String::from_utf8_lossy(&k[16..]).to_string();
                                Ok(Some((expires_at_ns, user_key, k.to_vec())))
                            }
                        },
                    }
                },
            )
            .await;

            let next = match next {
                Ok(Ok(v)) => v,
                Ok(Err(_)) | Err(_) => {
                    tokio::time::sleep(GC_POLL_INTERVAL).await;
                    continue;
                }
            };

            match next {
                None => {
                    tokio::time::sleep(GC_POLL_INTERVAL).await;
                }
                Some((expires_at_ns, user_key, _raw_key)) => {
                    let now = (self.now_fn)();
                    if expires_at_ns > now {
                        let wait_ns = (expires_at_ns - now).max(0) as u64;
                        tokio::time::sleep(Duration::from_nanos(wait_ns)).await;
                    }

                    // Route the delete through Raft (Phase 4 wires this).
                    // The state machine apply path removes the TTL index entry
                    // atomically with the data deletion, so we do NOT remove it
                    // eagerly here. If Raft rejects the delete, the entry stays
                    // in the index and will be retried on the next GC cycle.
                    let cmd = KvCommand::Delete { key: user_key };
                    if self.cmd_tx.send(cmd).await.is_err() {
                        // Receiver dropped — shut down.
                        break;
                    }

                    // Small backoff to avoid tight re-fire loops when not leader.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}
