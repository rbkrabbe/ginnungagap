use std::collections::HashMap;
use std::sync::RwLock;

use ggap_types::ShardId;

type TraceHeaders = (Option<String>, Option<String>, Option<String>);

/// In-memory store mapping `(shard_id, log_index)` to captured W3C trace headers.
///
/// Populated by `OpenRaftNode::propose()` on the leader immediately after
/// `client_write()` returns, consumed by `GgapStateMachine::apply()` and
/// `GgapNetwork::append_entries()` to re-anchor spans to the originating user trace.
///
/// `take` removes the entry, so growth is bounded by replication latency.
pub struct RequestMetadataStore {
    inner: RwLock<HashMap<(ShardId, u64), TraceHeaders>>,
}

impl RequestMetadataStore {
    pub fn new() -> Self {
        RequestMetadataStore {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Store trace headers for `(shard_id, log_index)`.
    /// No-ops when all three headers are `None`.
    pub fn store(
        &self,
        shard_id: ShardId,
        log_index: u64,
        traceparent: Option<String>,
        tracestate: Option<String>,
        baggage: Option<String>,
    ) {
        if traceparent.is_none() && tracestate.is_none() && baggage.is_none() {
            return;
        }
        if let Ok(mut g) = self.inner.write() {
            g.insert((shard_id, log_index), (traceparent, tracestate, baggage));
        }
    }

    /// Borrow the trace headers without removing them.
    pub fn lookup(&self, shard_id: ShardId, log_index: u64) -> Option<TraceHeaders> {
        self.inner.read().ok()?.get(&(shard_id, log_index)).cloned()
    }

    /// Remove and return the trace headers for `(shard_id, log_index)`.
    pub fn take(&self, shard_id: ShardId, log_index: u64) -> Option<TraceHeaders> {
        self.inner.write().ok()?.remove(&(shard_id, log_index))
    }
}

impl Default for RequestMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}
