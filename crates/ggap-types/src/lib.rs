use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub type NodeId = u64;
pub type ShardId = u64;

/// Injectable clock for wall-clock timestamps (nanoseconds since Unix epoch).
/// Use the default `system_now_fn()` in production. In deterministic simulation
/// tests, inject a mock that returns controlled time.
pub type NowFn = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Returns a `NowFn` backed by `SystemTime::now()`.
pub fn system_now_fn() -> NowFn {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64
    })
}

/// Key range owned by a shard: [start, end). Empty end means unbounded.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct KeyRange {
    /// Inclusive lower bound. Empty string means the minimum possible key.
    pub start: String,
    /// Exclusive upper bound. Empty string means unbounded (no upper limit).
    pub end: String,
}

impl KeyRange {
    /// Returns `true` if `key` falls within this range.
    pub fn contains(&self, key: &str) -> bool {
        let after_start = self.start.is_empty() || key >= self.start.as_str();
        let before_end = self.end.is_empty() || key < self.end.as_str();
        after_start && before_end
    }
}

/// State of a shard in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShardState {
    Active,
    Splitting,
}

/// Metadata for a single shard.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub range: KeyRange,
    pub state: ShardState,
}

/// The gRPC addresses at which a node can be reached.
///
/// Lives here rather than in `ggap-consensus` because both the directory and
/// `KvCommand::Split` need the same shape, and this crate depends on neither
/// openraft nor gRPC. Whoever holds these decides how they are reconciled; the
/// type itself carries no merge policy.
///
/// An empty field means the node advertises no such address — nothing can reach
/// it there. `ShardRegistry` treats that as a fact to propagate, not a gap to
/// fill from another source.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NodeAddrs {
    /// Cluster gRPC endpoint, shared by RaftService, AdminService and
    /// GossipService.
    pub cluster_addr: String,
    /// Client-facing gRPC endpoint (the KV API listener).
    pub client_addr: String,
}

impl NodeAddrs {
    /// Both addresses known.
    pub fn new(cluster_addr: impl Into<String>, client_addr: impl Into<String>) -> Self {
        NodeAddrs {
            cluster_addr: cluster_addr.into(),
            client_addr: client_addr.into(),
        }
    }

    /// A node that advertises no client address, so nothing can forward a
    /// client request to it. Production paths always supply both; this is for
    /// harnesses that serve no client API.
    pub fn cluster_only(cluster_addr: impl Into<String>) -> Self {
        NodeAddrs {
            cluster_addr: cluster_addr.into(),
            client_addr: String::new(),
        }
    }
}

/// Stored in last_applied metadata
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogId {
    pub term: u64,
    pub leader_id: u64,
    pub index: u64,
}

/// Stored per key (current value and history entries)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KvEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub version: u64,
    pub created_at_ns: i64,
    pub modified_at_ns: i64,
    pub expires_at_ns: Option<i64>,
}

/// Commands proposed through Raft (implements openraft::AppData in ggap-consensus)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum KvCommand {
    Put {
        key: String,
        value: Vec<u8>,
        ttl_ns: Option<i64>,
        expect_version: u64,
    },
    Delete {
        key: String,
    },
    Cas {
        key: String,
        expected: Vec<u8>,
        new_value: Vec<u8>,
        ttl_ns: Option<i64>,
    },
    /// Proposed by the split coordinator through the source shard's Raft log.
    /// Every node applies this deterministically: copy keys >= split_key to
    /// new_shard_id, delete them from source, update ShardMap ranges, and
    /// persist bootstrap membership for the new shard so it can be restarted
    /// with the correct Raft group.
    Split {
        split_key: String,
        new_shard_id: ShardId,
        source_range: KeyRange,
        /// Raft membership for the new shard: node_id → both addresses.
        /// Stored atomically alongside the data movement so that on restart
        /// main.rs can initialise the new shard with the correct peers.
        /// Carries `NodeAddrs` rather than the `GgapNode` that Raft membership
        /// carries, so this crate stays free of any openraft dependency.
        source_members: std::collections::BTreeMap<u64, NodeAddrs>,
    },
}

/// Responses returned from state machine apply
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum KvResponse {
    Written {
        version: u64,
    },
    Deleted {
        found: bool,
    },
    CasResult {
        success: bool,
        current: Option<KvEntry>,
    },
    /// Conditional Put whose expect_version didn't match. Returned to the
    /// client as Status::aborted; never a fatal storage error.
    Conflict {
        expected: u64,
        actual: u64,
    },
    /// Returned for Raft-internal entries (Blank, Membership).
    /// Never sent to clients; guarded by unreachable!() in ggap-server.
    NoOp,
    /// Returned to the split coordinator after a KvCommand::Split is applied.
    /// Never routed through the KV service path.
    SplitComplete {
        new_shard_id: ShardId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    Linearizable,
    Sequential,
    Eventual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Majority,
    All,
}

// ---------------------------------------------------------------------------
// Watch domain types
// ---------------------------------------------------------------------------

/// The kind of mutation that triggered a watch event.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WatchEventKind {
    Put,
    Delete,
    Expire,
}

/// A domain-level watch event, free of any gRPC/proto types.
///
/// Placed in `ggap-types` so `ggap-storage` can broadcast events without
/// creating an upward dependency on `ggap-proto`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DomainWatchEvent {
    pub kind: WatchEventKind,
    pub shard_id: ShardId,
    pub key: String,
    /// `None` for `Delete` and `Expire`; `Some` for `Put`.
    pub entry: Option<KvEntry>,
    pub version: u64,
    pub raft_index: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(thiserror::Error, Debug)]
pub enum GgapError {
    #[error("key not found")]
    NotFound,
    /// The receiving node is not the leader for this shard.
    ///
    /// `leader_id` is the leader's stable node id and `leader` the address that
    /// was current when the error was constructed. A forwarder should resolve
    /// `leader_id` through the membership-derived directory and treat `leader`
    /// only as a
    /// fallback, since the address can be stale by the time it is read.
    #[error("not the leader; hint: {leader_id:?} at {leader:?}")]
    NotLeader {
        leader_id: Option<u64>,
        leader: Option<String>,
    },
    #[error("version conflict: expected {expected}, got {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("operation timed out")]
    Timeout,
    #[error("storage error: {0}")]
    Storage(String),
    #[error("consensus error: {0}")]
    Consensus(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("wrong shard for key; try shard {shard_id}")]
    WrongShard { shard_id: ShardId, range: KeyRange },
    #[error("shard is splitting, retry later")]
    ShardSplitting,
    #[error("shard not found: {0}")]
    ShardNotFound(ShardId),
}

// ---------------------------------------------------------------------------
// Scan continuation token
// ---------------------------------------------------------------------------

/// Opaque cursor returned in `ScanResponse.next_page_token`. Clients must
/// pass it back unmodified; the structure is not stable across server
/// versions. Cross-shard scans hop server-side and encode the next key
/// (plus a shard hint) here.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScanContinuation {
    /// Inclusive start key for the next page.
    pub next_key: String,
    /// Shard the cursor was last positioned in. Informational; the router
    /// resolves the actual owning shard from `next_key` on each request, so
    /// this remains correct even if a split changes the boundary.
    pub shard_id_hint: ShardId,
}

impl ScanContinuation {
    pub fn encode(&self) -> Result<Vec<u8>, GgapError> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| GgapError::InvalidArgument(format!("encode page_token: {e}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, GgapError> {
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map(|(v, _)| v)
            .map_err(|_| GgapError::InvalidArgument("invalid page_token".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_continuation_roundtrip() {
        let c = ScanContinuation {
            next_key: "hello".into(),
            shard_id_hint: 7,
        };
        let bytes = c.encode().unwrap();
        let back = ScanContinuation::decode(&bytes).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn scan_continuation_roundtrip_empty_key() {
        let c = ScanContinuation {
            next_key: String::new(),
            shard_id_hint: 0,
        };
        let bytes = c.encode().unwrap();
        assert_eq!(ScanContinuation::decode(&bytes).unwrap(), c);
    }

    #[test]
    fn scan_continuation_decode_rejects_garbage() {
        // A short prefix that's nowhere near a valid bincode-serialized
        // ScanContinuation should be rejected as InvalidArgument.
        let err = ScanContinuation::decode(b"\xff\xff\xff").unwrap_err();
        assert!(matches!(err, GgapError::InvalidArgument(_)));
    }
}
