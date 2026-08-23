use std::sync::Arc;

use ggap_types::{KvCommand, KvResponse};
use openraft::{Config, SnapshotPolicy};

/// The openraft `Node` for this cluster: everything membership carries about a
/// peer beyond its id — which is, deliberately, nothing.
///
/// Addresses live in `ShardRegistry`'s directory and are resolved per RPC, so
/// membership is a set of ids and a node can move without a membership change.
/// The type stays because openraft needs a `Node`, and it is where a future
/// consensus-only field — a placement zone, say — belongs. openraft's `Node` is
/// a blanket-impl marker trait over `Debug + Clone + Default + Eq + serde`, so
/// no `impl Node` is needed here and `ggap-types` keeps its openraft-free
/// dependency list.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GgapNode {}

openraft::declare_raft_types!(
    pub GgapTypeConfig:
        D            = KvCommand,
        R            = KvResponse,
        NodeId       = u64,
        Node         = GgapNode,
        Entry        = openraft::Entry<GgapTypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
        Responder    = openraft::impls::OneshotResponder<GgapTypeConfig>,
);

pub fn build_raft_config(
    heartbeat_ms: u64,
    election_min_ms: u64,
    election_max_ms: u64,
    snapshot_threshold: u64,
) -> Arc<Config> {
    let config = Config {
        heartbeat_interval: heartbeat_ms,
        election_timeout_min: election_min_ms,
        election_timeout_max: election_max_ms,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_threshold),
        max_in_snapshot_log_to_keep: 200,
        ..Config::default()
    };
    Arc::new(config.validate().expect("valid raft config"))
}
