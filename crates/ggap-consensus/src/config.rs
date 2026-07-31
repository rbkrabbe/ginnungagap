use std::sync::Arc;

use ggap_types::{KvCommand, KvResponse, NodeAddrs};
use openraft::{Config, SnapshotPolicy};

/// The openraft `Node` for this cluster: everything membership carries about a
/// peer beyond its id.
///
/// Wraps the `ggap-types` domain type rather than being it, so a future
/// consensus-only field — a placement zone, say — has a home that is not the
/// shared domain crate. openraft's `Node` is a blanket-impl marker trait over
/// `Debug + Clone + Default + Eq + serde`, so no `impl Node` is needed here and
/// `ggap-types` keeps its openraft-free dependency list.
///
/// Membership does not yet populate `client_addr`; every construction site
/// leaves it empty until tk-fd58 and tk-10b7.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GgapNode {
    pub addrs: NodeAddrs,
}

impl GgapNode {
    /// Only the cluster address is known — the shape of every membership entry
    /// until a client address reaches consensus state.
    pub fn cluster_only(cluster_addr: impl Into<String>) -> Self {
        GgapNode {
            addrs: NodeAddrs::cluster_only(cluster_addr),
        }
    }

    /// Cluster gRPC endpoint. This is what `RaftNetwork` dials.
    pub fn cluster_addr(&self) -> &str {
        &self.addrs.cluster_addr
    }

    /// Client-facing gRPC endpoint. Empty means "not known here".
    pub fn client_addr(&self) -> &str {
        &self.addrs.client_addr
    }
}

impl From<NodeAddrs> for GgapNode {
    fn from(addrs: NodeAddrs) -> Self {
        GgapNode { addrs }
    }
}

/// Prints the cluster address alone, matching what openraft's `BasicNode` used
/// to print, so membership logs and error text stay greppable.
impl std::fmt::Display for GgapNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addrs.cluster_addr)
    }
}

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
