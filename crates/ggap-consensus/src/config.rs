use std::sync::Arc;

use ggap_types::{KvCommand, KvResponse};
use openraft::{BasicNode, Config};

openraft::declare_raft_types!(
    pub GgapTypeConfig:
        D            = KvCommand,
        R            = KvResponse,
        NodeId       = u64,
        Node         = BasicNode,
        Entry        = openraft::Entry<GgapTypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
        Responder    = openraft::impls::OneshotResponder<GgapTypeConfig>,
);

pub fn build_raft_config(
    heartbeat_ms: u64,
    election_min_ms: u64,
    election_max_ms: u64,
) -> Arc<Config> {
    let config = Config {
        heartbeat_interval: heartbeat_ms,
        election_timeout_min: election_min_ms,
        election_timeout_max: election_max_ms,
        ..Config::default()
    };
    Arc::new(config.validate().expect("valid raft config"))
}
