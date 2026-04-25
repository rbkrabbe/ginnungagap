use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::node::GgapRaft;
use ggap_types::ShardId;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct RaftMetricsTask {
    raft: Arc<GgapRaft>,
    shard_id: ShardId,
    cancel: CancellationToken,
    poll_interval: Duration,
}

impl RaftMetricsTask {
    pub fn new(raft: Arc<GgapRaft>, shard_id: ShardId, cancel: CancellationToken) -> Self {
        Self {
            raft,
            shard_id,
            cancel,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    pub fn with_poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    pub async fn run(self) {
        let shard = self.shard_id;
        let shard_str: Arc<str> = shard.to_string().into();
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    tracing::info!(shard_id = shard, "raft metrics task shutting down");
                    return;
                }
                _ = tokio::time::sleep(self.poll_interval) => {}
            }
            let m = self.raft.metrics().borrow().clone();
            let last_applied = m.last_applied.map(|l| l.index).unwrap_or(0);
            let last_log = m.last_log_index.unwrap_or(0);

            metrics::gauge!("raft_current_term", "shard_id" => shard_str.clone())
                .set(m.current_term as f64);
            metrics::gauge!("raft_last_applied_index", "shard_id" => shard_str.clone())
                .set(last_applied as f64);
            metrics::gauge!("raft_last_log_index", "shard_id" => shard_str.clone())
                .set(last_log as f64);
            metrics::gauge!("raft_commit_lag", "shard_id" => shard_str.clone())
                .set(last_log.saturating_sub(last_applied) as f64);
            metrics::gauge!("raft_is_leader", "shard_id" => shard_str.clone()).set(
                if m.state == openraft::ServerState::Leader {
                    1.0
                } else {
                    0.0
                },
            );

            // Per-peer match index — only available on the leader.
            if let Some(replication) = &m.replication {
                for (peer_id, match_log) in replication {
                    let peer_str: Arc<str> = peer_id.to_string().into();
                    let idx = match_log.map(|l| l.index).unwrap_or(0);
                    metrics::gauge!(
                        "raft_match_index",
                        "shard_id" => shard_str.clone(),
                        "peer_node_id" => peer_str,
                    )
                    .set(idx as f64);
                }
            }
        }
    }
}
