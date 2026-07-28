use ggap_proto::v1::{KeyValue, ReadConsistency, ResponseHeader, WriteQuorum};
use ggap_types::{GgapError, KvEntry, ReadMode, WriteMode};
use tonic::metadata::MetadataValue;
use tonic::Status;

pub fn kv_entry_to_proto(entry: KvEntry) -> KeyValue {
    KeyValue {
        key: entry.key,
        value: entry.value,
        version: entry.version,
        created_at_ns: entry.created_at_ns,
        modified_at_ns: entry.modified_at_ns,
        expires_at_ns: entry.expires_at_ns.unwrap_or(0),
    }
}

pub fn proto_read_consistency(raw: i32) -> ReadMode {
    match ReadConsistency::try_from(raw).unwrap_or(ReadConsistency::Linearizable) {
        ReadConsistency::Linearizable => ReadMode::Linearizable,
        ReadConsistency::Sequential => ReadMode::Sequential,
        ReadConsistency::Eventual => ReadMode::Eventual,
    }
}

pub fn proto_write_quorum(raw: i32) -> WriteMode {
    match WriteQuorum::try_from(raw).unwrap_or(WriteQuorum::Majority) {
        WriteQuorum::Majority => WriteMode::Majority,
        WriteQuorum::All => WriteMode::All,
    }
}

/// Response header with `cluster_id`/`raft_index`/`raft_term` left at `0`; these
/// are not yet populated from real Raft state.
pub fn stub_header(node_id: u64) -> ResponseHeader {
    ResponseHeader {
        cluster_id: 0,
        node_id,
        raft_index: 0,
        raft_term: 0,
    }
}

pub fn ggap_to_status(err: GgapError) -> Status {
    match &err {
        GgapError::NotFound => Status::not_found(err.to_string()),
        GgapError::NotLeader { leader_id, leader } => {
            let mut status = Status::unavailable(err.to_string());
            if let Some(id) = leader_id {
                if let Ok(val) = MetadataValue::try_from(id.to_string().as_str()) {
                    status.metadata_mut().insert("ggap-leader-id", val);
                }
            }
            if let Some(addr) = leader {
                if let Ok(val) = MetadataValue::try_from(addr.as_str()) {
                    status.metadata_mut().insert("ggap-leader-addr", val);
                }
            }
            status
        }
        GgapError::VersionConflict { .. } => Status::aborted(err.to_string()),
        GgapError::Timeout => Status::deadline_exceeded(err.to_string()),
        GgapError::Storage(_) | GgapError::Consensus(_) => Status::internal(err.to_string()),
        GgapError::InvalidArgument(_) => Status::invalid_argument(err.to_string()),
        GgapError::WrongShard { shard_id, .. } => {
            let mut status = Status::failed_precondition(err.to_string());
            if let Ok(val) = MetadataValue::try_from(shard_id.to_string().as_str()) {
                status.metadata_mut().insert("ggap-shard-id", val);
            }
            status
        }
        GgapError::ShardSplitting => Status::unavailable(err.to_string()),
        GgapError::ShardNotFound(_) => Status::not_found(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(status: &Status, key: &str) -> Option<String> {
        status
            .metadata()
            .get(key)
            .map(|v| v.to_str().unwrap().to_string())
    }

    #[test]
    fn not_leader_emits_both_id_and_addr() {
        let status = ggap_to_status(GgapError::NotLeader {
            leader_id: Some(7),
            leader: Some("10.0.0.4:17001".to_string()),
        });

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(meta(&status, "ggap-leader-id").as_deref(), Some("7"));
        assert_eq!(
            meta(&status, "ggap-leader-addr").as_deref(),
            Some("10.0.0.4:17001")
        );
    }

    /// Each half of the hint is emitted independently: openraft can know who the
    /// leader is without knowing an address for it, and a forwarder that
    /// resolves ids through gossip can still act on the id alone.
    #[test]
    fn not_leader_emits_id_without_addr() {
        let status = ggap_to_status(GgapError::NotLeader {
            leader_id: Some(7),
            leader: None,
        });

        assert_eq!(meta(&status, "ggap-leader-id").as_deref(), Some("7"));
        assert!(meta(&status, "ggap-leader-addr").is_none());
    }

    #[test]
    fn not_leader_with_no_hint_emits_no_metadata() {
        let status = ggap_to_status(GgapError::NotLeader {
            leader_id: None,
            leader: None,
        });

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(meta(&status, "ggap-leader-id").is_none());
        assert!(meta(&status, "ggap-leader-addr").is_none());
    }
}
