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

/// Stub header for Phase 2 — no real Raft state yet.
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
        GgapError::NotLeader { leader } => {
            let mut status = Status::unavailable(err.to_string());
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
    }
}
