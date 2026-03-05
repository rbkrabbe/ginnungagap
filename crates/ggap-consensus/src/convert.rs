use ggap_types::GgapError;
use openraft::LeaderId;

pub(crate) fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, GgapError> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
        .map_err(|e| GgapError::Storage(e.to_string()))
}

pub(crate) fn decode<T: for<'de> serde::Deserialize<'de>>(b: &[u8]) -> Result<T, GgapError> {
    bincode::serde::decode_from_slice(b, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| GgapError::Storage(e.to_string()))
}

pub(crate) fn or_log_id_to_log_id(log_id: openraft::log_id::LogId<u64>) -> ggap_types::LogId {
    ggap_types::LogId { term: log_id.leader_id.term, leader_id: log_id.leader_id.node_id, index: log_id.index }
}

pub(crate) fn log_id_to_or_log_id(log_id: ggap_types::LogId) -> openraft::log_id::LogId<u64> {
    openraft::log_id::LogId { leader_id: LeaderId::new(log_id.term, log_id.leader_id), index: log_id.index }
}