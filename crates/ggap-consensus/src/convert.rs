use ggap_storage::types::{LogEntry, LogPayload, LogState, Vote};
use ggap_types::GgapError;
use openraft::{Entry, EntryPayload, LeaderId, RaftTypeConfig};

use crate::config::GgapTypeConfig;

pub(crate) fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, GgapError> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
        .map_err(|e| GgapError::Storage(e.to_string()))
}

pub(crate) fn decode<T: for<'de> serde::Deserialize<'de>>(b: &[u8]) -> Result<T, GgapError> {
    bincode::serde::decode_from_slice(b, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| GgapError::Storage(e.to_string()))
}

// ---------------------------------------------------------------------------
// LogId conversions
// ---------------------------------------------------------------------------

pub(crate) fn or_log_id_to_log_id(log_id: openraft::log_id::LogId<u64>) -> ggap_types::LogId {
    ggap_types::LogId {
        term: log_id.leader_id.term,
        leader_id: log_id.leader_id.node_id,
        index: log_id.index,
    }
}

pub(crate) fn log_id_to_or_log_id(log_id: ggap_types::LogId) -> openraft::log_id::LogId<u64> {
    openraft::log_id::LogId {
        leader_id: LeaderId::new(log_id.term, log_id.leader_id),
        index: log_id.index,
    }
}

// ---------------------------------------------------------------------------
// Entry <-> LogEntry conversions
// ---------------------------------------------------------------------------

/// Convert an openraft `Entry<GgapTypeConfig>` to a domain `LogEntry`.
pub(crate) fn or_entry_to_log_entry(
    entry: &<GgapTypeConfig as RaftTypeConfig>::Entry,
) -> Result<LogEntry, GgapError> {
    let payload = match &entry.payload {
        EntryPayload::Blank => LogPayload::Blank,
        EntryPayload::Normal(cmd) => LogPayload::Normal(cmd.clone()),
        EntryPayload::Membership(m) => LogPayload::Raw(encode(m)?),
    };
    Ok(LogEntry {
        index: entry.log_id.index,
        term: entry.log_id.leader_id.term,
        leader_id: entry.log_id.leader_id.node_id,
        payload,
    })
}

/// Convert a domain `LogEntry` back to an openraft `Entry<GgapTypeConfig>`.
pub(crate) fn log_entry_to_or_entry(
    entry: &LogEntry,
) -> Result<Entry<GgapTypeConfig>, GgapError> {
    let log_id = openraft::log_id::LogId {
        leader_id: LeaderId::new(entry.term, entry.leader_id),
        index: entry.index,
    };
    let payload = match &entry.payload {
        LogPayload::Blank => EntryPayload::Blank,
        LogPayload::Normal(cmd) => EntryPayload::Normal(cmd.clone()),
        LogPayload::Raw(bytes) => {
            let membership = decode(bytes)?;
            EntryPayload::Membership(membership)
        }
    };
    Ok(Entry { log_id, payload })
}

// ---------------------------------------------------------------------------
// Vote conversions
// ---------------------------------------------------------------------------

/// Convert an openraft `Vote<u64>` to a domain `Vote`.
pub(crate) fn or_vote_to_vote(vote: &openraft::Vote<u64>) -> Vote {
    Vote {
        term: vote.leader_id().term,
        voted_for: Some(vote.leader_id().node_id),
        committed: vote.is_committed(),
    }
}

/// Convert a domain `Vote` to an openraft `Vote<u64>`.
pub(crate) fn vote_to_or_vote(vote: &Vote) -> openraft::Vote<u64> {
    let node_id = vote.voted_for.unwrap_or(0);
    let leader_id = LeaderId::new(vote.term, node_id);
    if vote.committed {
        openraft::Vote::new_committed(leader_id.term, leader_id.node_id)
    } else {
        openraft::Vote::new(leader_id.term, leader_id.node_id)
    }
}

// ---------------------------------------------------------------------------
// LogState conversion
// ---------------------------------------------------------------------------

/// Convert a domain `LogState` to an openraft `LogState`.
pub(crate) fn log_state_to_or_log_state(
    state: &LogState,
) -> openraft::storage::LogState<GgapTypeConfig> {
    openraft::storage::LogState {
        last_purged_log_id: state.last_purged_log_id.map(log_id_to_or_log_id),
        last_log_id: state.last_log_id.map(log_id_to_or_log_id),
    }
}
