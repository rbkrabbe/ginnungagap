use std::io::Cursor;
use std::sync::Arc;

use ggap_storage::fjall::FjallStateMachine;
use ggap_storage::traits::StateMachineStore;
use ggap_types::{GgapError, KvResponse, ShardId};
use openraft::storage::RaftStateMachine;
use openraft::AnyError;
use openraft::{
    BasicNode, EntryPayload, ErrorSubject, ErrorVerb, LogId, RaftSnapshotBuilder, RaftTypeConfig,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};

use crate::config::GgapTypeConfig;
use crate::convert::{self, decode, encode, log_id_to_or_log_id};

fn sto_err(msg: impl Into<String>) -> StorageError<u64> {
    let io = StorageIOError::new(
        ErrorSubject::Store,
        ErrorVerb::Write,
        AnyError::error(msg.into()),
    );
    StorageError::IO { source: io }
}

// ---------------------------------------------------------------------------
// GgapStateMachine
// ---------------------------------------------------------------------------

pub struct GgapStateMachine {
    pub(crate) fsm: Arc<FjallStateMachine>,
    pub(crate) shard_id: ShardId,
}

impl GgapStateMachine {
    pub fn new(fsm: Arc<FjallStateMachine>, shard_id: ShardId) -> Self {
        GgapStateMachine { fsm, shard_id }
    }
}

impl RaftStateMachine<GgapTypeConfig> for GgapStateMachine {
    type SnapshotBuilder = GgapSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let shard_id = self.shard_id;

        let last_applied = self
            .fsm
            .last_applied(shard_id)
            .await
            .map_err(|e| sto_err(e.to_string()))?;
        let log_id = last_applied.0.map(log_id_to_or_log_id);
        let membership = match last_applied.1 {
            Some(bytes) => decode::<StoredMembership<u64, BasicNode>>(&bytes)
                .map_err(|e| sto_err(e.to_string()))?,
            None => StoredMembership::default(),
        };

        Ok((log_id, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<KvResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = <GgapTypeConfig as RaftTypeConfig>::Entry> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let shard_id = self.shard_id;
        let entries: Vec<_> = entries.into_iter().collect();
        let mut responses = Vec::with_capacity(entries.len());

        for entry in &entries {
            let log_id = entry.log_id;
            match &entry.payload {
                EntryPayload::Blank => {
                    // Update last_applied, no data change.
                    let resp = match self
                        .fsm
                        .apply(shard_id, convert::or_log_id_to_log_id(log_id), None, None)
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => return Err(sto_err(e.to_string())),
                    };
                    responses.push(resp);
                }
                EntryPayload::Normal(cmd) => {
                    let converted_log_id = convert::or_log_id_to_log_id(log_id);
                    let resp = match self
                        .fsm
                        .apply(shard_id, converted_log_id, cmd.clone().into(), None)
                        .await
                    {
                        Ok(r) => r,
                        Err(GgapError::VersionConflict { expected, actual }) => {
                            KvResponse::Conflict { expected, actual }
                        }
                        Err(e) => return Err(sto_err(e.to_string())),
                    };
                    responses.push(resp);
                }
                EntryPayload::Membership(m) => {
                    let sm = StoredMembership::new(Some(log_id), m.clone());
                    let resp = match self
                        .fsm
                        .apply(
                            shard_id,
                            convert::or_log_id_to_log_id(log_id),
                            None,
                            Some(encode(&sm).map_err(|e| sto_err(e.to_string()))?),
                        )
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => return Err(sto_err(e.to_string())),
                    };
                    responses.push(resp);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> GgapSnapshotBuilder {
        GgapSnapshotBuilder {
            fsm: self.fsm.clone(),
            shard_id: self.shard_id,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let shard_id = self.shard_id;
        let bytes = snapshot.into_inner();

        let last_log_id = meta.last_log_id.map(convert::or_log_id_to_log_id);

        let our_snap = ggap_storage::types::Snapshot {
            meta: ggap_storage::types::SnapshotMeta {
                last_log_id,
                membership_bytes: encode(&meta.last_membership)
                    .map_err(|e| sto_err(e.to_string()))?,
                snapshot_id: meta.snapshot_id.clone(),
            },
            data: bytes,
        };

        // Install into the FSM.
        self.fsm
            .install_snapshot(shard_id, our_snap)
            .await
            .map_err(|e| sto_err(e.to_string()))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GgapTypeConfig>>, StorageError<u64>> {
        let shard_id = self.shard_id;

        // Build a fresh snapshot from the FSM.
        let our_snap = match self.fsm.build_snapshot(shard_id).await {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        if our_snap.meta.last_log_id.is_none() {
            return Ok(None);
        }

        let data = encode(&our_snap).map_err(|e| sto_err(e.to_string()))?;
        let cursor = Cursor::new(data);

        let membership = match our_snap.meta.membership_bytes.is_empty() {
            false => decode::<StoredMembership<u64, BasicNode>>(&our_snap.meta.membership_bytes)
                .map_err(|e| sto_err(e.to_string()))?,
            true => StoredMembership::default(),
        };

        let snap = Snapshot {
            meta: SnapshotMeta {
                last_log_id: our_snap.meta.last_log_id.map(convert::log_id_to_or_log_id),
                last_membership: membership,
                snapshot_id: our_snap.meta.snapshot_id,
            },
            snapshot: Box::new(cursor),
        };

        Ok(Some(snap))
    }
}

// ---------------------------------------------------------------------------
// GgapSnapshotBuilder
// ---------------------------------------------------------------------------

pub struct GgapSnapshotBuilder {
    fsm: Arc<FjallStateMachine>,
    shard_id: ShardId,
}

impl RaftSnapshotBuilder<GgapTypeConfig> for GgapSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<GgapTypeConfig>, StorageError<u64>> {
        let shard_id = self.shard_id;

        let our_snap = self
            .fsm
            .build_snapshot(shard_id)
            .await
            .map_err(|e| sto_err(e.to_string()))?;

        let snapshot_id = our_snap.meta.snapshot_id.clone();
        let data = encode(&our_snap).map_err(|e| sto_err(e.to_string()))?;
        let cursor = Cursor::new(data);

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: our_snap.meta.last_log_id.map(convert::log_id_to_or_log_id),
                last_membership: decode::<StoredMembership<u64, BasicNode>>(
                    &our_snap.meta.membership_bytes,
                )
                .map_err(|e| sto_err(e.to_string()))?,
                snapshot_id,
            },
            snapshot: Box::new(cursor),
        })
    }
}
