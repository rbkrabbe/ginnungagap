use std::io::Cursor;
use std::sync::Arc;

use ggap_storage::fjall::FjallStateMachine;
use openraft::AnyError;
use ggap_storage::fjall::FjallStore;
use ggap_storage::keys::meta_key;
use ggap_storage::traits::StateMachineStore;
use ggap_types::{GgapError, KvResponse, ShardId};
use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, EntryPayload, ErrorSubject, ErrorVerb, LogId, RaftSnapshotBuilder, RaftTypeConfig,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};

use crate::config::GgapTypeConfig;
use crate::convert::{decode, encode};

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
    pub(crate) store: Arc<FjallStore>,
    pub(crate) shard_id: ShardId,
}

impl GgapStateMachine {
    pub fn new(fsm: Arc<FjallStateMachine>, store: Arc<FjallStore>, shard_id: ShardId) -> Self {
        GgapStateMachine { fsm, store, shard_id }
    }
}

impl RaftStateMachine<GgapTypeConfig> for GgapStateMachine {
    type SnapshotBuilder = GgapSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;

        tokio::task::spawn_blocking(move || {
            let last_applied: Option<LogId<u64>> = match store
                .meta
                .get(meta_key(shard_id, "or_last_applied"))
                .map_err(|e| sto_err(e.to_string()))?
            {
                None => None,
                Some(b) => decode::<Option<LogId<u64>>>(&b).map_err(|e| sto_err(e.to_string()))?,
            };

            let membership: StoredMembership<u64, BasicNode> = match store
                .meta
                .get(meta_key(shard_id, "or_membership"))
                .map_err(|e| sto_err(e.to_string()))?
            {
                None => StoredMembership::default(),
                Some(b) => decode::<StoredMembership<u64, BasicNode>>(&b)
                    .map_err(|e| sto_err(e.to_string()))?,
            };

            Ok((last_applied, membership))
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<KvResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = <GgapTypeConfig as RaftTypeConfig>::Entry>
            + openraft::OptionalSend,
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
                    self.write_last_applied_and_membership(&log_id, None).await?;
                    responses.push(KvResponse::NoOp);
                }
                EntryPayload::Normal(cmd) => {
                    let resp = match self.fsm.apply(shard_id, log_id.index, cmd.clone()).await {
                        Ok(r) => r,
                        Err(GgapError::VersionConflict { expected, actual }) => {
                            KvResponse::Conflict { expected, actual }
                        }
                        Err(e) => return Err(sto_err(e.to_string())),
                    };
                    // Update last_applied in the or_ namespace.
                    self.write_last_applied_and_membership(&log_id, None).await?;
                    responses.push(resp);
                }
                EntryPayload::Membership(m) => {
                    let sm = StoredMembership::new(Some(log_id), m.clone());
                    self.write_last_applied_and_membership(&log_id, Some(&sm)).await?;
                    responses.push(KvResponse::NoOp);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> GgapSnapshotBuilder {
        GgapSnapshotBuilder {
            fsm: self.fsm.clone(),
            store: self.store.clone(),
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

        // Decode our internal snapshot format.
        let our_snap: ggap_storage::types::Snapshot =
            decode(&bytes).map_err(|e| sto_err(e.to_string()))?;

        // Install into the FSM.
        self.fsm
            .install_snapshot(shard_id, our_snap)
            .await
            .map_err(|e| sto_err(e.to_string()))?;

        // Persist the openraft meta.
        let store = self.store.clone();
        let last_applied = meta.last_log_id;
        let membership = StoredMembership::new(
            *meta.last_membership.log_id(),
            meta.last_membership.membership().clone(),
        );
        let last_applied_bytes =
            encode(&last_applied).map_err(|e| sto_err(e.to_string()))?;
        let membership_bytes =
            encode(&membership).map_err(|e| sto_err(e.to_string()))?;

        tokio::task::spawn_blocking(move || {
            let mut batch = store.db.batch();
            batch.insert(&store.meta, meta_key(shard_id, "or_last_applied"), last_applied_bytes);
            batch.insert(&store.meta, meta_key(shard_id, "or_membership"), membership_bytes);
            batch.commit().map_err(|e| sto_err(e.to_string()))
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GgapTypeConfig>>, StorageError<u64>> {
        let shard_id = self.shard_id;
        let store = self.store.clone();

        // Build a fresh snapshot from the FSM.
        let our_snap = match self.fsm.build_snapshot(shard_id).await {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        // Retrieve the stored last_applied and membership for accurate metadata.
        let (last_applied, membership) = tokio::task::spawn_blocking(move || {
            let last_applied: Option<LogId<u64>> =
                match store.meta.get(meta_key(shard_id, "or_last_applied")).map_err(|e| sto_err(e.to_string()))? {
                    None => None,
                    Some(b) => decode::<Option<LogId<u64>>>(&b).map_err(|e| sto_err(e.to_string()))?,
                };
            let membership: StoredMembership<u64, BasicNode> =
                match store.meta.get(meta_key(shard_id, "or_membership")).map_err(|e| sto_err(e.to_string()))? {
                    None => StoredMembership::default(),
                    Some(b) => decode::<StoredMembership<u64, BasicNode>>(&b)
                        .map_err(|e| sto_err(e.to_string()))?,
                };
            Ok::<_, StorageError<u64>>((last_applied, membership))
        })
        .await
        .map_err(|e| sto_err(e.to_string()))??;

        if last_applied.is_none() {
            return Ok(None);
        }

        let data = encode(&our_snap).map_err(|e| sto_err(e.to_string()))?;
        let cursor = Cursor::new(data);

        let snap = Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership: membership,
                snapshot_id: our_snap.meta.snapshot_id,
            },
            snapshot: Box::new(cursor),
        };

        Ok(Some(snap))
    }
}

impl GgapStateMachine {
    async fn write_last_applied_and_membership(
        &self,
        log_id: &LogId<u64>,
        membership: Option<&StoredMembership<u64, BasicNode>>,
    ) -> Result<(), StorageError<u64>> {
        let store = self.store.clone();
        let shard_id = self.shard_id;
        let last_applied_bytes = encode(&Some(*log_id)).map_err(|e| sto_err(e.to_string()))?;
        let membership_bytes = membership
            .map(|m| encode(m).map_err(|e| sto_err(e.to_string())))
            .transpose()?;

        tokio::task::spawn_blocking(move || {
            let mut batch = store.db.batch();
            batch.insert(&store.meta, meta_key(shard_id, "or_last_applied"), last_applied_bytes);
            if let Some(mb) = membership_bytes {
                batch.insert(&store.meta, meta_key(shard_id, "or_membership"), mb);
            }
            batch.commit().map_err(|e| sto_err(e.to_string()))
        })
        .await
        .map_err(|e| sto_err(e.to_string()))?
    }
}

// ---------------------------------------------------------------------------
// GgapSnapshotBuilder
// ---------------------------------------------------------------------------

pub struct GgapSnapshotBuilder {
    fsm: Arc<FjallStateMachine>,
    store: Arc<FjallStore>,
    shard_id: ShardId,
}

impl RaftSnapshotBuilder<GgapTypeConfig> for GgapSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<GgapTypeConfig>, StorageError<u64>> {
        let shard_id = self.shard_id;
        let store = self.store.clone();

        let our_snap = self
            .fsm
            .build_snapshot(shard_id)
            .await
            .map_err(|e| sto_err(e.to_string()))?;

        let (last_applied, membership) = tokio::task::spawn_blocking(move || {
            let last_applied: Option<LogId<u64>> = match store
                .meta
                .get(meta_key(shard_id, "or_last_applied"))
                .map_err(|e| sto_err(e.to_string()))?
            {
                None => None,
                Some(b) => decode::<Option<LogId<u64>>>(&b).map_err(|e| sto_err(e.to_string()))?,
            };
            let membership: StoredMembership<u64, BasicNode> = match store
                .meta
                .get(meta_key(shard_id, "or_membership"))
                .map_err(|e| sto_err(e.to_string()))?
            {
                None => StoredMembership::default(),
                Some(b) => decode::<StoredMembership<u64, BasicNode>>(&b)
                    .map_err(|e| sto_err(e.to_string()))?,
            };
            Ok::<_, StorageError<u64>>((last_applied, membership))
        })
        .await
        .map_err(|e| sto_err(e.to_string()))??;

        let snapshot_id = our_snap.meta.snapshot_id.clone();
        let data = encode(&our_snap).map_err(|e| sto_err(e.to_string()))?;
        let cursor = Cursor::new(data);

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership: membership,
                snapshot_id,
            },
            snapshot: Box::new(cursor),
        })
    }
}
