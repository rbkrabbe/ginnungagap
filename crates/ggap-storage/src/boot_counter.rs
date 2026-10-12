//! The boot counter: the incarnation this node publishes its own descriptor at.
//!
//! A descriptor is authored by the node it describes, so its incarnation only
//! has to be a clock over one writer's publications. A counter in the data dir,
//! incremented once per start, is exactly that: a node that restarts at a new
//! address publishes above every copy of itself still in flight, so the move is
//! resolved by rank rather than by arrival order.
//!
//! # Starting low is worse than not starting
//!
//! A node that publishes below the rank its peers already hold for it does not
//! merely lose an ordering guarantee — it loses authorship of its own entry.
//! Peers' copies win on rank, the local self-publication loses the same
//! comparison every gossip tick, and because the gossip round re-snapshots the
//! directory per peer, the node forwards the stale address *about itself*. It
//! cannot recover, and it looks healthy while doing it.
//!
//! That is why [`BootCounter::advance`] would rather fail the boot than return a
//! rank it cannot stand behind. There are only three cases:
//!
//! - **Absent counter** — a first boot. Publish at 1.
//! - **Readable counter** — one above it, written back durably.
//! - **Anything else** — the boot fails.
//!
//! A counter that is present and unreadable is not recovered from. It could
//! be: the persisted directory holds this node's own last-published
//! descriptor, and its rank could be read back from there. That was the
//! behaviour, and it was removed, because the directory is written by the
//! gossip task and therefore lags — a node whose counter rotted before that
//! task had persisted this boot's entry would recover a rank it had already
//! published at and reissue it. A fallback that is usually right and
//! occasionally wrong forever is worse than no fallback, and a corrupt key is
//! not a condition to recover from in the first place: it means something is
//! wrong that recovering would hide.
//!
//! A rank that cannot be *written* is refused for the same reason. Returning
//! it would leave the next boot to read the stale counter and issue the same
//! number again — and the directory keeps the entry it already holds when two
//! ranks are equal, so the second of those boots could not install an address
//! it had changed. The write is also flushed to the disk
//! ([`crate::FjallStore::sync_to_disk`]), not merely to the OS: an
//! acknowledged write lost to power loss would leave the next boot reading a
//! stale counter, which is the same defect by a different route.
//!
//! **Together those make a rank unique to the boot that issued it, with no
//! residual window** — which is the property the directory's ordering rests
//! on: two *ranked* copies at one incarnation describe one published state and
//! can be used interchangeably. The exception is incarnation 0, which this
//! counter never issues — it marks a descriptor
//! written on a node's behalf, and those are ordered by arrival rather than by
//! rank.
//!
//! **Wiping the data dir loses the count**, and no recovery covers it: the
//! node restarts at 1 and cannot outbid peers still holding a higher
//! incarnation for that id. A wipe combined with an address change needs a
//! fresh node id — and so does a corrupt counter, since wiping is the only way
//! to clear one and that puts the node back at 1.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ggap_types::GgapError;

use crate::fjall::FjallStore;
use crate::keys::node_key;

fn counter_key() -> Vec<u8> {
    node_key("boot_counter")
}

/// Reads, increments and writes the boot counter in the `node` keyspace.
pub struct BootCounter {
    store: Arc<FjallStore>,
    self_node_id: u64,
    /// Fault injection: when `true`, [`Self::write`] fails before the insert,
    /// as though the keyspace rejected it. Always present, armable only via
    /// [`Self::arm_write_failure`] behind `#[cfg(any(test, feature =
    /// "test-utils"))]`.
    fail_write: AtomicBool,
    /// Fault injection for the other shape: the insert lands and the flush
    /// fails. Distinct from `fail_write` because the states differ — the
    /// counter holds the new value, so the next boot takes one above *it* and
    /// skips a rank rather than reissuing one. Armable only via
    /// [`Self::arm_flush_failure`].
    fail_flush: AtomicBool,
}

impl BootCounter {
    /// `self_node_id` names the node whose rank this counter tracks. It is
    /// carried for the error message on an unreadable counter, which has to
    /// tell an operator which host needs a fresh id.
    pub fn new(store: Arc<FjallStore>, self_node_id: u64) -> Self {
        BootCounter {
            store,
            self_node_id,
            fail_write: AtomicBool::new(false),
            fail_flush: AtomicBool::new(false),
        }
    }

    /// Make the next counter write fail before it inserts. Test builds and
    /// `test-utils` only.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn arm_write_failure(&self) {
        self.fail_write.store(true, Ordering::SeqCst);
    }

    /// Make the next counter write insert and then fail to flush — the shape
    /// a full disk or a failing device produces once the record is already in
    /// the keyspace. Test builds and `test-utils` only.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn arm_flush_failure(&self) {
        self.fail_flush.store(true, Ordering::SeqCst);
    }

    /// The incarnation for this boot: one above the last rank this node can
    /// establish for itself, persisted before it is returned.
    ///
    /// Fails when the counter is there and unreadable, so the rank cannot be
    /// established, and when the rank cannot be written durably. Nothing is
    /// consulted but the counter. See the module docs for why each is a
    /// refusal to start rather than a warning.
    pub fn advance(&self) -> Result<u64, GgapError> {
        let incarnation = self.previous()?.saturating_add(1);
        // A rank this boot cannot persist is a rank the *next* boot will issue
        // again, having read the stale value. Two boots at one rank is the one
        // way a tie can carry conflicting addresses, and the directory settles
        // ties in favour of the entry already held — so the second of those
        // boots could never install an address it changed. Refuse here, where
        // the cause is still visible, rather than at a move that silently
        // does not take.
        self.write(incarnation).map_err(|e| {
            GgapError::Storage(format!(
                "cannot persist the boot counter at incarnation {incarnation}: {e}. \
                 A rank this node cannot record is one the next boot would reissue, \
                 and an address changed across those two boots would never take."
            ))
        })?;
        Ok(incarnation)
    }

    /// The last incarnation this node published, or 0 for a first boot.
    ///
    /// A counter that is there and unreadable is not recovered from — see the
    /// module docs for what was tried and why it went.
    fn previous(&self) -> Result<u64, GgapError> {
        match self.read_counter() {
            Ok(Some(incarnation)) => Ok(incarnation),
            Ok(None) => Ok(0),
            Err(e) => Err(GgapError::Storage(format!(
                "the boot counter for node {} is present and unreadable ({e}), so the \
                 rank this node last published cannot be established. Starting at 1 \
                 would publish below the rank its peers hold and lose authorship of \
                 this node's own address. Clearing the data dir puts it back at 1 for \
                 the same reason, so give this host a fresh --node-id.",
                self.self_node_id
            ))),
        }
    }

    /// `Ok(None)` for a counter that was never written, `Err` for one that is
    /// there and is not a `u64`.
    fn read_counter(&self) -> Result<Option<u64>, GgapError> {
        let bytes = match self.store.node.get(counter_key()) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(e) => return Err(GgapError::Storage(e.to_string())),
        };
        match <[u8; 8]>::try_from(&bytes[..]) {
            Ok(be) => Ok(Some(u64::from_be_bytes(be))),
            Err(_) => Err(GgapError::Storage(format!(
                "boot counter is {} bytes, not 8",
                bytes.len()
            ))),
        }
    }

    /// Write the rank and flush it to the disk. An insert alone is durable
    /// against this process dying and no further; acknowledged and then lost
    /// to power loss leaves the next boot reading a stale counter, which is
    /// the defect [`Self::advance`] refuses for.
    fn write(&self, incarnation: u64) -> Result<(), GgapError> {
        if self.fail_write.load(Ordering::SeqCst) {
            return Err(GgapError::Storage("injected write failure".into()));
        }
        self.store
            .node
            .insert(counter_key(), incarnation.to_be_bytes().to_vec())
            .map_err(|e| GgapError::Storage(e.to_string()))?;
        if self.fail_flush.load(Ordering::SeqCst) {
            return Err(GgapError::Storage("injected flush failure".into()));
        }
        self.store.sync_to_disk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    const SELF: u64 = 1;

    fn store() -> (Arc<FjallStore>, TempDir) {
        let tempdir = TempDir::new().unwrap();
        let store = FjallStore::open(tempdir.path()).unwrap();
        (store, tempdir)
    }

    fn counter(store: Arc<FjallStore>) -> BootCounter {
        BootCounter::new(store, SELF)
    }

    fn corrupt_the_counter(store: &Arc<FjallStore>) {
        store.node.insert(counter_key(), b"seven".to_vec()).unwrap();
    }

    /// The rank is refused rather than returned unpersisted. Returning it
    /// would leave the next boot to reissue the same number, and a directory
    /// tie is settled in favour of the entry already held — so an address
    /// changed across those two boots would never install anywhere.
    #[test]
    fn a_rank_that_cannot_be_persisted_fails_the_boot() {
        let (store, _tempdir) = store();
        let counter = counter(store.clone());
        counter.arm_write_failure();

        let err = counter
            .advance()
            .expect_err("an unpersistable rank must not be returned");
        let msg = err.to_string();
        assert!(
            msg.contains("boot counter"),
            "the error should name the counter: {msg}"
        );

        // And nothing was recorded, so the refusal is not itself a half-write.
        assert_eq!(store.node.get(counter_key()).unwrap(), None);
    }

    /// The other write failure: the insert lands and the flush does not. The
    /// boot is still refused — an unflushed rank can be lost to power loss and
    /// reissued — but the state it leaves differs from a pre-insert failure,
    /// and the difference is what an operator is told. The counter holds the
    /// *new* value, so the next boot takes one above it and skips a rank
    /// rather than reissuing one. Skipping is harmless; reissuing is not.
    #[test]
    fn a_flush_failure_refuses_the_boot_and_leaves_the_rank_written() {
        let (store, _tempdir) = store();
        assert_eq!(counter(store.clone()).advance().unwrap(), 1);

        let armed = counter(store.clone());
        armed.arm_flush_failure();
        assert!(
            armed.advance().is_err(),
            "an unflushed rank must not be returned"
        );

        // The insert landed, so the counter already reads 2 — unlike a
        // pre-insert failure, which leaves it at 1.
        let next = counter(store).advance().unwrap();
        assert_eq!(
            next, 3,
            "the next boot takes one above the written value, skipping the refused rank"
        );
    }

    /// The refusal is about *this* boot's rank, not about the store: a counter
    /// that can be written still advances past the failed attempt.
    #[test]
    fn a_boot_after_a_failed_write_still_advances() {
        let (store, _tempdir) = store();
        assert_eq!(counter(store.clone()).advance().unwrap(), 1);

        let armed = counter(store.clone());
        armed.arm_write_failure();
        assert!(armed.advance().is_err());

        // The rank the failed boot would have taken is still free, and the
        // next boot takes it.
        assert_eq!(counter(store).advance().unwrap(), 2);
    }

    /// A counter that is there and cannot be read fails the boot. It is not
    /// recovered from the persisted directory — that fallback existed and was
    /// removed, because the directory lags and a recovered rank could be one
    /// this node had already published at. See the module docs.
    #[test]
    fn a_corrupt_counter_fails_the_boot() {
        let (store, _tempdir) = store();
        corrupt_the_counter(&store);

        let err = counter(store)
            .advance()
            .expect_err("an unreadable counter must not yield a rank");
        let msg = err.to_string();
        assert!(
            msg.contains("unreadable"),
            "the error should say the counter could not be read: {msg}"
        );
        assert!(
            msg.contains("--node-id"),
            "the error should name the recovery: {msg}"
        );
    }

    #[test]
    fn a_first_boot_publishes_at_one() {
        let (store, _tempdir) = store();
        assert_eq!(counter(store).advance().unwrap(), 1);
    }

    #[test]
    fn each_boot_outranks_the_last() {
        let (store, _tempdir) = store();
        let counter = counter(store);
        assert_eq!(counter.advance().unwrap(), 1);
        assert_eq!(counter.advance().unwrap(), 2);
        assert_eq!(counter.advance().unwrap(), 3);
    }

    /// The count is what survives the process, not the handle: a reopened store
    /// must continue from what the previous one wrote.
    #[test]
    fn the_count_survives_reopening_the_store() {
        let tempdir = TempDir::new().unwrap();
        {
            let store = FjallStore::open(tempdir.path()).unwrap();
            assert_eq!(counter(store).advance().unwrap(), 1);
        }
        let store = FjallStore::open(tempdir.path()).unwrap();
        assert_eq!(counter(store).advance().unwrap(), 2);
    }

    /// The counter shares the `node` keyspace with the shard map, which scans by
    /// label prefix — its record must be invisible there.
    #[tokio::test]
    async fn the_shard_map_ignores_the_counter_record() {
        let (store, _tempdir) = store();
        counter(store.clone()).advance().unwrap();

        let map = crate::shard_map::ShardMap::load(store).unwrap();
        assert!(map.all_shards().await.is_empty());
    }
}
