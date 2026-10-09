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
//! rank it knows to be unfounded. It only does so once every reading has been
//! tried:
//!
//! - **Absent counter** — a first boot. Publish at 1.
//! - **Readable counter** — one above it.
//! - **Unusable counter** — recover the rank from the persisted directory's
//!   entry for this node, which records what this node last published. A
//!   directory that is absent, or holds no entry for this node, is a first boot
//!   like any other; one that is present and cannot be read leaves the rank
//!   unknown, and the boot fails.
//!
//! A rank that cannot be *written* is refused for the same reason. Returning
//! it would leave the next boot to read the stale counter and issue the same
//! number again — and the directory keeps the entry it already holds when two
//! ranks are equal, so the second of those boots could not install an address
//! it had changed. The failure surfaces here, at the cause, rather than later
//! as a move that quietly does not happen.
//!
//! That is what makes a rank unique to one boot, which is the property the
//! directory's ordering rests on: two *ranked* copies at one incarnation
//! describe one published state and can be used interchangeably. The exception
//! is incarnation 0, which this counter never issues — it marks a descriptor
//! written on a node's behalf, and those are ordered by arrival rather than by
//! rank.
//!
//! **Wiping the data dir loses the count**, and no recovery covers it: both
//! records go together, so the node restarts at 1 and cannot outbid peers still
//! holding a higher incarnation for that id. A wipe combined with an address
//! change needs a fresh node id.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ggap_types::GgapError;

use crate::directory::DirectoryStore;
use crate::fjall::FjallStore;
use crate::keys::node_key;

fn counter_key() -> Vec<u8> {
    node_key("boot_counter")
}

/// Reads, increments and writes the boot counter in the `node` keyspace.
pub struct BootCounter {
    store: Arc<FjallStore>,
    self_node_id: u64,
    /// Fault injection: when `true`, [`Self::write`] fails as though the
    /// keyspace rejected it, so a test can reach the refusal in
    /// [`Self::advance`]. Always present, armable only via
    /// [`Self::arm_write_failure`] behind `#[cfg(any(test, feature =
    /// "test-utils"))]`.
    fail_write: AtomicBool,
}

impl BootCounter {
    /// `self_node_id` is the id whose entry the directory fallback reads: the
    /// rank being recovered is this node's own, and no other entry describes it.
    pub fn new(store: Arc<FjallStore>, self_node_id: u64) -> Self {
        BootCounter {
            store,
            self_node_id,
            fail_write: AtomicBool::new(false),
        }
    }

    /// Make the next counter write fail. Test builds and `test-utils` only.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn arm_write_failure(&self) {
        self.fail_write.store(true, Ordering::SeqCst);
    }

    /// The incarnation for this boot: one above the last rank this node can
    /// establish for itself, persisted before it is returned.
    ///
    /// Fails when the rank cannot be established — the counter unusable *and*
    /// the persisted directory unreadable — and when it cannot be persisted.
    /// See the module docs for why both are a refusal to start.
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
    fn previous(&self) -> Result<u64, GgapError> {
        match self.read_counter() {
            Ok(Some(incarnation)) => Ok(incarnation),
            Ok(None) => Ok(0),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "the boot counter is unusable; recovering this node's rank from the \
                     persisted directory"
                );
                self.recover_from_directory()
            }
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

    /// The rank this node last published, read back from the persisted
    /// directory's entry for itself. 0 when the directory has nothing to say
    /// about this node — a first boot; `Err` when it has something and it
    /// cannot be read, which is the case [`Self::advance`] refuses to guess at.
    fn recover_from_directory(&self) -> Result<u64, GgapError> {
        let Some(entries) = DirectoryStore::new(self.store.clone()).try_load()? else {
            tracing::warn!("no persisted directory either; treating this as a first boot");
            return Ok(0);
        };
        // A tombstone for this node carries no rank to recover. It is a first
        // boot as far as the counter is concerned; the node is refused a start
        // for being retired long before the rank could matter.
        let recovered = entries
            .iter()
            .find(|(node_id, _)| *node_id == self.self_node_id)
            .and_then(|(_, entry)| entry.descriptor())
            .map_or(0, |desc| desc.incarnation);
        tracing::warn!(
            node_id = self.self_node_id,
            recovered,
            "recovered this node's last published rank from the persisted directory"
        );
        Ok(recovered)
    }

    fn write(&self, incarnation: u64) -> Result<(), GgapError> {
        if self.fail_write.load(Ordering::SeqCst) {
            return Err(GgapError::Storage("injected write failure".into()));
        }
        self.store
            .node
            .insert(counter_key(), incarnation.to_be_bytes().to_vec())
            .map_err(|e| GgapError::Storage(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ggap_types::{DirectoryEntry, NodeAddrs, NodeDescriptor};
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

    fn desc(incarnation: u64) -> DirectoryEntry {
        DirectoryEntry::Live(NodeDescriptor::new(
            NodeAddrs::new("node:17001", "node:17000"),
            incarnation,
        ))
    }

    /// A tombstone records no rank, so recovery treats it as a first boot. The
    /// node never gets that far in practice: `ggap-node` refuses to start a node
    /// its own persisted directory says was retired.
    #[test]
    fn a_tombstoned_self_entry_recovers_no_rank() {
        let (store, _tempdir) = store();
        DirectoryStore::new(store.clone())
            .save(&[(SELF, DirectoryEntry::Removed)])
            .unwrap();
        corrupt_the_counter(&store);

        assert_eq!(counter(store).advance().unwrap(), 1);
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

    /// The recovery that matters: the counter rotted, but the directory still
    /// records what this node last published, so the rank survives it.
    #[test]
    fn a_corrupt_counter_recovers_its_rank_from_the_directory() {
        let (store, _tempdir) = store();
        DirectoryStore::new(store.clone())
            .save(&[(SELF, desc(7)), (2, desc(3))])
            .unwrap();
        corrupt_the_counter(&store);

        assert_eq!(counter(store.clone()).advance().unwrap(), 8);
        assert_eq!(
            counter(store).advance().unwrap(),
            9,
            "and the repaired counter carries on from there"
        );
    }

    /// Only this node's own entry is a record of what this node published;
    /// another node's rank says nothing about ours.
    #[test]
    fn recovery_ignores_other_nodes_entries() {
        let (store, _tempdir) = store();
        DirectoryStore::new(store.clone())
            .save(&[(2, desc(9)), (3, desc(11))])
            .unwrap();
        corrupt_the_counter(&store);

        assert_eq!(counter(store).advance().unwrap(), 1);
    }

    /// A corrupt counter with no directory at all is indistinguishable from a
    /// first boot, so it is treated as one rather than as a failure.
    #[test]
    fn a_corrupt_counter_with_no_directory_is_a_first_boot() {
        let (store, _tempdir) = store();
        corrupt_the_counter(&store);

        assert_eq!(counter(store).advance().unwrap(), 1);
    }

    /// Both records unusable: the rank is unknown rather than unset, and
    /// starting at 1 would strand the node below peers that already hold a
    /// higher incarnation for it. Refuse the boot instead.
    #[test]
    fn a_corrupt_counter_and_an_unreadable_directory_fails_the_boot() {
        let (store, _tempdir) = store();
        DirectoryStore::new(store.clone())
            .save(&[(SELF, desc(7))])
            .unwrap();
        let key = crate::keys::node_key("directory");
        let mut bytes = store.node.get(&key).unwrap().unwrap().to_vec();
        bytes.truncate(bytes.len() / 2);
        store.node.insert(&key, bytes).unwrap();
        corrupt_the_counter(&store);

        assert!(counter(store).advance().is_err());
    }

    /// An *absent* counter is a first boot and must not consult the directory:
    /// a node re-seeded by peers before it ever published would otherwise adopt
    /// a rank it never wrote.
    #[test]
    fn an_absent_counter_is_a_first_boot_even_with_a_directory() {
        let (store, _tempdir) = store();
        DirectoryStore::new(store.clone())
            .save(&[(SELF, desc(7))])
            .unwrap();

        assert_eq!(counter(store).advance().unwrap(), 1);
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
