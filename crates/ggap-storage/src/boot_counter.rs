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
//! **Wiping the data dir loses the count**, and no recovery covers it: both
//! records go together, so the node restarts at 1 and cannot outbid peers still
//! holding a higher incarnation for that id. A wipe combined with an address
//! change needs a fresh node id.

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
}

impl BootCounter {
    /// `self_node_id` is the id whose entry the directory fallback reads: the
    /// rank being recovered is this node's own, and no other entry describes it.
    pub fn new(store: Arc<FjallStore>, self_node_id: u64) -> Self {
        BootCounter {
            store,
            self_node_id,
        }
    }

    /// The incarnation for this boot: one above the last rank this node can
    /// establish for itself, persisted before it is returned.
    ///
    /// Fails only when the counter is unusable *and* the persisted directory
    /// cannot be read either — see the module docs for why that is a refusal to
    /// start rather than a warning.
    pub fn advance(&self) -> Result<u64, GgapError> {
        let incarnation = self.previous()?.saturating_add(1);
        if let Err(e) = self.write(incarnation) {
            // This boot is still correctly ranked; the debt falls on the next
            // one, which will read the stale value and publish this incarnation
            // again. That is a tie rather than a deficit — the self-publication
            // wins ties back on the following tick — so it does not justify
            // refusing to start.
            tracing::warn!(
                error = %e,
                incarnation,
                "cannot persist the boot counter; the next boot will publish at this \
                 incarnation again"
            );
        }
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
        let recovered = entries
            .iter()
            .find(|(node_id, _)| *node_id == self.self_node_id)
            .map(|(_, desc)| desc.incarnation)
            .unwrap_or(0);
        tracing::warn!(
            node_id = self.self_node_id,
            recovered,
            "recovered this node's last published rank from the persisted directory"
        );
        Ok(recovered)
    }

    fn write(&self, incarnation: u64) -> Result<(), GgapError> {
        self.store
            .node
            .insert(counter_key(), incarnation.to_be_bytes().to_vec())
            .map_err(|e| GgapError::Storage(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ggap_types::{NodeAddrs, NodeDescriptor};
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

    fn desc(incarnation: u64) -> NodeDescriptor {
        NodeDescriptor::new(NodeAddrs::new("node:17001", "node:17000"), incarnation)
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
