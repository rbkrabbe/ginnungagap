//! The boot counter: the incarnation this node publishes its own descriptor at.
//!
//! A descriptor is authored by the node it describes, so its incarnation only
//! has to be a clock over one writer's publications. A counter in the data dir,
//! incremented once per start, is exactly that: a node that restarts at a new
//! address publishes above every copy of itself still in flight, so the move is
//! resolved by rank rather than by arrival order.
//!
//! **Wiping the data dir loses the count.** A node that starts from an empty
//! data dir publishes at 1 again, which cannot outbid peers still holding a
//! higher incarnation for that id — so a wipe combined with an address change
//! needs a fresh node id.

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
}

impl BootCounter {
    pub fn new(store: Arc<FjallStore>) -> Self {
        BootCounter { store }
    }

    /// The incarnation for this boot: one above the last one recorded, and
    /// persisted before it is returned.
    ///
    /// Never fails. An absent record is a first boot and yields 1; an
    /// unreadable, corrupt or unwritable one warns and restarts the count,
    /// because a node that cannot rank its own descriptor is still a node that
    /// should start. Both cases cost only the guarantee that this boot outranks
    /// the last — which is what a wiped data dir costs anyway.
    pub fn advance(&self) -> u64 {
        let incarnation = self.previous().saturating_add(1);
        if let Err(e) = self.write(incarnation) {
            tracing::warn!(
                error = %e,
                incarnation,
                "cannot persist the boot counter; the next boot will publish at this \
                 incarnation again"
            );
        }
        incarnation
    }

    /// The last recorded count, or 0 for anything this node cannot read as one.
    fn previous(&self) -> u64 {
        let bytes = match self.store.node.get(counter_key()) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return 0,
            Err(e) => {
                tracing::warn!(error = %e, "cannot read the boot counter; restarting the count");
                return 0;
            }
        };
        match <[u8; 8]>::try_from(&bytes[..]) {
            Ok(be) => u64::from_be_bytes(be),
            Err(_) => {
                tracing::warn!(
                    len = bytes.len(),
                    "the boot counter is corrupt; restarting the count"
                );
                0
            }
        }
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

    use tempfile::TempDir;

    fn store() -> (Arc<FjallStore>, TempDir) {
        let tempdir = TempDir::new().unwrap();
        let store = FjallStore::open(tempdir.path()).unwrap();
        (store, tempdir)
    }

    #[test]
    fn a_first_boot_publishes_at_one() {
        let (store, _tempdir) = store();
        assert_eq!(BootCounter::new(store).advance(), 1);
    }

    #[test]
    fn each_boot_outranks_the_last() {
        let (store, _tempdir) = store();
        let counter = BootCounter::new(store);
        assert_eq!(counter.advance(), 1);
        assert_eq!(counter.advance(), 2);
        assert_eq!(counter.advance(), 3);
    }

    /// The count is what survives the process, not the handle: a reopened store
    /// must continue from what the previous one wrote.
    #[test]
    fn the_count_survives_reopening_the_store() {
        let tempdir = TempDir::new().unwrap();
        {
            let store = FjallStore::open(tempdir.path()).unwrap();
            assert_eq!(BootCounter::new(store).advance(), 1);
        }
        let store = FjallStore::open(tempdir.path()).unwrap();
        assert_eq!(BootCounter::new(store).advance(), 2);
    }

    /// Garbage restarts the count rather than failing the boot: an unrankable
    /// node is worth more than no node.
    #[test]
    fn a_corrupt_record_restarts_the_count() {
        let (store, _tempdir) = store();
        store.node.insert(counter_key(), b"seven".to_vec()).unwrap();

        assert_eq!(BootCounter::new(store.clone()).advance(), 1);
        assert_eq!(
            BootCounter::new(store).advance(),
            2,
            "and is repaired by it"
        );
    }

    /// The counter shares the `node` keyspace with the shard map, which scans by
    /// label prefix — its record must be invisible there.
    #[tokio::test]
    async fn the_shard_map_ignores_the_counter_record() {
        let (store, _tempdir) = store();
        BootCounter::new(store.clone()).advance();

        let map = crate::shard_map::ShardMap::load(store).unwrap();
        assert!(map.all_shards().await.is_empty());
    }
}
