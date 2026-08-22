//! Persisted copy of the node directory (`node_id -> NodeDescriptor`).
//!
//! The directory itself lives in `ggap-consensus`'s `ShardRegistry` and is
//! rebuilt from gossip; this is a cache of it, written by the gossip task and
//! read back at startup. Its only job is *immediacy*: a node that restarts and
//! is elected before any peer has gossiped to it can resolve its peers straight
//! away instead of failing sends until someone dials it.
//!
//! Because it is a cache, [`DirectoryStore::load`] never fails. A missing,
//! unreadable or corrupt record warns and yields an empty directory — the node
//! recovers by being dialled, exactly as one with no record at all does.

use std::sync::Arc;

use ggap_types::{GgapError, NodeDescriptor};

use crate::fjall::FjallStore;
use crate::keys::node_key;

/// The whole directory is one `node` record: it is written and read as a unit,
/// so per-node keys would buy nothing but a range scan.
fn directory_key() -> Vec<u8> {
    node_key("directory")
}

/// Reads and writes the persisted directory in the `node` keyspace.
pub struct DirectoryStore {
    store: Arc<FjallStore>,
}

impl DirectoryStore {
    pub fn new(store: Arc<FjallStore>) -> Self {
        DirectoryStore { store }
    }

    /// The persisted directory, or an empty one if there is nothing usable to
    /// read. Sorted by node id, as [`Self::save`] wrote it.
    pub fn load(&self) -> Vec<(u64, NodeDescriptor)> {
        let bytes = match self.store.node.get(directory_key()) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Vec::new(),
            Err(e) => {
                tracing::warn!(error = %e, "cannot read the persisted directory; starting empty");
                return Vec::new();
            }
        };
        match bincode::serde::decode_from_slice(&bytes, bincode::config::standard()) {
            Ok((entries, _)) => entries,
            Err(e) => {
                tracing::warn!(error = %e, "persisted directory is corrupt; starting empty");
                Vec::new()
            }
        }
    }

    /// Replace the persisted directory with `entries`.
    pub fn save(&self, entries: &[(u64, NodeDescriptor)]) -> Result<(), GgapError> {
        let bytes = bincode::serde::encode_to_vec(entries, bincode::config::standard())
            .map_err(|e| GgapError::Storage(e.to_string()))?;
        self.store
            .node
            .insert(directory_key(), bytes)
            .map_err(|e| GgapError::Storage(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ggap_types::NodeAddrs;
    use tempfile::TempDir;

    fn store() -> (Arc<FjallStore>, TempDir) {
        let tempdir = TempDir::new().unwrap();
        let store = FjallStore::open(tempdir.path()).unwrap();
        (store, tempdir)
    }

    fn desc(cluster: &str, client: &str, incarnation: u64) -> NodeDescriptor {
        NodeDescriptor::new(NodeAddrs::new(cluster, client), incarnation)
    }

    #[test]
    fn round_trips_descriptors() {
        let (store, _tempdir) = store();
        let dir = DirectoryStore::new(store);
        let entries = vec![
            (2, desc("b:17001", "b:17000", 1)),
            (3, desc("c:17001", "c:17000", 4)),
        ];

        dir.save(&entries).unwrap();
        assert_eq!(dir.load(), entries);
    }

    #[test]
    fn a_save_replaces_the_previous_record() {
        let (store, _tempdir) = store();
        let dir = DirectoryStore::new(store);

        dir.save(&[(2, desc("b:17001", "b:17000", 1))]).unwrap();
        dir.save(&[(3, desc("c:17001", "c:17000", 1))]).unwrap();

        assert_eq!(dir.load(), vec![(3, desc("c:17001", "c:17000", 1))]);
    }

    #[test]
    fn an_absent_record_loads_empty() {
        let (store, _tempdir) = store();
        assert_eq!(DirectoryStore::new(store).load(), vec![]);
    }

    /// A cache must never keep the node from starting: garbage decodes to an
    /// empty directory, and the node re-learns it from gossip.
    #[test]
    fn a_corrupt_record_loads_empty() {
        let (store, _tempdir) = store();
        store
            .node
            .insert(directory_key(), b"not bincode".to_vec())
            .unwrap();

        assert_eq!(DirectoryStore::new(store).load(), vec![]);
    }

    /// The directory shares the `node` keyspace with the shard map, so a
    /// persisted directory must be invisible to `ShardMap::load` — which scans
    /// by label prefix, never the whole keyspace.
    #[tokio::test]
    async fn the_shard_map_ignores_the_directory_record() {
        let (store, _tempdir) = store();
        DirectoryStore::new(store.clone())
            .save(&[(2, desc("b:17001", "b:17000", 1))])
            .unwrap();

        let map = crate::shard_map::ShardMap::load(store).unwrap();
        assert!(map.all_shards().await.is_empty());
    }
}
