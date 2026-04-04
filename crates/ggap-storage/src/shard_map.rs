use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use ggap_types::{GgapError, KeyRange, ShardId, ShardInfo, ShardState};

use crate::fjall::FjallStore;
use crate::keys::meta_key;

/// Sentinel shard_id used as the prefix for ShardMap metadata in the `meta`
/// keyspace. This must never collide with a real shard_id.
const SHARD_MAP_PREFIX: ShardId = u64::MAX;

pub(crate) fn shard_map_key(shard_id: ShardId) -> Vec<u8> {
    // meta_key(SHARD_MAP_PREFIX, "shard:XXXX") where XXXX is be_u64(shard_id)
    let label = format!("shard:{}", shard_id);
    meta_key(SHARD_MAP_PREFIX, &label)
}

/// Key used to store bootstrap membership for a shard created by a split.
/// Written atomically with the split data movement so restart can find peers.
pub(crate) fn bootstrap_members_key(shard_id: ShardId) -> Vec<u8> {
    meta_key(shard_id, "bootstrap_members")
}

pub(crate) fn encode<T: serde::Serialize>(val: &T) -> Result<Vec<u8>, GgapError> {
    bincode::serde::encode_to_vec(val, bincode::config::standard())
        .map_err(|e| GgapError::Storage(e.to_string()))
}

fn decode<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8]) -> Result<T, GgapError> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| GgapError::Storage(e.to_string()))
}

/// Persistent shard-to-range mapping backed by the `meta` keyspace of fjall.
///
/// The ShardMap tracks which `ShardId` owns which `KeyRange`. It keeps an
/// in-memory cache that is loaded on startup and updated on mutations.
pub struct ShardMap {
    store: Arc<FjallStore>,
    shards: RwLock<BTreeMap<ShardId, ShardInfo>>,
}

impl ShardMap {
    /// Load all shard entries from storage and build the in-memory cache.
    pub fn load(store: Arc<FjallStore>) -> Result<Self, GgapError> {
        let prefix = SHARD_MAP_PREFIX.to_be_bytes().to_vec();
        let mut shards = BTreeMap::new();

        for guard in store.meta.prefix(&prefix) {
            let (_, v) = guard
                .into_inner()
                .map_err(|e| GgapError::Storage(e.to_string()))?;
            let info = decode::<ShardInfo>(&v)?;
            shards.insert(info.shard_id, info);
        }

        Ok(ShardMap {
            store,
            shards: RwLock::new(shards),
        })
    }

    /// If no shards exist, create shard 0 with the full key range.
    pub async fn initialize_default(&self) -> Result<(), GgapError> {
        let mut shards = self.shards.write().await;
        if shards.is_empty() {
            let info = ShardInfo {
                shard_id: 0,
                range: KeyRange {
                    start: String::new(),
                    end: String::new(),
                },
                state: ShardState::Active,
            };
            self.persist_shard(&info)?;
            shards.insert(0, info);
        }
        Ok(())
    }

    /// Look up which shard owns a given key.
    pub async fn lookup_shard(&self, key: &str) -> Option<ShardInfo> {
        let shards = self.shards.read().await;
        shards.values().find(|s| s.range.contains(key)).cloned()
    }

    /// Get info for a specific shard.
    pub async fn get_shard(&self, shard_id: ShardId) -> Option<ShardInfo> {
        self.shards.read().await.get(&shard_id).cloned()
    }

    /// Persist a shard info entry (insert or update).
    pub async fn put_shard(&self, info: ShardInfo) -> Result<(), GgapError> {
        self.persist_shard(&info)?;
        self.shards.write().await.insert(info.shard_id, info);
        Ok(())
    }

    /// Remove a shard entry.
    pub async fn remove_shard(&self, shard_id: ShardId) -> Result<(), GgapError> {
        self.store
            .meta
            .remove(shard_map_key(shard_id))
            .map_err(|e| GgapError::Storage(e.to_string()))?;
        self.shards.write().await.remove(&shard_id);
        Ok(())
    }

    /// Return all shard infos.
    pub async fn all_shards(&self) -> Vec<ShardInfo> {
        self.shards.read().await.values().cloned().collect()
    }

    /// Allocate the next shard_id (max existing + 1).
    pub async fn next_shard_id(&self) -> ShardId {
        let shards = self.shards.read().await;
        shards.keys().max().map(|m| m + 1).unwrap_or(0)
    }

    fn persist_shard(&self, info: &ShardInfo) -> Result<(), GgapError> {
        let key = shard_map_key(info.shard_id);
        let val = encode(info)?;
        self.store
            .meta
            .insert(key, val)
            .map_err(|e| GgapError::Storage(e.to_string()))
    }

    /// Synchronous variant of `put_shard` — writes only to fjall storage,
    /// not to the in-memory cache. Safe to call from `spawn_blocking` contexts.
    /// The caller must subsequently update the in-memory cache via `put_shard`.
    pub fn put_shard_sync(&self, info: &ShardInfo) -> Result<(), GgapError> {
        self.persist_shard(info)
    }

    /// Update only the in-memory cache for two shards after a split whose
    /// storage writes have already been committed atomically by the caller.
    /// Does NOT write to fjall — storage is already consistent.
    pub async fn update_cache_after_split(&self, updated_source: ShardInfo, new_shard: ShardInfo) {
        let mut shards = self.shards.write().await;
        shards.insert(updated_source.shard_id, updated_source);
        shards.insert(new_shard.shard_id, new_shard);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_store() -> (TempDir, Arc<FjallStore>) {
        let dir = TempDir::new().unwrap();
        let store = FjallStore::open(dir.path()).unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn initialize_default_creates_shard_zero() {
        let (_dir, store) = open_store();
        let map = ShardMap::load(store).unwrap();
        map.initialize_default().await.unwrap();

        let shards = map.all_shards().await;
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, 0);
        assert_eq!(shards[0].range.start, "");
        assert_eq!(shards[0].range.end, "");
        assert_eq!(shards[0].state, ShardState::Active);
    }

    #[tokio::test]
    async fn lookup_shard_routes_correctly() {
        let (_dir, store) = open_store();
        let map = ShardMap::load(store).unwrap();

        // Shard 0: ["", "m"), Shard 1: ["m", "")
        map.put_shard(ShardInfo {
            shard_id: 0,
            range: KeyRange {
                start: String::new(),
                end: "m".into(),
            },
            state: ShardState::Active,
        })
        .await
        .unwrap();
        map.put_shard(ShardInfo {
            shard_id: 1,
            range: KeyRange {
                start: "m".into(),
                end: String::new(),
            },
            state: ShardState::Active,
        })
        .await
        .unwrap();

        assert_eq!(map.lookup_shard("a").await.unwrap().shard_id, 0);
        assert_eq!(map.lookup_shard("hello").await.unwrap().shard_id, 0);
        assert_eq!(map.lookup_shard("m").await.unwrap().shard_id, 1);
        assert_eq!(map.lookup_shard("zebra").await.unwrap().shard_id, 1);
    }

    #[tokio::test]
    async fn persistence_survives_reload() {
        let dir = TempDir::new().unwrap();
        let store = FjallStore::open(dir.path()).unwrap();
        let map = ShardMap::load(store).unwrap();
        map.initialize_default().await.unwrap();
        drop(map);

        let store2 = FjallStore::open(dir.path()).unwrap();
        let map2 = ShardMap::load(store2).unwrap();
        let shards = map2.all_shards().await;
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, 0);
    }

    #[tokio::test]
    async fn next_shard_id_increments() {
        let (_dir, store) = open_store();
        let map = ShardMap::load(store).unwrap();
        assert_eq!(map.next_shard_id().await, 0);
        map.initialize_default().await.unwrap();
        assert_eq!(map.next_shard_id().await, 1);
    }

    #[test]
    fn key_range_contains() {
        let full = KeyRange {
            start: String::new(),
            end: String::new(),
        };
        assert!(full.contains("anything"));

        let lower = KeyRange {
            start: String::new(),
            end: "m".into(),
        };
        assert!(lower.contains("a"));
        assert!(lower.contains(""));
        assert!(!lower.contains("m"));
        assert!(!lower.contains("z"));

        let upper = KeyRange {
            start: "m".into(),
            end: String::new(),
        };
        assert!(!upper.contains("a"));
        assert!(upper.contains("m"));
        assert!(upper.contains("z"));
    }
}
