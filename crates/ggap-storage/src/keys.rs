use ggap_types::ShardId;

/// `raft_log` partition: `shard(8) ++ index(8)` — fixed 16 bytes.
/// Big-endian on both components → lexicographic order = numeric order.
pub fn raft_log_key(shard_id: ShardId, index: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&shard_id.to_be_bytes());
    key[8..].copy_from_slice(&index.to_be_bytes());
    key
}

/// `data` partition: `shard(8) ++ key_utf8`
pub fn data_key(shard_id: ShardId, key: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + key.len());
    buf.extend_from_slice(&shard_id.to_be_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf
}

/// Exclusive upper bound for a shard's data keys: `(shard_id + 1)(8)`.
/// Used to build half-open range scans that stay within one shard.
pub fn data_shard_end(shard_id: ShardId) -> [u8; 8] {
    shard_id.wrapping_add(1).to_be_bytes()
}

/// `history` partition: `shard(8) ++ key_utf8 ++ \x00 ++ version(8)`
///
/// The `\x00` null-byte delimiter guarantees that a prefix scan for key
/// "foo\x00" never bleeds into entries for "foobar\x00…" (UTF-8 strings
/// cannot contain null bytes, so the delimiter is unique).
pub fn history_key(shard_id: ShardId, key: &str, version: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + key.len() + 1 + 8);
    buf.extend_from_slice(&shard_id.to_be_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(&version.to_be_bytes());
    buf
}

/// Prefix for scanning all history versions of a single key within a shard.
/// `shard(8) ++ key_utf8 ++ \x00`
pub fn history_prefix(shard_id: ShardId, key: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + key.len() + 1);
    buf.extend_from_slice(&shard_id.to_be_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.push(0x00);
    buf
}

/// `ttl_index` partition: `shard(8) ++ expires_at_ns_be_i64(8) ++ key_utf8`
///
/// Big-endian i64 preserves sort order for expiry timestamps (all positive
/// Unix nanosecond values sort correctly in big-endian).
pub fn ttl_index_key(shard_id: ShardId, expires_at_ns: i64, key: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 8 + key.len());
    buf.extend_from_slice(&shard_id.to_be_bytes());
    buf.extend_from_slice(&expires_at_ns.to_be_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf
}

/// 8-byte prefix for all TTL index entries belonging to a shard.
pub fn ttl_shard_prefix(shard_id: ShardId) -> [u8; 8] {
    shard_id.to_be_bytes()
}

/// `meta` partition: `shard(8) ++ label_utf8`
pub fn meta_key(shard_id: ShardId, label: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + label.len());
    buf.extend_from_slice(&shard_id.to_be_bytes());
    buf.extend_from_slice(label.as_bytes());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_log_keys_sort_by_index() {
        let a = raft_log_key(0, 1);
        let b = raft_log_key(0, 2);
        let c = raft_log_key(0, 1000);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn raft_log_key_different_shards_dont_interleave() {
        // Shard 0, high index should still be less than shard 1, low index.
        let shard0_high = raft_log_key(0, u64::MAX);
        let shard1_low = raft_log_key(1, 0);
        assert!(shard0_high < shard1_low);
    }

    #[test]
    fn history_prefix_no_bleed() {
        // "foo" prefix must not match entries for "foobar".
        let foo_prefix = history_prefix(0, "foo");
        let foobar_v1 = history_key(0, "foobar", 1);
        let foo_v1 = history_key(0, "foo", 1);

        // foo_v1 starts with foo_prefix
        assert!(foo_v1.starts_with(&foo_prefix));
        // foobar_v1 must NOT start with foo_prefix
        assert!(!foobar_v1.starts_with(&foo_prefix));
    }

    #[test]
    fn ttl_index_sorts_by_expiry_time() {
        let early = ttl_index_key(0, 1_000_000_000, "a");
        let late = ttl_index_key(0, 2_000_000_000, "a");
        assert!(early < late);
    }

    #[test]
    fn ttl_index_same_time_sorts_by_key() {
        let k1 = ttl_index_key(0, 1_000_000_000, "a");
        let k2 = ttl_index_key(0, 1_000_000_000, "b");
        assert!(k1 < k2);
    }

    #[test]
    fn data_key_is_shard_prefixed() {
        let key = data_key(42, "hello");
        assert_eq!(&key[..8], &42u64.to_be_bytes());
        assert_eq!(&key[8..], b"hello");
    }

    #[test]
    fn meta_key_is_shard_prefixed() {
        let key = meta_key(0, "last_applied");
        assert_eq!(&key[..8], &0u64.to_be_bytes());
        assert_eq!(&key[8..], b"last_applied");
    }
}
