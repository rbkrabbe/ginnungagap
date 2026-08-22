# ggap-storage

The persistence layer. Provides two storage traits (`LogStorage`,
`StateMachineStore`) with in-memory implementations for tests and fjall-backed
implementations for production. Has no dependency on `openraft`; the adapter
shim lives in `ggap-consensus`.

## Key encoding (`keys.rs`)

Every key in a shard-scoped keyspace starts with `be_u64(shard_id)`. This prefix
is **never omitted**. Multi-shard is live (splits create shards), and the prefix
is what isolates them: a range scan bounded to `[shard_prefix, shard_prefix+1)`
naturally covers exactly one shard. The `node` keyspace is the sole exception —
see below.

### Partition layouts

| Partition | Key | Value |
|-----------|-----|-------|
| `raft_log` | `shard(8) ++ index(8)` | `bincode(LogEntry)` |
| `data` | `shard(8) ++ key_utf8` | `bincode(KvEntry)` |
| `history` | `shard(8) ++ key_utf8 ++ \x00 ++ version(8)` | `bincode(KvEntry)` |
| `ttl_index` | `shard(8) ++ expires_at_ns_be_i64(8) ++ key_utf8` | `b""` |
| `meta` | `shard(8) ++ label_utf8` | `bincode(value)` |
| `node` | `label_utf8` (`++ shard(8)` where keyed by shard) | `bincode(value)` |

All multi-byte integers are big-endian so that lexicographic byte order matches
numeric order, making range queries correct without any post-sort step.

**History null-byte delimiter.** UTF-8 strings cannot contain `\x00`, so
inserting it between the user key and the version field ensures that a prefix
scan for `"foo\x00"` cannot accidentally match entries for the key `"foobar"`.
Without this delimiter, the prefix `shard ++ "foo"` would be a prefix of
`shard ++ "foobar\x00..." ` and the scan would return spurious results.

**The `node` keyspace.** Some records describe *this node* rather than one
shard: the shard map (`"shard:" ++ shard(8)`) and the persisted directory
(`"directory"`). They live in their own keyspace and are the one exception to
the shard-prefix rule — a fact with no shard does not get a fake shard id.
Records are separated by label, so a scan for one family must match on its
label prefix, never the bare keyspace: `ShardMap::load` scanning everything
would try to decode the directory record as a `ShardInfo` and fail the boot.

The shard map's records are keyed by shard id, but that does not make them
shard-scoped data. The `be_u64(shard_id)` prefix exists so `[be(N), be(N+1))`
isolates one shard's bytes; nothing scans the shard map that way. What it holds
is a node fact — `all_shards` is what decides which Raft groups this node
starts.

`node` lives in the same fjall `Database` as every other keyspace, so a batch
still spans it: a split commits data movement, `last_applied`, both shard map
records and `bootstrap_members` together or not at all.

**The persisted directory (`directory.rs`).** `DirectoryStore` writes the whole
`node_id -> NodeDescriptor` map as one record and reads it back at startup. It is
a cache of gossip whose only job is immediacy: a node that restarts and is
elected before any peer has gossiped to it resolves its peers straight away
instead of failing sends until it is dialled. `load` therefore never fails — a
missing, unreadable or corrupt record warns and yields an empty directory, and
the node re-learns it from the first peer that dials it.

**TTL index sort order.** Sorting by `expires_at_ns` first means the GC task can
find the next-to-expire key with a single prefix scan, taking only the first
result — O(1) rather than a full scan of the index.

## Storage traits (`traits.rs`)

Both traits use RPITIT (`-> impl Future<Output=...> + Send`), matching the style
of `RaftNode` in `ggap-consensus`. No `async-trait` dependency.

### `LogStorage`

Invariants that all implementations must maintain:

- **Durability before ack.** `save_vote` must flush to durable storage before
  returning `Ok`. Raft safety depends on a node never granting two votes in the
  same term; if the vote is lost across a crash, safety can be violated.
- **Append semantics.** `append` may overwrite an existing entry at the same
  index (the leader sends a corrective AppendEntries when a follower's log
  diverges). This is correct; the prior entry at that index was never committed.
- **Purge vs. truncate direction.** `purge(up_to)` removes the *oldest* entries
  (already snapshotted). `truncate(from)` removes the *newest* entries (in
  conflict with the leader). Confusing these two operations would corrupt the log.
- **`last_purged_index` persistence.** This value must survive restarts.
  Openraft uses it to know which log entries have been replaced by a snapshot.

### `StateMachineStore`

- **Version = log index.** `apply(shard_id, index, cmd)` uses `index` as both
  the MVCC version of the written entry and the `last_applied` cursor. This
  makes version monotonicity a consequence of Raft's guarantee that log indices
  are monotonically increasing. No separate version counter is needed.
- **`last_applied` must be updated even on CAS failure.** A CAS that fails
  (wrong expected value) still advances `last_applied` to `index`. This is
  essential: openraft may re-apply the same log index after a leader change, and
  the state machine must be idempotent with respect to `last_applied` — it must
  not apply an index it has already seen.
- **History survives delete.** `Delete` removes the entry from `data` but does
  not touch `history`. A client holding an old version can still perform a
  point-read at `at_version > 0`. History entries are compacted on write, not
  on delete.
- **Snapshot atomicity.** `install_snapshot` wipes the entire shard
  (`data`, `history`, `ttl_index`) and inserts the snapshot contents in a
  single batch. A partial install (e.g. crash mid-write) would leave the
  state machine in an undefined state. The single batch guarantees all-or-nothing
  semantics at the fjall level.

## MVCC (`mem.rs`, `fjall.rs`)

Each write to a key stores the new `KvEntry` in two places:

1. **`data` partition** — current value, keyed by `(shard, user_key)`.
   Overwritten on every write.
2. **`history` partition** — all versions, keyed by `(shard, user_key, version)`.
   Append-only. Compacted when the count per key exceeds `max_history_versions`
   (default 10).

`get(key, at_version=0)` reads from `data`. `get(key, at_version=N)` is a
point-lookup in `history`. There is no cross-version garbage collection; history
is compacted only on write and only by count.

### History compaction

After each successful write, if the number of `history` entries for the key
exceeds `max_history_versions`, the oldest entries (lowest version numbers) are
deleted in a separate batch. This is a best-effort operation: a crash between
the main write commit and the compaction batch leaves extra history entries,
which are benign — they will be cleaned up on the next write to the same key.

## Concurrency model (`fjall.rs`)

`FjallStore` is wrapped in `Arc<FjallStore>`. All fjall operations are
synchronous, so every trait method spawns a `tokio::task::spawn_blocking`
closure. The closure captures `Arc<FjallStore>` by clone, which is `Send`.

Cross-keyspace atomic writes use `db.batch()`. The batch is created, populated,
and committed entirely within a single `spawn_blocking` closure, so no async
context switch can interleave with a partial batch.

## TTL GC (`ttl.rs`)

`TtlGcTask` runs the expiry loop:

1. Scan `ttl_index` from the shard prefix; take the first entry (earliest
   expiry).
2. If `expires_at_ns <= now`, send `KvCommand::Delete` via an mpsc channel.
3. If `expires_at_ns > now`, sleep (via `tokio::time`) until then before sending.

The GC task does **not** apply the delete directly — it sends it through the Raft
proposal channel (`raft.client_write`). This ensures that TTL expiry is a
replicated operation, not a local side effect that would diverge across nodes.
The eager removal of the `ttl_index` entry after sending is an optimisation to
prevent the next poll from re-triggering; the Raft-committed delete will also
clean up the entry via the normal `Delete` path.

Because it sleeps on `tokio::time`, the task is driveable by the simulation
harness — never use `std::time` here.
