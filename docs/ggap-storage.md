# ggap-storage

The persistence layer. Provides two storage traits (`LogStorage`,
`StateMachineStore`) with in-memory implementations for tests and fjall-backed
implementations for production. Has no dependency on `openraft`; the adapter
shim lives in `ggap-consensus` (Phase 4).

## Key encoding (`keys.rs`)

Every storage key starts with `be_u64(shard_id)`. This prefix is **never
omitted**, even in the single-shard (Phase 1–6) deployment. The prefix makes
Phase 7 multi-shard additive: a range scan bounded to
`[shard_prefix, shard_prefix+1)` naturally isolates one shard with no schema
migration.

### Partition layouts

| Partition | Key | Value |
|-----------|-----|-------|
| `raft_log` | `shard(8) ++ index(8)` | `bincode(LogEntry)` |
| `data` | `shard(8) ++ key_utf8` | `bincode(KvEntry)` |
| `history` | `shard(8) ++ key_utf8 ++ \x00 ++ version(8)` | `bincode(KvEntry)` |
| `ttl_index` | `shard(8) ++ expires_at_ns_be_i64(8) ++ key_utf8` | `b""` |
| `meta` | `shard(8) ++ label_utf8` | `bincode(value)` |

All multi-byte integers are big-endian so that lexicographic byte order matches
numeric order, making range queries correct without any post-sort step.

**History null-byte delimiter.** UTF-8 strings cannot contain `\x00`, so
inserting it between the user key and the version field ensures that a prefix
scan for `"foo\x00"` cannot accidentally match entries for the key `"foobar"`.
Without this delimiter, the prefix `shard ++ "foo"` would be a prefix of
`shard ++ "foobar\x00..." ` and the scan would return spurious results.

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
point-lookup in `history`. There is no cross-version garbage collection in
Phase 3; history is compacted only on write and only by count.

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

`TtlGcTask` is a skeleton. The design is:

1. Scan `ttl_index` from the shard prefix; take the first entry (earliest
   expiry).
2. If `expires_at_ns <= now`, send `KvCommand::Delete` via an mpsc channel.
3. If `expires_at_ns > now`, sleep until then before sending.

The GC task does **not** apply the delete directly — it sends it through the
Raft proposal channel (wired in Phase 4). This ensures that TTL expiry is a
replicated operation, not a local side effect that would diverge across nodes.
The eager removal of the `ttl_index` entry after sending is an optimisation to
prevent the next poll from re-triggering; the Raft-committed delete will also
clean up the entry via the normal `Delete` path.

The task is not spawned in `ggap-node` until Phase 4.
