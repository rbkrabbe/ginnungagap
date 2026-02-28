# ggap-types

Shared domain types imported by every other crate. Has no workspace dependencies of its own, so it can never create a dependency cycle.

## Type aliases

| Alias | Underlying | Invariant |
|-------|-----------|-----------|
| `NodeId` | `u64` | Opaque node identity. 0 is not reserved but not conventionally assigned. |
| `ShardId` | `u64` | Always `0` in Phases 1–6. Phase 7 (multi-raft) will use non-zero values. All storage keys are prefixed with the shard id regardless. |

## `KvEntry`

The canonical in-memory and on-disk representation of a key-value record.

```
key             : String   — valid UTF-8, non-empty (enforced by the gRPC layer)
value           : Vec<u8>  — arbitrary bytes
version         : u64      — equals the Raft log index at which this entry was written;
                             monotonically increasing per key, never reused
created_at_ns   : i64      — Unix nanoseconds; set on first write, never updated
modified_at_ns  : i64      — Unix nanoseconds; updated on every write including CAS
expires_at_ns   : Option<i64> — absolute deadline, not a duration
```

**Version monotonicity.** Because version = Raft log index, each committed write
to a key produces a strictly larger version than the previous one. A version of
`0` means "no version constraint" in `Put::expect_version`; it is never stored
in an actual entry.

**Timestamp semantics.** Timestamps are wall-clock nanoseconds from the node that
applies the command. They are not globally coordinated and must not be used for
ordering across nodes. They exist for client convenience (TTL display, auditing)
only.

## `KvCommand`

Commands that flow through the Raft log. They are serialized with bincode when
stored as `LogPayload::Normal(cmd)`.

| Variant | Effect | Idempotency |
|---------|--------|-------------|
| `Put` | Write or overwrite; optional `expect_version` CAS guard | Not idempotent — each application increments the version. |
| `Delete` | Remove the current entry; MVCC history survives | Idempotent once the key is absent. |
| `Cas` | Byte-level compare-and-swap; no version guard | Not idempotent — depends on current value. |

**`expect_version` semantics.** A value of `0` means "create or overwrite
unconditionally". Any non-zero value requires the current stored version to
match exactly, otherwise `GgapError::VersionConflict` is returned and the
command is not applied. This is checked before assigning the new version.

## `KvResponse`

Returned by `StateMachineStore::apply()` and `RaftNode::propose()`. Not stored;
it is derived from the state transition at apply time.

- `Written { version }` — the version assigned to the new entry (= log index).
- `Deleted { found }` — `false` when the key was already absent (not an error).
- `CasResult { success, current }` — `current` is the pre-swap entry regardless of success.

## `GgapError`

| Variant | When |
|---------|------|
| `NotFound` | Read for a key that does not exist (internal use; gRPC layer converts to `NOT_FOUND` status). |
| `NotLeader { leader }` | Write rejected because the node is not the current leader. `leader` is a hint only and may be stale. |
| `VersionConflict` | `expect_version` mismatch. Carries expected and actual values for client diagnostics. |
| `Timeout` | Operation exceeded the configured deadline. |
| `Storage(String)` | Unrecoverable I/O or serialization failure. Always fatal to the operation; upper layers should not retry without investigation. |
| `Consensus(String)` | openraft-level error (Phase 4+). |
| `InvalidArgument(String)` | Bad input caught before touching storage. |

Error types use `thiserror` (never `anyhow`) because `ggap-types` is a library.
`anyhow` is used only in the binary (`ggap-node`).
