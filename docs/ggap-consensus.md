# ggap-consensus

Defines the `RaftNode` trait and provides `StubRaftNode` as a test double.
The real openraft-backed implementation is Phase 4.

## `RaftNode` trait

The single abstraction that `ggap-server` depends on. Any type implementing
`RaftNode` can be handed to the gRPC servers without them knowing whether they
are talking to a stub, a real Raft node, or a future multi-shard router.

```rust
pub trait RaftNode: Send + Sync + 'static {
    fn shard_id(&self) -> ShardId;
    fn propose(cmd, mode) -> impl Future<Output = Result<KvResponse, GgapError>> + Send;
    fn read(key, at_version, mode) -> impl Future<Output = Result<Option<KvEntry>, GgapError>> + Send;
    fn scan(start_key, end_key, limit, mode) -> impl Future<Output = Result<(Vec<KvEntry>, Option<String>), GgapError>> + Send;
}
```

### Contract for implementors

- **`propose` is the write path.** It must not return until the command is
  durably committed by a quorum (for `WriteMode::Majority`) or all nodes (for
  `WriteMode::All`). Returning `Ok` before commitment violates linearizability.
- **`read` consistency.** `ReadMode::Linearizable` requires the node to confirm
  it is still the leader (read-index or lease) before returning. `Sequential`
  and `Eventual` may serve stale reads. The stub ignores the mode.
- **`NotLeader` error.** If the node is not the leader, `propose` must return
  `Err(GgapError::NotLeader { leader: Option<String> })`. The `leader` hint
  lets the client retry against the right node without a full rediscovery round.
- **`at_version = 0`** in `read` means current value. Non-zero requests an
  exact historical version; return `None` if that version does not exist or has
  been compacted.
- **Scan continuation key.** If more results exist past the page, `scan` returns
  `Some(key)` as the continuation token. That key is the first key of the next
  page, not the last key of the current page. Callers use it as `start_key` in
  the next request.

## `StubRaftNode`

A minimal in-memory implementation used in Phases 1–3 to allow the gRPC layer
to be tested before real Raft exists. Backed by a `BTreeMap` under an
`Arc<RwLock<...>>`.

It is correct enough for basic API testing but has no durability, no
replication, and no read consistency enforcement. It will be replaced by the
real `RaftNode` in Phase 4.

## Phase 4 plan

Phase 4 will introduce a struct (working name `RaftNode` or `OpenRaftNode`)
that:

- Wraps `openraft::Raft` typed with the application's `TypeConfig`.
- Adapts `FjallLogStorage` and `FjallStateMachine` from `ggap-storage` into
  openraft's `RaftLogStorage` and `RaftStateMachine` traits.
- Carries a `ShardId` so the multi-shard dispatch in Phase 7 is
  `HashMap<ShardId, RaftNode>` with no structural changes.
- Returns `GgapError::NotLeader` from `propose` when openraft signals the node
  is not the current leader.
