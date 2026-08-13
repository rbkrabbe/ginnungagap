# ggap-consensus

Defines the `RaftNode` trait, the real openraft-backed implementation
(`OpenRaftNode`), the multi-shard router, and the split machinery. Also provides
`StubRaftNode` as a lightweight test double.

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
  `Err(GgapError::NotLeader { leader_id: Option<u64>, leader: Option<String> })`.
  Both halves come from openraft's `ForwardToLeader`; construct the error via
  `node::not_leader()` rather than building the struct, so they cannot drift
  apart. `leader_id` is the stable identity a forwarder resolves through the
  gossip directory; `leader` is the address that was current when the error was
  built and may already be stale. Either half may be absent. The hint lets the
  caller retry against the right node without a full rediscovery round.
- **`at_version = 0`** in `read` means current value. Non-zero requests an
  exact historical version; return `None` if that version does not exist or has
  been compacted.
- **Scan continuation key.** If more results exist past the page, `scan` returns
  `Some(key)` as the continuation token. That key is the first key of the next
  page, not the last key of the current page. Callers use it as `start_key` in
  the next request.

## `StubRaftNode`

A minimal in-memory implementation used in unit tests to exercise the gRPC layer
without spinning up real Raft. Backed by a `BTreeMap` under an `Arc<RwLock<...>>`.
It has no durability, no replication, and no read-consistency enforcement, so it
is for testing only — production paths use `OpenRaftNode`.

## `OpenRaftNode`

The production `RaftNode` implementation. It:

- Wraps `openraft::Raft` (`GgapRaft`) typed with `GgapTypeConfig`.
- Adapts `ggap-storage`'s fjall log store and state machine into openraft's
  `RaftLogStorage` / `RaftStateMachine` traits (`GgapLogStorage`,
  `GgapStateMachine`), and reaches the cluster over `GgapNetwork`.
- Carries a `ShardId`; a node hosting multiple shards holds one `OpenRaftNode`
  per shard, dispatched by `ShardRouter`.
- Returns `GgapError::NotLeader` from `propose` when openraft signals the node is
  not the current leader.

## Multi-shard and splits

- **`ShardRouter`** maps `key -> ShardId -> OpenRaftNode` for reads and writes,
  blocks writes to shards mid-split, and rejects scans that span shards.
- **`SplitCoordinator` / `run_split_handler`** create new shards from a range
  split via `KvCommand::Split`.
- **`ShardRegistry` + `GossipTask`** maintain a cluster-wide view of which node
  hosts which shard, plus a `node_id -> NodeAddrs` directory holding each node's
  cluster and client gRPC addresses. `LeaseManager` backs lease-based leader
  reads; `ClusterNode` keeps openraft types out of the `ggap-server` dependency
  tree.

  **Both addresses live in the Raft membership** (`GgapNode`, the openraft
  `Node` for this cluster). Cluster bootstrap, `AddLearner` and a split's
  `source_members` each put a full `NodeAddrs` into consensus state — a
  split-created shard inherits both addresses for every member, on the split
  itself and again from `bootstrap_members` after a restart. `refresh_local`
  derives the directory from `raft.metrics()` every tick, and **no node
  originates its own entry**: a node's addresses reach its own directory the
  same way every peer's do, through membership.

  The directory is therefore a *cache of committed state*. Entries for shards a
  node hosts are **derived** and authoritative; entries for shards it does not
  host are **copies** carried by gossip, which is the only reason gossip still
  exchanges the directory at all. Stop the gossip task entirely and a node still
  reports both addresses for every peer in a shard it hosts.

  `merge_directory` replaces **whole values**: an entry is a copy of one
  membership record, and its two fields belong together. The rule it depends on
  is about *provenance, not content* — every entry comes from membership, and
  which addresses that record carries is membership's business. So merging an
  entry with no client address **clears** a previously-known one rather than
  treating the gap as "unknown, don't touch", and that is what makes a stale
  address retractable at all. In production both are always present
  (`AddLearner` rejects an empty `client_addr`; both CLI flags have non-empty
  defaults), so clearing is reachable only where a test harness builds
  cluster-only membership on purpose.

  Bootstrap gossip peers are *not* directory entries. `ShardRegistry::new` takes
  `seed_peers` as `(node_id, cluster_addr)` into a separate field that
  `peers_excluding_self` unions in and `snapshot_for_gossip` never emits, so a
  dial hint can never be gossiped over a fully-known entry. Nothing in
  `ggap-node` passes any: a node joins by being added to a shard's membership,
  which is exactly what tells it about the cluster (tk-9bcd tracks whether the
  parameter earns its keep).

  **Still open:** copied entries are ordered last-write-wins, so a peer holding
  an older copy can overwrite a newer one. tk-c4fc stamps them with the
  membership `(term, index)` they came from; tk-1bf0 is the same problem.
