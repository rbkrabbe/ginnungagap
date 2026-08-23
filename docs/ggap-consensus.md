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
  hosts which shard, plus a `node_id -> NodeDescriptor` directory holding each node's
  cluster and client gRPC addresses. `LeaseManager` backs lease-based leader
  reads; `ClusterNode` keeps openraft types out of the `ggap-server` dependency
  tree.

  **Membership carries ids; the directory resolves addresses.** `GgapNode`, the
  openraft `Node` for this cluster, is an empty struct — it exists because
  openraft needs a `Node` type, and it is where a future consensus-only field
  belongs. Cluster bootstrap, `AddLearner` and a split's `source_members` all
  put ids alone into consensus state. `GgapNetwork` resolves its target through
  the directory on **every** RPC, so a node that moves is dialled at its new
  address on the next send with no new client, and an id the directory cannot
  resolve fails the RPC — which openraft treats as an unreachable peer and
  retries.

  **Every node is the sole author of its own descriptor.** `refresh_local`
  publishes `(cluster_addr, client_addr, incarnation)` for this node each tick,
  whether or not it hosts a shard, and gossip carries it everywhere.
  `merge_directory` orders copies by **incarnation, highest wins**: a node
  restarted at a new address publishes at a higher incarnation and outbids every
  stale copy in flight. The incarnation comes from a boot counter persisted in
  the `node` keyspace and starts at 1.

  `AddLearner` is the one exception, and only in appearance. It still carries
  the joining node's addresses over the wire — they are how the cluster first
  learns where that node is, since nothing could dial it otherwise — and writes
  them into the leader's directory at **incarnation 0**. The node's own first
  publication outranks that hint immediately, so authorship is never actually
  divided.

  The directory is therefore the *source of truth* for addresses, not a cache of
  anything. Gossip is the only path an address travels: stop the gossip task on
  a node and it can still route Raft traffic to peers it already knows, but it
  learns nothing about a node that moves.

  `merge_directory` replaces **whole values**: a descriptor is one node's
  complete statement about where it can be reached, so its two fields belong
  together. Merging a descriptor with no client address **clears** a
  previously-known one rather than treating the gap as "unknown, don't touch",
  which is what makes a stale address retractable at all. A descriptor with
  neither address describes no node and is skipped. In production both are
  always present (`AddLearner` rejects an empty `client_addr`; both CLI flags
  have non-empty defaults), so clearing is reachable only where a test harness
  publishes a cluster-only descriptor on purpose.

  The directory is also **cached on disk** (`ggap-storage`'s `DirectoryStore`,
  one `node` record). The gossip task writes it out after each round when it has
  changed, and `ggap-node` seeds the registry from it before starting the task.
  This buys immediacy, not capability: peers re-seed a restarted node within a
  gossip round anyway, but a node that restarts and is elected before that
  happens can resolve its peers straight away. Incarnations are persisted with
  the entries, so a restored entry still outranks the stale copies in flight.
  Being a cache, it never blocks a boot — a missing or corrupt record logs a
  warning and starts the node with an empty directory.

  Bootstrap gossip peers are *not* directory entries. `ShardRegistry::new` takes
  `seed_peers` as `(node_id, cluster_addr)` into a separate field that
  `peers_excluding_self` unions in and `snapshot_for_gossip` never emits, so a
  dial hint can never be gossiped over a fully-known entry. Nothing in
  `ggap-node` passes any: a node joins by being added to a shard's membership,
  which is exactly what tells it about the cluster. `seed_peers` survives for
  nodes in no membership that nobody will ever dial — the observer harness in
  `ggap-server/tests/three_node_cluster.rs`, and later `ggap-pd`.

  **Still open:** nothing removes a departed node from the directory, and the
  removal now outlives a restart (tk-c47e).
