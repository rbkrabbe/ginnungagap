# ggap-server

Translates between the protobuf wire format and the `RaftNode` trait. Contains
no business logic — every decision that touches stored state is delegated to the
`RaftNode` implementation.

## Server topology

Two tonic servers are started by `ggap-node` and run concurrently:

| Function | Default port | Services |
|----------|-------------|---------|
| `serve_client` | `:17000` | `KvService`, server-reflection |
| `serve_cluster` | `:17001` | `RaftService`, `AdminService`, server-reflection |

Both register the full file descriptor set for server-reflection so that
`grpcurl` can introspect the schema on either port.

## `KvServiceImpl`

Parameterised over `R: RaftNode`. The only state it holds is `Arc<R>` and the
local `node_id` (used to populate `ResponseHeader`).

### Request validation

The following is checked before touching the `RaftNode`:

- `key` must be non-empty — returns `INVALID_ARGUMENT` immediately.
- `page_token` in `Scan` must be valid UTF-8 — returns `INVALID_ARGUMENT` if not.

No other validation is performed here. Key length limits, value size limits, and
character set restrictions (if any) are enforced at the storage layer.

### Proto ↔ domain mapping (`convert.rs`)

| Proto field | Domain |
|-------------|--------|
| `ttl_secs = 0` | `ttl_ns = None` (no expiry) |
| `ttl_secs = N` | `ttl_ns = Some(N * 1_000_000_000)` — converted to nanoseconds |
| `expect_version = 0` | Unconditional write |
| `ReadConsistency::LINEARIZABLE` | `ReadMode::Linearizable` |
| `WriteQuorum::MAJORITY` | `WriteMode::Majority` |
| `page_token` (bytes) | UTF-8 continuation key from previous scan |

### Error mapping

`GgapError` → gRPC status:

| Error | Status |
|-------|--------|
| `NotFound` | `NOT_FOUND` |
| `NotLeader` | `UNAVAILABLE` (with leader hint in message) |
| `VersionConflict` | `ABORTED` (expected and actual in message) |
| `Timeout` | `DEADLINE_EXCEEDED` |
| `Storage` | `INTERNAL` |
| `Consensus` | `INTERNAL` |
| `InvalidArgument` | `INVALID_ARGUMENT` |

### `Watch`

`Watch` is a server stream fed by a `broadcast` channel from the state machine's
change fan-out. A node without a configured `watch_tx` returns `UNIMPLEMENTED`.

## `RaftServiceImpl` / `AdminServiceImpl` / `GossipServiceImpl`

All implemented and served on the cluster-facing port:

- **`RaftService`** — `AppendEntries`, `Vote`, `InstallSnapshot`. Payloads are
  opaque bytes decoded inside `ggap-consensus`; the server never sees openraft
  types.
- **`AdminService`** — `ClusterStatus`, `AddLearner`, `ChangeMembership`,
  `SplitShard`, `ListShards`. Shard-aware, enriched with per-shard Raft state.
  For shards a node does not host locally it zeroes out (never fabricates)
  consensus fields — closing that gap needs the cluster-wide placement view.
- **`GossipService`** — carries the shard-registry gossip between nodes.
