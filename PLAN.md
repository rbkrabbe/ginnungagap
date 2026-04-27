# Ginnungagap — Distributed KV Store

A CP-by-default distributed key-value store in Rust with a gRPC interface. Named after the primordial Norse void from which order emerges.

## Design Goals

- **CP by default** — Raft consensus guarantees linearizable reads/writes
- **Configurable consistency knobs** — per-request read consistency (linearizable / sequential / eventual) and write quorum (majority / all) to consciously trade consistency for availability
- **gRPC interface** — external client API + internal cluster/admin API
- **Pure Rust** — no C FFI dependencies (fjall storage, not RocksDB)
- **MVCC** — per-key version history (configurable depth), enabling point-in-time reads

## Key Technology Choices

| Concern        | Choice                                        | Rationale |
|---------------|----------------------------------------------|-----------|
| Consensus     | `openraft 0.9`                               | Async-native, trait-based, production-used in Databend |
| Storage       | `fjall 3`                                    | Pure Rust LSM-tree, no C FFI, MVCC snapshots |
| gRPC          | `tonic 0.12` + `prost 0.13`                  | Standard Rust gRPC stack |
| Async runtime | `tokio 1`                                    | De facto standard |
| Config        | `figment` (TOML → env → CLI layering)        | Clean config hierarchy |
| Observability | `tracing` + `metrics-exporter-prometheus`    | Structured logs + Prometheus metrics |

---

## Workspace Layout

```
ginnungagap/
├── Cargo.toml                  # virtual workspace
├── Cargo.lock
├── PLAN.md                     # this file
├── config/
│   └── default.toml            # canonical defaults (embedded in binary via include_str!)
├── proto/
│   └── ginnungagap/v1/
│       ├── types.proto         # KeyValue, NodeInfo, ReadConsistency, WriteQuorum enums
│       ├── kv.proto            # KvService — external client API
│       └── cluster.proto       # RaftService + AdminService — internal cluster API
└── crates/
    ├── ggap-proto/             # build.rs + generated tonic/prost code only
    ├── ggap-types/             # domain types, KvCommand, GgapError — no gRPC dep
    ├── ggap-storage/           # LogStorage + StateMachineStore traits + fjall impls
    ├── ggap-consensus/         # openraft TypeConfig, RaftNetwork, RaftNode facade
    ├── ggap-server/            # tonic KvService + ClusterService + AdminService impls
    └── ggap-node/              # binary: CLI, config loading, wiring, startup/shutdown
```

---

## Proto Design

### `types.proto` — shared enums and messages
- `KeyValue { key, value, version, created_at_ns, modified_at_ns, expires_at_ns }`
- `NodeInfo { node_id, client_addr, cluster_addr }`
- `ResponseHeader { cluster_id, node_id, raft_index, raft_term }`
- `ReadConsistency` enum: `LINEARIZABLE` (default) | `SEQUENTIAL` | `EVENTUAL`
- `WriteQuorum` enum: `MAJORITY` (default) | `ALL`

### `kv.proto` — external client API
```protobuf
service KvService {
  rpc Get              (GetRequest)              returns (GetResponse);
  rpc Put              (PutRequest)              returns (PutResponse);
  rpc Delete           (DeleteRequest)           returns (DeleteResponse);
  rpc Scan             (ScanRequest)             returns (ScanResponse);
  rpc Watch            (stream WatchRequest)     returns (stream WatchEvent);
  rpc CompareAndSwap   (CasRequest)             returns (CasResponse);
}
```

Notable request fields:
- `GetRequest.at_version` — `0` = latest value; `>0` = fetch from version history (MVCC)
- `PutRequest.expect_version` — `0` = unconditional; `>0` = optimistic concurrency check
- `PutRequest.ttl_secs` — `0` = no expiry
- `ScanRequest.page_token` — continuation token for paginated scans
- `WatchCreateRequest.start_index` — replay events from a given Raft log index

### `cluster.proto` — internal only (bound on `cluster_addr`)
```protobuf
service RaftService   { AppendEntries, RequestVote, InstallSnapshot (streaming) }
service AdminService  { ClusterStatus, AddLearner, ChangeMembership }
```

---

## Crate Responsibilities

### `ggap-types`
Pure domain types with no network dependency. Other crates import from here to avoid circular deps through gRPC types.
- `KvCommand`: `Put { key, value, ttl_ns, expect_version }` | `Delete { key }` | `Cas { key, expected, new_value, ttl_ns }`
- `KvResponse`: `Written { version }` | `Deleted { found }` | `CasResult { success, current }` | `Conflict { expected, actual }` | `NoOp`
  - `Conflict` is returned (not errored) when a conditional Put's `expect_version` mismatches; mapped to `Status::aborted` at the gRPC layer. This prevents openraft from treating a business-logic conflict as a fatal storage failure.
  - `NoOp` is returned for Raft-internal entries (Blank, Membership); guarded by `unreachable!()` in `ggap-server`.
- `ReadMode`, `WriteMode` enums
- `GgapError` (thiserror): `NotFound`, `NotLeader { leader }`, `VersionConflict`, `Timeout`, `Storage`, `Consensus`, ...

`KvCommand` implements `openraft::AppData`.

### `ggap-storage`
Fjall partition layout:

| Partition   | Key encoding                                  | Value               | Purpose                        |
|------------|-----------------------------------------------|---------------------|-------------------------------|
| `raft_log` | `be_u64(shard_id) ++ be_u64(index)`           | `bincode(LogEntry)` | Raft log entries               |
| `data`     | `be_u64(shard_id) ++ key_bytes`               | `bincode(KvEntry)`  | Current value per key          |
| `history`  | `be_u64(shard_id) ++ key_bytes ++ \x00 ++ be_u64(ver)`| `bincode(KvEntry)`  | Per-key version history (MVCC) |
| `ttl_index`| `be_u64(shard_id) ++ be_i64(expires_at_ns) ++ key` | empty          | Expiry scan by timestamp       |
| `meta`     | `be_u64(shard_id) ++ string literal`          | bincode             | vote, last_applied, membership |

All partitions are prefixed with `be_u64(shard_id)`. In Phase 1–6 this is always `0`. The prefix costs 8 bytes per key and avoids a data migration if multi-raft is added later (Phase 7).

Traits: `LogStorage`, `StateMachineStore` (also `MemLogStorage` / `MemStateMachine` for tests).

`TtlGcTask`: tokio background task that scans the `ttl_index` partition, sleeps until the next expiry, then routes a `KvCommand::Delete` through Raft (so TTL expiry is committed, replicated, and watched). The Raft channel is wired in Phase 4.

History compaction: after each write, if depth > `max_history_versions`, delete the oldest `history` entry.

### `ggap-consensus`
- `GgapTypeConfig`: `openraft::RaftTypeConfig` impl wiring all associated types
- `GgapNetworkFactory` + `GgapRaftClient`: makes outbound tonic calls to peer `RaftService`; connection pool via `DashMap<NodeId, RaftServiceClient>`
- `RaftNode { shard_id: ShardId, ... }` facade (only thing `ggap-server` touches):
  - `propose(cmd, write_mode) -> KvResponse`
  - `linearizable_read(key, at_version?)` — ReadIndex or lease-based
  - `sequential_read(key, at_version?)` — local SM, no leader check
  - `eventual_read(key, at_version?)` — local SM, no staleness bound
- `LeaseManager`: tracks `lease_acquired_at: Instant`; `is_valid()` checks monotonic clock against `lease_duration_ms`

`ShardId` is a newtype `u64`. In Phase 1–6 `ggap-node` always constructs `RaftNode { shard_id: ShardId(0), .. }`. The field costs nothing at runtime and means multi-raft is an additive change (a `HashMap<ShardId, RaftNode>`) rather than a structural refactor.

### `ggap-server`
- `KvServiceImpl` on `client_addr`: proto ↔ domain translation, calls `RaftNode`, maps `GgapError → tonic::Status`
  - `NotLeader` → `Status::unavailable` + `ggap-leader-addr` trailing metadata (client SDK uses this to retry)
- `ClusterServiceImpl` on `cluster_addr`: `RaftService` delegates to openraft handle; `AdminService` calls `RaftNode` membership methods
- `WatchManager`: per-connection, subscribes to `tokio::sync::broadcast::Receiver<WatchEvent>` (capacity: 1024), filters by key range; lagged receivers get `WatchEvent { canceled: true }`

### `ggap-node`
- `clap` derive: `--node-id`, `--client-addr`, `--cluster-addr`, `--peers id=addr,...`, `--config`, `--data-dir`
- `figment` config hierarchy: embedded `default.toml` → config file → `GINNUNGAGAP_*` env vars → CLI
- Graceful shutdown on `SIGTERM`/`SIGINT`: drain in-flight requests, flush fjall, stop Raft

---

## Key Data Flows

### Write path (Put)
```
Client → KvService::put()
  → validate request
  → KvCommand::Put
  → RaftNode::propose()
  → openraft::Raft::client_write()   # replicates to quorum
  → FjallStateMachine::apply()
      write batch: data + history + ttl_index
      broadcast WatchEvent (after commit)
  → PutResponse { new_version }
```

### Read path — Linearizable (default)
```
KvService::get()
  → RaftNode::linearizable_read()
      if leader + lease valid     → serve from local SM (zero extra RTT)
      if leader + no lease        → ReadIndex heartbeat, then serve
      if not leader               → Err(NotLeader { leader_hint })
                                     client retries against leader
```

### Read path — Sequential / Eventual
```
Any node, no leader check
  → local FjallStateMachine::get()
  → ResponseHeader.raft_index = last_applied  (client tracks freshness)
```

### MVCC history read (`at_version > 0`)
```
After consistency check:
  → FjallStateMachine::get(key, Some(at_version))
  → point-lookup in `history` partition at (key_bytes ++ be_u64(version))
  → NotFound if compacted
```

### Log compaction / snapshot
```
SnapshotTrigger fires when last_applied - last_snapshot > threshold
  → FjallSnapshotBuilder::build_snapshot()  # fjall checkpoint
  → openraft purges log entries up to boundary
Lagging follower:
  → leader streams InstallSnapshot chunks
  → follower drops keyspace, restores from checkpoint
```

---

## Configuration Knobs

```toml
[storage]
data_dir             = "/var/lib/ginnungagap"
max_key_bytes        = 4096
max_value_bytes      = 1048576
max_history_versions = 10       # MVCC depth per key; 0 = disable history
ttl_gc_interval_secs = 30

[raft]
heartbeat_interval_ms    = 150
election_timeout_min_ms  = 500
election_timeout_max_ms  = 1000
snapshot_threshold       = 50000

[consistency]                        # ← CP/AP tradeoff knobs
default_read_mode    = "linearizable" # linearizable | sequential | eventual
default_write_quorum = "majority"     # majority | all
lease_enabled        = true           # false = safe in containerized/VM envs
lease_duration_ms    = 4000           # must be < election_timeout_min_ms

[server]
watch_broadcast_capacity = 1024
request_timeout_ms       = 5000

[observability]
log_level             = "info"
log_format            = "json"        # json | pretty
metrics_addr          = "0.0.0.0:9090"
tracing_otlp_endpoint = ""
```

---

## Workspace Dependencies

```toml
[workspace.dependencies]
tokio                       = { version = "1", features = ["full"] }
tonic                       = { version = "0.12", features = ["tls", "transport"] }
tonic-build                 = "0.12"
prost                       = "0.13"
openraft                    = { version = "0.9", features = ["serde"] }
fjall                       = "3"
serde                       = { version = "1", features = ["derive"] }
bincode                     = { version = "2", features = ["serde"] }
thiserror                   = "1"
anyhow                      = "1"
clap                        = { version = "4", features = ["derive"] }
figment                     = { version = "0.10", features = ["toml", "env"] }
toml                        = "0.8"
tracing                     = "0.1"
tracing-subscriber          = { version = "0.3", features = ["json", "env-filter"] }
tower                       = "0.4"
bytes                       = "1"
dashmap                     = "5"
tokio-util                  = { version = "0.7", features = ["time"] }
metrics                     = "0.22"
metrics-exporter-prometheus = "0.13"
uuid                        = { version = "1", features = ["v4"] }
turmoil                     = "0.6"   # dev-dependency; simulation harness (Phase 6)
```

---

## Phased Implementation

### Phase 1 — Skeleton ✅
- [x] Workspace `Cargo.toml`, all 6 crate stubs with minimal `lib.rs` / `main.rs`
- [x] Proto files (`types.proto`, `kv.proto`, `cluster.proto`)
- [x] `ggap-proto/build.rs` with `tonic_build::configure()`
- [x] `ggap-types`: all domain types, `KvCommand`, `KvResponse`, `GgapError`
- [x] `ggap-node`: CLI parsing (`clap`) + `figment` config loading + `config/default.toml`

### Phase 2 — gRPC Server (skeleton) ✅
- [x] `KvServiceImpl`, `ClusterServiceImpl`, `AdminServiceImpl` stubs — compiles, binds, returns `Status::unimplemented`
- [x] Proto ↔ domain conversions; `GgapError → tonic::Status` mapping
- [x] `RaftNode` trait defined in `ggap-consensus` (interface only, no impl yet)
- [x] `KvServiceImpl` wired against a `StubRaftNode` backed by a plain `HashMap` (no Raft, no persistence) — sufficient for `grpcurl` smoke tests
- [x] `ggap-node` starts both servers; single-node `grpcurl` Get/Put/Delete works end-to-end
- [x] gRPC server reflection (`tonic-reflection`) registered on both servers

### Phase 3 — Storage Layer ✅
- [x] `MemLogStorage` + `MemStateMachine` (test-only implementations)
- [x] `FjallLogStorage`: append, truncate, get, purge
- [x] `FjallStateMachine`: apply, get (with `at_version`), scan, snapshot
- [x] Partition key encoding helpers, history write + compaction
- [x] `TtlGcTask` (sleep-loop-based background expiry skeleton; wired to Raft in Phase 4)
- [x] Unit tests: log append/truncate, SM apply, snapshot round-trip

### Phase 4 — Consensus Layer ✅
- [x] `GgapTypeConfig` (`openraft::RaftTypeConfig` impl), `GgapNetworkFactory` + `GgapNetwork` (outbound tonic calls to peer `RaftService`)
- [x] `GgapLogStorage` (`RaftLogStorage` + `RaftLogReader` v2 trait impls over `FjallLogStorage`)
- [x] `GgapStateMachine` (`RaftStateMachine` v2 impl): apply, snapshot build/install, `applied_state`
- [x] `OpenRaftNode` (`RaftNode` trait): `propose` via `raft.client_write()`, linearizable/sequential/eventual reads
- [x] `LeaseManager`: tracks `acquired_at: Instant`, `is_valid()` checks monotonic clock; `ensure_linearizable_or_lease()` skips ReadIndex when lease is fresh
- [x] `OpenRaftCluster` (`ClusterNode` trait): `append_entries`, `vote`, `install_snapshot` as bytes-in/bytes-out handlers keeping openraft types out of `ggap-server`
- [x] Swap `StubRaftNode` → `OpenRaftNode` in `ggap-node`; `TtlGcTask` wired through `raft.client_write()`
- [x] `ggap-node` initialises a real single-shard Raft cluster; single-node smoke-test passes
- [x] **DST discipline**: `tokio::time` used throughout (`LeaseManager`, `TtlGcTask`, all timeouts)
- [x] 3-node cluster integration tests (leader election, basic ops, sequential/eventual reads, leader failover)

**Post-phase bug fixes (same commit):**
- `install_snapshot` and `apply` now both encode `or_last_applied` as `Option<LogId<u64>>`; all readers decode the same type (was a crash-on-restart deserialization mismatch)
- `KvResponse::Conflict` added; `GgapStateMachine::apply` catches `VersionConflict` and returns `Ok(Conflict)` instead of mapping to `StorageError` (openraft treats any `Err` from `apply` as fatal — node would halt on every stale conditional Put)
- `kv_service.rs` match arms made explicit: `Conflict → Status::aborted`, `NoOp → unreachable!()`

### Phase 5 — Advanced Features ✅
- [x] Watch fan-out: broadcast channel in `FjallStateMachine`, key-range filtering, unique `watch_id` via atomic counter, `WatchCancelRequest` handling via `tokio::select!`, lagged receivers get `canceled=true` then stream closes, TTL `EXPIRE` events broadcast from `TtlGcTask`
- [x] MVCC `at_version` reads via `history` partition (integrated into `KvServiceImpl.get()`)
- [x] Log compaction: snapshot build + install on lagging follower (tested via DST `test_snapshot_catchup`)
- [x] Sequential + eventual read paths tested end-to-end (`sequential_read_from_follower`, `eventual_read_from_any_node` in 3-node cluster tests)
- [x] Watch integration tests: event ordering, key-range filtering, lag-based cancellation

### Phase 6 — Hardening (in progress)
- [x] **Deterministic simulation testing (DST)**: `sim_cluster.rs` harness — spawn N in-memory nodes, `FaultController` injects partitions/message drops/delays via configurable drop rates, seeded PRNG for reproducible fault sequences. 7 tests: `test_election_under_paused_time`, `test_leader_failure_and_reelection`, `test_partition_and_heal`, `test_message_drop_linearizability`, `test_message_drop_linearizability_concurrent`, `test_snapshot_catchup`, `test_membership_change_under_partition`
- [x] Chaos tests: kill leader + verify re-election, network partition + heal, message drops with concurrent writes, membership change under partition — all covered by DST suite
- [x] Prometheus metrics: request rate/latency p50/p99, Raft term, commit lag, match index
- [x] OpenTelemetry trace propagation (client → server → consensus)
- [x] Admin RPCs: `ClusterStatus` (reads Raft metrics for term, leader, last_applied, voter membership), `AddLearner` (adds non-voting learner via openraft), `ChangeMembership` (replaces voter set via joint-consensus). All wired through `ShardRouter` → `OpenRaftNode` → openraft APIs

---

## Phase 7 — Cross-Shard Consistent Reads

### Scope and Non-Goals

Phase 7 has one user-visible feature: **a `Scan` whose result is a consistent MVCC snapshot across shards** when a key range spans a split. Everything else in this phase is plumbing required to make that feature provably correct.

**In scope:**
- A globally meaningful logical timestamp (Hybrid Logical Clock) attached to every committed entry
- Storage and read paths re-keyed so MVCC point-in-time reads work in HLC, not per-shard log index
- A read-wait barrier so a shard can serve "as of timestamp T" only after it has applied everything ≤ T
- Cross-shard scan fan-out + merge with epoch-checked routing
- HLC continuity across shard splits (the source's HLC frontier becomes the new shard's floor)
- Hooks (no logic) for cross-shard write coordination — enough that Phase 8 can add transactional batch puts without reshaping the apply path

**Explicitly out of scope:**
- No external load driver, no benchmark harness, no perf gating
- No automated split heuristics, placement driver, or rebalancer
- No cross-shard write transactions (only the foundation that makes them implementable)
- No client-driven snapshot timestamp negotiation beyond the existing `ResponseHeader`
- No watch fan-out across shards — `Watch` remains single-shard for this phase

**Already in place** (do not rebuild):
- Storage keys prefixed with `be_u64(shard_id)`
- `ShardId`, `KeyRange`, `ShardInfo`, `ShardState` in `ggap-types`
- `ShardMap` (persistent, in-memory cache, sync + async writers, `next_shard_id`, `update_cache_after_split`)
- `ShardRouter` (`route_read`, `route_write`, `route_scan`, `add_shard`, `remove_shard`, `get_node`, `get_cluster`)
- `SplitCoordinator` + `KvCommand::Split { split_key, new_shard_id, source_range, source_members }`
- `run_split_handler` background task and atomic two-phase apply (data move + ShardMap update)
- `AdminService.split_shard()` / `list_shards()` RPCs
- `GgapError::WrongShard` and `ShardSplitting` routing errors
- Single-shard MVCC via `at_version` against the `history` keyspace

The remaining work assumes all of the above and changes them only where called out.

### Consistency Model

The guarantee Phase 7 must deliver, stated precisely:

> Given a `ConsistentScan(start, end, snapshot_hlc=T)`, the response is the union of every committed write `w` such that `w.commit_hlc ≤ T` and `w.key ∈ [start, end)`, and excludes every write with `commit_hlc > T`. The set of shards consulted is determined by the `ShardMap` epoch captured when the scan was planned; if a split commits with epoch ≤ T after planning, the scan retries against the new map.

Equivalently:
- **Snapshot isolation across shards** — all shards observe the same logical "as-of" point.
- **External consistency** when `T` is left unspecified and the server picks `T = HLC.now()`: every write that returned to its client before the scan started has `commit_hlc < T` and is therefore included. (Bounded by HLC's clock-skew assumption — same caveat as CockroachDB without TrueTime.)
- **Causal continuity across shards**: a client that reads version `V` from shard A and then issues an op against shard B observes shard B at HLC ≥ V. Implemented by piggybacking the latest observed HLC on `ResponseHeader` and clients echoing it on the next request.

What this does **not** promise:
- No serializable transactions. Concurrent `Put` against the same key on the same shard remains last-writer-wins by HLC; cross-shard atomic writes are explicitly out of scope.
- No snapshot reads "in the future" — `T` must be ≤ the leader's current HLC (else error). We do not block waiting for a future timestamp.

### Design — Hybrid Logical Clocks

We use HLC because it gives globally-comparable timestamps without a centralized timestamp oracle, integrates cleanly with Raft (each apply ticks the clock), and matches the existing single-leader-per-shard topology. The alternative (centralized TSO à la TiKV/PD) was rejected: it would add a required RPC to the write path and a new Raft group dedicated to ticking timestamps, both larger architectural commitments than this phase warrants.

**Type** (lives in `ggap-types`):

```rust
pub struct Hlc {
    pub physical_ms: u64,   // wall-clock ms since Unix epoch, 44 bits used
    pub logical: u32,       // monotonic counter when physical doesn't advance
}
```

Wire form: a single `u64` packing `(physical_ms << 16) | (logical & 0xFFFF)`. 16 bits of logical counter is enough for ~65k events per millisecond per shard — well above any realistic apply rate. Physical ms in 48 bits is good through year ~10880.

**Per-shard clock**: every `OpenRaftNode` owns an `Arc<HybridClock>` that supports:
- `now() -> Hlc` — returns `max(last, (wall_now_ms, 0))`, ticking logical if wall time hasn't advanced.
- `observe(remote: Hlc) -> Hlc` — `last = max(last, remote, wall_now); logical += 1 if needed`.
- `applied_frontier() -> Hlc` — the largest HLC of any entry that has been applied to the state machine.
- `wait_until_applied(target: Hlc, deadline: Instant) -> Result<()>` — async wait for `applied_frontier ≥ target`.

The clock is fed by:
1. **Leader propose path**: leader calls `now()` and stamps the command before `client_write`.
2. **Apply path** (every node): `apply()` calls `observe(commit_hlc)` so followers' clocks track the leader.
3. **gRPC ingress**: `KvServiceImpl` calls `observe(request_header.commit_hlc)` so client-supplied HLCs propagate causality.
4. **Idle heartbeat**: when `applied_frontier` lags `now()` by more than `idle_tick_ms`, the leader proposes a `KvCommand::ClockTick { hlc }` no-op so reads don't stall waiting for a quiet shard.

**Determinism for DST**: `HybridClock` takes an injectable `NowFn` (already exists in `ggap-types`). Simulation tests pass a controlled clock; production uses `tokio::time` (which respects `start_paused`) wrapped to nanos. We never call `std::time::Instant` directly.

### Storage Changes Summary

| Concern | Today | After Phase 7 |
|---------|-------|---------------|
| `KvEntry.version` | u64 = `log_id.index` (per-shard, opaque) | u64 = packed HLC (globally comparable) |
| Public `at_version` semantics | "give me this exact prior log index" | "give me the value committed at this HLC" — point-in-time |
| `history` key | `shard ++ key ++ \x00 ++ be_u64(version)` | `shard ++ key ++ \x00 ++ be_u64(commit_hlc)` (same shape, different value) |
| `history` ordering | by log-index per shard (already monotonic) | by HLC per shard (still monotonic — leader stamps with `now()` which only goes up) |
| Per-shard meta | `last_applied: LogId`, `membership` | + `applied_hlc_frontier: Hlc`, + `shard_map_epoch: u64` |
| `KvCommand` variants | `Put`, `Delete`, `Cas`, `Split` | + `ClockTick { hlc }`, + `WriteIntent { txn_id, key, value, ttl_ns }`, + `ResolveIntent { txn_id, key, commit }` |
| `ShardMap` | flat list of `ShardInfo` | + `epoch: u64` bumped on every successful split apply |

The `version`-becomes-HLC change is intentional: it keeps the public `KvEntry.version` field as a single u64 the way clients already use it, while making the value globally meaningful. Internal sites that today compare versions for ordering (`history` scan, watch event ordering) keep working because HLCs are also monotonically ordered. Sites that treat version as the Raft log index — there is currently exactly one such use, the watch event's `raft_index` field — are split out: watch events get a separate `raft_index: u64` (already present) plus a new `commit_hlc: u64`.

### RPC Changes Summary

`proto/ginnungagap/v1/types.proto`:

```protobuf
message ResponseHeader {
  uint64 cluster_id = 1;
  uint64 node_id = 2;
  uint64 raft_index = 3;
  uint64 raft_term = 4;
  uint64 commit_hlc = 5;        // NEW: HLC observed at the apply boundary that produced this response
  uint64 shard_map_epoch = 6;   // NEW: ShardMap epoch the server used to route
}

message RequestHeader {           // NEW message; carried via gRPC metadata, not body
  uint64 observed_hlc = 1;        // client echoes the largest HLC it has seen
  uint64 min_shard_map_epoch = 2; // optional; client refuses stale routing
}
```

`proto/ginnungagap/v1/kv.proto` — additive only:

```protobuf
service KvService {
  // ... existing RPCs unchanged
  rpc ConsistentScan (ConsistentScanRequest) returns (stream ConsistentScanChunk);
}

message ConsistentScanRequest {
  string start_key = 1;
  string end_key   = 2;
  uint64 snapshot_hlc = 3;     // 0 = server picks HLC.now()
  uint32 chunk_size  = 4;      // entries per stream message; 0 = server default
  bytes  page_token  = 5;      // opaque; encodes (snapshot_hlc, last_key, shard_map_epoch)
}

message ConsistentScanChunk {
  ResponseHeader header = 1;   // commit_hlc echoes snapshot_hlc; shard_map_epoch is the planning epoch
  repeated KeyValue kvs = 2;
  bytes next_page_token = 3;   // empty on terminal chunk
}
```

`Get`, `Put`, `Delete`, `Cas`, `Scan` proto bodies are **unchanged**. Causal-continuity HLC and routing epoch travel as gRPC metadata so existing clients ignore them.

`cluster.proto` gains one internal RPC for the read-wait barrier:

```protobuf
service RaftService {
  // ... existing RPCs unchanged
  rpc WaitApplied (WaitAppliedRequest) returns (WaitAppliedResponse);  // NEW
}

message WaitAppliedRequest {
  uint64 shard_id = 1;
  uint64 target_hlc = 2;
  uint64 deadline_ms = 3;
}

message WaitAppliedResponse {
  uint64 applied_hlc = 1;
  bool   timed_out = 2;
}
```

This is internal-only — only the cross-shard scan coordinator calls it, and only when fanning out to shards on remote nodes. Same-node fan-out uses `HybridClock::wait_until_applied` directly.

### Implementation Increments

Each increment is sized to one PR. Every increment ends in a green `cargo fmt && cargo clippy && cargo build && cargo test --all`. Each builds on the previous; do not skip ahead. The whole sequence touches ~10–15 files.

---

#### 7.0 — Cleanup and rebase (this PR)

**Goal**: erase obsolete planning artifacts so future contributors aren't confused.

- Delete `SPLIT_PLAN.md` (its contents are realized — split is Raft-replicated, atomic, and bootstraps full membership).
- Replace the "Multi-Raft Readiness (Phase 7 — deferred)" section in `PLAN.md` with this Phase 7 plan.
- No code changes.

**Done when**: `git ls-files | grep -i split_plan` is empty; `PLAN.md` reflects the new scope.

---

#### 7.1 — `Hlc` type and per-shard `HybridClock` (no behavior change)

**Goal**: introduce the clock type and the per-shard clock service. Nothing reads or writes through it yet; this is pure plumbing so the next PR can be small.

**Files**:
- `crates/ggap-types/src/lib.rs` — add `Hlc { physical_ms, logical }` plus `pack/unpack: u64`, `Ord`, `serde`.
- `crates/ggap-consensus/src/clock.rs` (new) — `HybridClock` with `now`, `observe`, `applied_frontier`, `wait_until_applied`, plus `NowFn` injection.
- `crates/ggap-consensus/src/lib.rs` — re-export.

**Tests** (in `clock.rs`):
- `now_is_monotonic` — repeated calls strictly increase even when wall clock is frozen.
- `observe_jumps_forward_then_ticks` — observing a future HLC pulls clock forward; subsequent `now` is greater.
- `wait_until_applied_unblocks` — set up a clock with a manual `set_applied(t)` test hook; one task waits for `t+1`, another sets it; the waiter completes within a paused-tokio `advance(1ms)`.
- `wait_until_applied_times_out` — never satisfied; returns `Err(Timeout)` at deadline.

**Done when**: clock module compiles, tests pass, no other crate uses it yet.

---

#### 7.2 — Stamp every applied entry with HLC

**Goal**: HLC starts being the single source of truth for "when did this commit happen", but the public-facing `version` field still uses `log_id.index`. This isolates the storage migration from the API migration.

**Files**:
- `crates/ggap-types/src/lib.rs` — add `KvCommand::ClockTick { hlc: u64 }`; extend every other `KvCommand` variant with no fields (the HLC isn't carried in the command, it's stamped at apply time on the leader).
- `crates/ggap-consensus/src/node.rs` — `propose` calls `clock.now()` and passes it into `apply` via a side channel (an `Arc<HybridClock>` already shared with the FSM); the leader's `now()` becomes the entry's `commit_hlc` for that shard.
- `crates/ggap-consensus/src/state_machine.rs` — `apply` receives `commit_hlc` (added to the FSM `apply` signature), calls `clock.observe(commit_hlc)`, and threads it into `FjallStateMachine::apply`.
- `crates/ggap-storage/src/fjall.rs` — `KvEntry` gains `commit_hlc: u64` (default 0 to keep existing on-disk records readable). New writes populate it. `apply` persists per-shard `applied_hlc_frontier` in the `meta` keyspace alongside `last_applied`.
- `crates/ggap-storage/src/keys.rs` — add `applied_hlc_key(shard_id) -> Vec<u8>`.

**Decision: leader-only HLC stamping.** Followers don't re-tick on apply; they observe the leader's stamp. This guarantees byte-identical apply on every replica (deterministic state machine). The leader picks `commit_hlc` *before* `client_write`; if the proposing node loses leadership, openraft drops the proposal and the next leader re-proposes with its own (necessarily-later) HLC.

**Tests**:
- Extend `FjallStateMachine` apply tests: write three keys, assert `commit_hlc` strictly monotonic.
- Crash-restart test: write, restart, assert `applied_hlc_frontier` recovered.
- `three_node_cluster` test: assert all three nodes' `applied_hlc_frontier` converge after a few writes.

**Done when**: every committed entry has a well-defined `commit_hlc`; `version` (public API) is still `log_id.index`.

---

#### 7.3 — HLC-indexed history and `at_hlc` reads

**Goal**: switch the MVCC index from log-index to HLC. The public `version` field becomes the packed HLC. Per-key history is now a point-in-time index.

**Files**:
- `crates/ggap-storage/src/keys.rs` — `history_key` now takes `commit_hlc: u64` instead of `version: u64`. Same byte layout, different semantic. Add `history_seek_le(shard_id, key, hlc) -> Vec<u8>` for "largest HLC ≤ T" lookups.
- `crates/ggap-storage/src/fjall.rs` — `apply` writes history under `commit_hlc`. `get(shard_id, key, at_version)` is renamed `get_at(shard_id, key, at_hlc)`: if `at_hlc == 0`, read `data` keyspace; else seek-le in `history`. History compaction trims oldest by HLC instead of oldest by log index — same code, different field.
- `crates/ggap-server/src/kv_service.rs` — `GetRequest.at_version` semantics now mean "at HLC"; the field name stays for proto stability. Update doc comments.
- `crates/ggap-types/src/lib.rs` — `KvEntry.version` is no longer `log_id.index`; it equals `commit_hlc` (still a `u64`). Drop the now-redundant `commit_hlc` field added in 7.2.
- `crates/ggap-server/src/watch.rs` — `WatchEvent.raft_index` keeps `log_id.index`; a new `commit_hlc` is added to the proto and wired through.

**Migration**: Phase 7 has no production deployments to migrate. The on-disk format change is acceptable; we bump a `STORAGE_FORMAT_VERSION` constant in `meta` and refuse to open older directories with a clear error.

**Tests**:
- Replace `at_version=N` MVCC tests with `at_hlc=T`. Verify "largest HLC ≤ T" semantics, including: T below any write returns `NotFound`; T at exactly a write returns it; T between writes returns the earlier one; T after all writes returns the latest.
- History compaction by HLC.
- Watch event carries both `raft_index` and `commit_hlc`.

**Done when**: `cargo test --all` green; no code references `log_id.index` as a "version" anywhere.

---

#### 7.4 — HLC propagation across requests (causal continuity)

**Goal**: a client (or another shard) that has seen HLC = X can demand that its next read on any shard observes HLC ≥ X.

**Files**:
- `crates/ggap-server/src/header.rs` (new) — gRPC metadata helpers: `extract_request_header(req) -> Option<RequestHeader>`, `inject_response_header(resp, hdr)`. Metadata keys: `ggap-observed-hlc` (binary), `ggap-min-epoch` (binary).
- `crates/ggap-server/src/kv_service.rs` — every RPC handler:
  1. On entry: `clock.observe(request_header.observed_hlc)`.
  2. On exit: set `response.header.commit_hlc = clock.now()`, `shard_map_epoch = router.shard_map().epoch()`.
- `crates/ggap-consensus/src/network.rs` — outbound `RaftService` calls (append_entries, vote, install_snapshot) carry sender's `clock.now()` in metadata; receiver `observe`s it. Already a leader→follower flow; this just makes follower clocks track leader clocks even when no entries are flowing.
- `crates/ggap-types/src/lib.rs` — `ResponseHeader` domain type gains `commit_hlc: Hlc`, `shard_map_epoch: u64`.

**Tests**:
- `causality_across_shards`: split shard 0, write key in upper half (lands on shard 1, returns `commit_hlc = T1`), then `Get` of a key in lower half (shard 0) with `observed_hlc = T1`; assert response header reports `commit_hlc ≥ T1`. (Today this would not be guaranteed because the shards' clocks are independent.)
- `lagging_follower_clock_tracks_leader`: in a 3-node cluster with no writes, leader ticks; followers' `applied_hlc_frontier` reaches leader's `now()` within one heartbeat (driven by 7.5's idle-tick).

**Done when**: any HLC observed by a client is reflected in the next request's effect; cross-shard causality is verifiable.

---

#### 7.5 — Read-wait barrier and idle-shard heartbeat

**Goal**: a shard can serve "as of T" only after applying everything ≤ T. An idle shard mustn't make readers wait forever.

**Files**:
- `crates/ggap-consensus/src/clock.rs` — flesh out `wait_until_applied`: tokio notify, registered when `target > applied_frontier`, fired when `apply` advances the frontier past any waiter.
- `crates/ggap-consensus/src/state_machine.rs` — `apply` calls `clock.set_applied(commit_hlc)` after persisting `applied_hlc_frontier`, which wakes waiters.
- `crates/ggap-consensus/src/idle_tick.rs` (new) — background task per shard: if `clock.now() - applied_frontier > config.idle_tick_ms` and this node is leader, propose `KvCommand::ClockTick { hlc: clock.now() }`. Apply path simply advances the frontier; no data side effects.
- `crates/ggap-consensus/src/state_machine.rs` — handle `KvCommand::ClockTick` (no-op except frontier advance).
- `crates/ggap-server/src/raft_service.rs` — implement `WaitApplied` RPC by calling `clock.wait_until_applied`.
- `config/default.toml` — add `[consistency] idle_tick_ms = 250`.

**Tests**:
- `wait_until_applied_quiet_shard`: 3-node cluster, no writes for 2 idle-tick periods, then a remote `WaitApplied(target = clock.now())` returns within one tick.
- `wait_until_applied_busy_shard`: with writes flowing, target = mid-write HLC; assert wait returns once that exact HLC has applied.
- `wait_until_applied_timeout`: target far in the future, deadline 100ms; returns `timed_out = true`.
- DST: idle-tick under partitioned leader (no quorum) — assert no infinite loop, no spurious commits.

**Done when**: a shard can be asked "are you caught up to T?" and answer correctly, even if it's been idle.

---

#### 7.6 — Versioned `ShardMap` with epoch capture

**Goal**: cross-shard scans need to know the routing was stable for the duration of the scan. We add an epoch counter that increments on each split apply and is captured in scan plans.

**Files**:
- `crates/ggap-storage/src/shard_map.rs` — `ShardMap` gains `epoch: AtomicU64`. Persisted under `meta_key(SHARD_MAP_PREFIX, "epoch")`. `put_shard` / `remove_shard` / `update_cache_after_split` bump the epoch atomically with the storage update.
- `crates/ggap-storage/src/fjall.rs` — Split apply writes the bumped epoch in the same atomic batch as the data move and `ShardMap` updates.
- `crates/ggap-consensus/src/router.rs` — `ShardRouter` exposes `epoch() -> u64` and `snapshot() -> ShardMapSnapshot` (immutable copy of (epoch, all_shards) for planning).
- `crates/ggap-types/src/lib.rs` — `ShardMapSnapshot { epoch, shards: Vec<ShardInfo> }` plus a `find_shards_for_range(&self, [start, end)) -> Vec<ShardInfo>`.

**Tests**:
- `shard_map_epoch_advances_on_split`: pre-split epoch = N; post-split epoch = N+1.
- `shard_map_epoch_persists_across_restart`: split, restart, assert epoch survives.
- `shard_map_snapshot_is_immutable`: take snapshot, do a split, assert the snapshot still reports the old shards/epoch.
- `find_shards_for_range_handles_gaps_and_singletons`: range fully inside one shard, range spanning two, range spanning three with a split between.

**Done when**: every routing decision can be tagged with an epoch, and stale epochs are detectable.

---

#### 7.7 — `ConsistentScan` cross-shard fan-out

**Goal**: the user-visible feature. Streaming RPC, fan-out, merge, paginate.

**Files**:
- `proto/ginnungagap/v1/kv.proto` — add `ConsistentScan` RPC, `ConsistentScanRequest`, `ConsistentScanChunk`.
- `crates/ggap-server/src/consistent_scan.rs` (new) — the coordinator:
  1. Decode `page_token` if present, else plan fresh: capture `snapshot = router.snapshot()`; pick `T = req.snapshot_hlc.unwrap_or(clock.now())`.
  2. Validate `T ≤ clock.now() + max_skew_ms` (reject far-future).
  3. `shards = snapshot.find_shards_for_range(start, end)`.
  4. For each shard: ensure `applied_hlc ≥ T`. Local shards: `clock.wait_until_applied(T)`. Remote shards: `RaftService::WaitApplied`. Bounded by `req.deadline` or 5s default.
  5. For each shard in key order: scan `[max(start, shard.range.start), min(end, shard.range.end))` at HLC `T`. Reuse existing `FjallStateMachine::scan_at(shard_id, start, end, limit, hlc)`.
  6. Merge sorted streams (k-way merge across at most ~dozens of shards in practice).
  7. Stream `ConsistentScanChunk` of `chunk_size` entries; on terminal chunk, `next_page_token` empty.
  8. On non-terminal chunk: `next_page_token = encode(snapshot_hlc=T, last_key, snapshot.epoch, shard_cursor_per_shard)`.
- `crates/ggap-storage/src/fjall.rs` — `scan_at(shard_id, start, end, limit, hlc) -> Vec<KvEntry>`: range-iterate `data` keyspace; for each key, if `entry.commit_hlc > hlc`, fall back to `history` seek-le at `hlc` for that key; collect.
- `crates/ggap-server/src/kv_service.rs` — wire the new RPC.

**Pagination correctness**: page tokens carry `(snapshot_hlc, planning_epoch, last_key, per-shard cursors)`. On token resume, the coordinator re-validates `router.epoch() == planning_epoch`. If a split has happened mid-scan, return `Status::aborted` with code `ROUTING_CHANGED` — clients re-issue the scan from `start_key = last_key` with no token. (Cross-shard scans are inherently retryable; this is acceptable for Phase 7. A stitch-across-split path is a Phase 8 polish item.)

**Tests**:
- `consistent_scan_single_shard_matches_legacy_scan`: same key range in one shard, both APIs return identical entries.
- `consistent_scan_two_shards_after_split`: split shard 0, write keys on both sides, full-range scan returns merged sorted result, all entries dated ≤ snapshot_hlc.
- `consistent_scan_excludes_writes_after_snapshot`: take `T = clock.now()`; concurrent writer adds keys with `commit_hlc > T`; scan does NOT include them.
- `consistent_scan_includes_writes_before_snapshot_on_idle_shard`: write to shard A, idle shard B, scan range covering both at `T = post-write-HLC`; B is forced to tick (via 7.5's idle-tick) and the scan completes.
- `consistent_scan_split_mid_scan_returns_aborted`: start a scan, return one chunk, split, request next chunk → `aborted` with `ROUTING_CHANGED`.
- `consistent_scan_paginates`: 10k entries, chunk_size 1000, assert all 10 chunks deliver every entry exactly once.

**Done when**: `ConsistentScan` is the documented way to scan a range that may span a split.

---

#### 7.8 — Split + HLC integration

**Goal**: HLC continuity across a split. The new shard inherits the source's HLC frontier so its first writes are ordered after every prior write to the source range.

**Files**:
- `crates/ggap-types/src/lib.rs` — `KvCommand::Split` gains `inherited_hlc_floor: u64` (the source's `applied_hlc_frontier` at the moment the leader proposes the split).
- `crates/ggap-consensus/src/split.rs` — `SplitCoordinator::do_split` snapshots `clock.applied_frontier()` and includes it in the `Split` command.
- `crates/ggap-storage/src/fjall.rs` — Split apply, in the same atomic batch:
  1. Move data (already implemented).
  2. Initialize the new shard's `applied_hlc_frontier = inherited_hlc_floor`.
  3. Bump `ShardMap.epoch` (per 7.6).
  4. Persist new shard's `last_applied = LogId::default()` and bootstrap membership (already implemented).
- `crates/ggap-consensus/src/split.rs` — `run_split_handler`, when it constructs the new shard's `OpenRaftNode`, instantiates its `HybridClock` with `last = inherited_hlc_floor` so the new shard's leader, when elected, will not stamp anything ≤ that floor.

**Why this matters**: without HLC continuity, a `ConsistentScan` at `T = source.applied_hlc_frontier` issued just after a split could observe an empty new shard even though all of its data was visible at `T-1` on the source. The inherited floor guarantees the new shard's frontier is at least `T` from creation.

**Tests**:
- `split_inherits_hlc_floor`: write keys with HLC `T1`...`T5` to source; split at key in the middle; assert new shard's `applied_hlc_frontier ≥ T5`.
- `consistent_scan_immediately_after_split`: split, scan whole range at `T = pre-split frontier`, verify all pre-split keys present on whichever shard now owns them.
- `consistent_scan_during_split_propose`: harder DST scenario — start `Split` propose, before apply, fire a scan; it must either see all pre-split state or all post-split state, never partial.

**Done when**: splits are invisible to `ConsistentScan` correctness — same answers before and after.

---

#### 7.9 — Foundation hooks for cross-shard writes (no commit logic)

**Goal**: leave behind exactly enough scaffolding that Phase 8 can implement transactional batch puts as a coordinator on top, without reshaping apply.

**Files**:
- `crates/ggap-types/src/lib.rs` — new variants:
  ```rust
  KvCommand::WriteIntent { txn_id: u128, key: String, value: Vec<u8>, ttl_ns: Option<i64>, expect_version: u64 }
  KvCommand::ResolveIntent { txn_id: u128, key: String, commit: bool }
  ```
  No reads consult intents yet; this is just a Raft-replicated record.
- `crates/ggap-storage/src/keys.rs` — new keyspace prefix `intent`: `shard ++ key ++ \x02 ++ be_u128(txn_id)`.
- `crates/ggap-storage/src/fjall.rs` — `apply` of `WriteIntent` writes the intent record (key, value, ttl, txn_id, hlc_proposed). `ResolveIntent { commit: true }` upgrades the intent to a real `Put` (writes `data` + `history`); `commit: false` just deletes the intent. Both bump `applied_hlc_frontier` normally.
- **Reads ignore intents** in this phase. `Get` at HLC T returns the latest `data` entry ≤ T, never an intent. This is correct under "intents are uncommitted" semantics; Phase 8 will add intent resolution / read-blocking.

**No new RPC.** No coordinator. No 2PC. Phase 8 owns those. The only thing this phase adds is a place to write the intent and resolve it, deterministically, through Raft.

**Tests**:
- `write_intent_apply_roundtrip`: apply `WriteIntent`, then `ResolveIntent { commit: true }`, then `Get` returns the new value.
- `write_intent_aborted`: apply `WriteIntent`, then `ResolveIntent { commit: false }`, then `Get` returns the previous value.
- `write_intent_visible_in_admin_dump`: `AdminService.list_shards()` (or a new `list_intents` debug RPC if cheap) reports the pending intent — useful for Phase 8 development without exposing it on the client API.

**Done when**: a future "transactional batch put" coordinator can be implemented entirely in `ggap-server` without touching `apply`, `state_machine`, or `fjall`.

---

### Test Strategy

Per-increment tests are listed inline above. The three cross-cutting test families that prove Phase 7 hangs together:

1. **HLC unit tests** (in `ggap-consensus/src/clock.rs`): monotonicity, observation, wait/wake, timeout, determinism under paused tokio time.

2. **DST scenarios** (extend `crates/ggap-consensus/tests/sim_cluster.rs`):
   - `dst_consistent_scan_under_partition` — split a 3-node cluster, partition one node from each shard's leader, run `ConsistentScan` against the surviving node; assert correctness or graceful timeout, never wrong data.
   - `dst_split_during_concurrent_writes` — drive writes against shard 0 while a split propose is in flight; assert every committed write either lands on the source shard's narrowed range or on the new shard, never lost, never duplicated.
   - `dst_idle_tick_under_partitioned_leader` — leader loses quorum mid-idle-tick; assert no log corruption when it rejoins.
   - `dst_cross_shard_scan_seed_replay` — same seed, same fault sequence → same scan result, byte-identical.

3. **Integration tests** (`crates/ggap-server/tests/cross_shard_scan.rs`, new file):
   - End-to-end gRPC: spin up a 3-node cluster, do a manual split, run `ConsistentScan` over the full range, validate sorted output and snapshot semantics under concurrent writers.
   - Causality check: write on shard A via `KvService`, capture `commit_hlc` from response header; subsequent `Get` on shard B with `observed_hlc = commit_hlc` reports `applied_hlc ≥ commit_hlc`.
   - Pagination round-trip across a split boundary.

A passing run of all three families is the Phase 7 acceptance bar.

### Risks and Open Questions

| Risk | Mitigation | Status |
|------|------------|--------|
| HLC clock skew between physical machines exceeds `max_skew_ms` and a `ConsistentScan` returns "future" timestamp errors | `max_skew_ms` configurable; default 500ms; document NTP requirement; metric `hlc_skew_observed_ms` | accepted, documented |
| Idle-tick proposes a `ClockTick` while the leader is partitioned, log fills with no-ops once it rejoins | Idle-tick only runs while `is_leader && lease_valid`; ticks are dedup'd by checking `applied_frontier` first | mitigated in 7.5 |
| Storage format change in 7.3 breaks restart for nodes that have written under earlier phases | We have no production data; bump `STORAGE_FORMAT_VERSION` and refuse mixed-version directories with a clear error | accepted |
| `ConsistentScan` mid-scan split forces full-scan retry — bad for very large scans | Phase 7 ships the simple "abort + retry from `last_key`" path; Phase 8 can add per-shard cursor stitching if needed | accepted |
| Watch events now carry both `raft_index` and `commit_hlc`; clients keying on one or the other may break | Both fields exist; document that `commit_hlc` is the cross-shard ordering field, `raft_index` is per-shard. Client SDK guidance updated | accepted |
| HLC stamped before `client_write` — if the proposal is rejected and re-proposed under a new leader, the original HLC is wasted | Wasted HLCs are fine (HLC is monotonic, gaps are allowed). Observed value is non-issue. | accepted |
| Read-wait deadlocks if a remote `WaitApplied` RPC is itself routed through a stalled shard | `WaitApplied` is a non-Raft RPC: it only reads the local clock, never proposes. No deadlock possible. | by design |
| `KvCommand::ClockTick` makes the Raft log grow under low write load | Tick interval is conservative (250ms default); compaction unaffected since ticks are normal log entries | accepted |

**Open questions** (resolve during implementation, not during planning):
- Should `WaitApplied` be exposed on `RaftService` (internal port) or as a method on `AdminService`? Leaning internal — it's a hot path for cross-shard reads, not an admin op.
- Should `ConsistentScan` accept a `max_staleness_ms` knob for clients that want "as fresh as possible without waiting more than X"? Yes if it's cheap; defer if the implementation is non-trivial.
- Page token format — bincode of a typed struct, or hand-rolled bytes? Bincode for speed of implementation; revisit if it becomes a stability concern.

### Phase 8 Preview (not in scope)

Once Phase 7 lands, Phase 8 can add **transactional cross-shard batch puts** as a pure server-side coordinator on top of `WriteIntent` / `ResolveIntent`. Sketch:
1. Coordinator allocates `txn_id`.
2. For each (key, value) in batch: route to its shard, propose `WriteIntent` with `expect_version`.
3. If all intents commit: propose `ResolveIntent { commit: true }` to each shard.
4. If any intent fails: propose `ResolveIntent { commit: false }` to the rest.
5. Reads consulting a key with a pending intent: either wait for resolution (strict) or skip the intent (snapshot read at HLC < intent.proposed_hlc).

Phase 7's job is to make sure step 5 has well-defined HLC semantics and that step 1's intent records are durably replicated. Nothing from this section is in scope for Phase 7.

---

## Test Summary

70 tests across all crates, all passing. Zero clippy warnings.

| Crate | Tests | Scope |
|-------|-------|-------|
| `ggap-storage` | 39 | Log storage, SM apply, MVCC, snapshot, keys, shard map |
| `ggap-consensus` | 11 | StubRaftNode (2), DST sim_cluster (7), single-node (1), raft metrics task (1) |
| `ggap-server` | 20 | 3-node cluster + admin ops (9), shard split (5), watch (3), trace propagation (2), rpc metrics (1) |

## Verification Checklist

1. `cargo test --all` — 66/66 tests pass ✅
2. Single-node smoke: `ggap-node --node-id 1 --client-addr 0.0.0.0:17000 --cluster-addr 0.0.0.0:17001`; `grpcurl` Get/Put/Delete/Scan
3. 3-node cluster: write to leader, read from followers with `SEQUENTIAL`/`EVENTUAL`; verify `raft_index` in response header ✅ (automated)
4. Consistency knobs: `quorum=ALL` write fails when one node is down; `SEQUENTIAL` read from follower returns bounded-stale data
5. MVCC: write key 3 times → `at_version=1` returns first value, `at_version=0` returns third (latest) ✅ (automated)
6. Watch: open stream, issue puts/deletes, verify ordered events with correct `watch_id`; verify `canceled=true` on lagged receiver ✅ (automated)
7. Leader failover: kill leader, verify election, client retries succeed via `ggap-leader-addr` metadata ✅ (automated via DST + 3-node tests)
