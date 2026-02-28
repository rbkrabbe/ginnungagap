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
service AdminService  { ClusterStatus, AddLearner, ChangeMembership, TransferLeader }
```

---

## Crate Responsibilities

### `ggap-types`
Pure domain types with no network dependency. Other crates import from here to avoid circular deps through gRPC types.
- `KvCommand`: `Put { key, value, ttl_ns, expect_version }` | `Delete { key }` | `Cas { key, expected, new_value, ttl_ns }`
- `KvResponse`: `Written { version }` | `Deleted { found }` | `CasResult { success, current }`
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

### Phase 4 — Consensus Layer
- [ ] `GgapTypeConfig`, `GgapNetworkFactory`, `GgapRaftClient`
- [ ] `RaftNode` impl: `propose`, `linearizable_read`, `sequential_read`, `eventual_read`
- [ ] `LeaseManager`
- [ ] Swap `StubRaftNode` for real `RaftNode` in `ggap-node`
- [ ] Integration test: 3-node cluster, leader election, basic CRUD

### Phase 5 — Advanced Features
- [ ] Watch fan-out (`WatchManager`, broadcast channel, TTL `EXPIRE` events)
- [ ] MVCC `at_version` reads via `history` partition
- [ ] Log compaction: snapshot trigger + install on lagging follower
- [ ] Sequential + eventual read paths tested end-to-end

### Phase 6 — Hardening
- [ ] Chaos tests: kill leader, isolate follower, restart, linearizability check
- [ ] Prometheus metrics: request rate/latency p50/p99, Raft term, commit lag, match index
- [ ] OpenTelemetry trace propagation (client → server → consensus)
- [ ] TLS/mTLS for external and internal gRPC

---

## Multi-Raft Readiness (Phase 7 — deferred)

The single-raft implementation is deliberately shaped to make multi-raft a future addition, not a rewrite.

**Already in place (Phases 1–6):**
- Storage keys are prefixed with `be_u64(shard_id)` — no data migration needed to add shards
- `RaftNode` carries a `ShardId` field — a physical node hosting multiple shards is just `HashMap<ShardId, RaftNode>`
- The external KV proto API has no shard awareness — routing stays server-side, clients are unaffected

**What Phase 7 would add:**
- `ShardMap`: tracks which `ShardId` owns which key range; stored in a dedicated `shards` fjall partition
- Routing layer in `ggap-server`: lookup `ShardId` from `ShardMap` before dispatching to `RaftNode`; return `WrongShard` error (new `GgapError` variant) with redirect hint for requests that land on the wrong node
- Placement driver (`ggap-pd` crate or external): monitors shard sizes, triggers split when a shard exceeds a threshold, assigns new shard replicas to underloaded nodes
- Shard split protocol: leader snapshots the upper half of its key range, bootstraps a new Raft group with `ShardId(new)`, atomically updates `ShardMap`
- `ggap-node` startup: reads `ShardMap` from local storage, starts one `RaftNode` per assigned shard

**What does not change in Phase 7:**
- Proto files (`kv.proto`, `cluster.proto`) — the client API is unchanged
- `ggap-storage` traits — `LogStorage` and `StateMachineStore` are already shard-scoped via the key prefix
- `RaftNode` internals — it already knows its `ShardId`; multi-raft is composition, not modification

---

## Verification Checklist

1. `cargo test -p ggap-storage` — storage unit tests pass
2. Single-node smoke: `ggap-node --node-id 1 --client-addr 0.0.0.0:17000 --cluster-addr 0.0.0.0:17001`; `grpcurl` Get/Put/Delete/Scan
3. 3-node cluster: write to leader, read from followers with `SEQUENTIAL`/`EVENTUAL`; verify `raft_index` in response header
4. Consistency knobs: `quorum=ALL` write fails when one node is down; `SEQUENTIAL` read from follower returns bounded-stale data
5. MVCC: write key 3 times → `at_version=1` returns first value, `at_version=0` returns third (latest)
6. Watch: open stream, issue puts/deletes, verify ordered events with correct `raft_index`; verify `canceled=true` on lagged receiver
7. Leader failover: kill leader, verify election, client retries succeed via `ggap-leader-addr` metadata
