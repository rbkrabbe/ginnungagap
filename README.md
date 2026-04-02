# Ginnungagap

> **Experimental / AI-generated** — This project is a learning exercise built almost entirely with [Claude Code](https://claude.ai/claude-code). It is not production software. Architecture decisions, code quality, and completeness reflect an iterative AI-assisted design process rather than a hardened engineering effort.

A CP-by-default distributed key-value store in pure Rust, with a gRPC interface, Raft consensus, and multi-raft range sharding. Named after the primordial Norse void from which order emerges.

## Goals

- **CP by default** — Raft consensus guarantees linearizable reads and writes
- **Configurable consistency** — per-request read mode (`linearizable` / `sequential` / `eventual`) and write quorum (`majority` / `all`)
- **Multi-raft sharding** — independent Raft group per shard with manual range splitting
- **MVCC** — per-key version history with configurable depth and point-in-time reads
- **Pure Rust** — no C FFI; `fjall` for storage instead of RocksDB

## Tech Stack

| Concern | Choice |
|---|---|
| Consensus | `openraft 0.9` |
| Storage | `fjall 3` |
| gRPC | `tonic 0.12` + `prost 0.13` |
| Runtime | `tokio 1` |
| Config | `figment` (TOML → env → CLI) |
| Serialization | `bincode 2` |

## Crate Layout

```
crates/
├── ggap-proto/       # build.rs + generated tonic/prost code
├── ggap-types/       # domain types, KvCommand, GgapError — no gRPC dep
├── ggap-storage/     # LogStorage + StateMachineStore traits, fjall impls, ShardMap
├── ggap-consensus/   # openraft TypeConfig, RaftNetwork, ShardRouter, SplitCoordinator
├── ggap-server/      # tonic service impls (KvService, RaftService, AdminService)
└── ggap-node/        # binary: CLI, config loading, multi-shard startup/shutdown
```

## Status

| Phase | Description | Status |
|---|---|---|
| 1 | Skeleton — workspace, protos, domain types, CLI + config | Done |
| 2 | gRPC layer — KvService, RaftService, AdminService with server reflection | Done |
| 3 | Storage — `MemLogStorage` / `MemStateMachine`, then `Fjall*` impls | Done |
| 4 | Consensus — real `OpenRaftNode`; swap out `StubRaftNode` | Done |
| 5 | Advanced — Watch, MVCC reads, snapshots, TTL GC | Pending |
| 6 | Hardening — chaos tests, metrics, TLS, tracing | Pending |
| 7 | Multi-raft — shard routing, manual range splitting | Done |

## Multi-Raft Architecture

Each shard is an independent Raft group with its own log, state machine, and leader election. Key components:

- **ShardMap** — persistent shard-to-key-range mapping stored in fjall. Loaded into an in-memory `BTreeMap` on startup.
- **ShardRouter** — routes `key -> ShardId -> RaftNode` for reads/writes. Blocks writes to shards in `Splitting` state. Rejects scans that span multiple shards.
- **SplitCoordinator** — executes a 9-step split protocol that blocks writes on the source shard during the split to guarantee consistency:
  1. Validate the split request
  2. Mark source shard as `Splitting`
  3. Write barrier (linearizable no-op)
  4. Build partial snapshot of keys >= split_key
  5. Allocate new shard ID
  6. Install partial snapshot on new shard
  7. Delete transferred keys from source
  8. Update ShardMap with new ranges
  9. Bootstrap new Raft group and resume writes

All storage keys are prefixed with `be_u64(shard_id)`, so splitting is a metadata + data copy operation with no key re-encoding.

### Limitations

- **Manual splits only** — no automatic load-based or size-based splitting
- **Single-shard scans** — scans that span shard boundaries are rejected; clients must issue per-shard scans
- **Writes blocked during split** — the source shard is unavailable for writes while the split executes

## Building

```bash
cargo build --release -p ggap-node
# binary at: target/release/ggap-node
```

## Running

### Single node

```bash
ggap-node \
  --node-id 1 \
  --client-addr 127.0.0.1:17000 \
  --cluster-addr 127.0.0.1:17001 \
  --data-dir /tmp/ggap-node1
```

### Multi-node cluster

A production-grade Raft cluster runs 3 or 5 nodes (odd count ensures a majority quorum can always form). Each node requires a unique `--node-id`, `--client-addr` (gRPC for clients), `--cluster-addr` (internal Raft RPC), and `--data-dir`.

**Terminal 1 — node 1 (bootstraps as initial leader)**

```bash
ggap-node \
  --node-id 1 \
  --client-addr 127.0.0.1:17000 \
  --cluster-addr 127.0.0.1:17001 \
  --data-dir /tmp/ggap-node1
```

Node 1 initialises a single-member Raft group on first boot and immediately becomes leader.

**Terminal 2 — node 2**

```bash
ggap-node \
  --node-id 2 \
  --client-addr 127.0.0.1:17010 \
  --cluster-addr 127.0.0.1:17011 \
  --data-dir /tmp/ggap-node2
```

**Terminal 3 — node 3**

```bash
ggap-node \
  --node-id 3 \
  --client-addr 127.0.0.1:17020 \
  --cluster-addr 127.0.0.1:17021 \
  --data-dir /tmp/ggap-node3
```

**Add nodes to the cluster**

Once all three processes are running, register nodes 2 and 3 as learners by calling the leader's cluster port (17001):

```bash
# Add node 2 as a learner
grpcurl -plaintext \
  -d '{"node":{"node_id":2,"client_addr":"127.0.0.1:17010","cluster_addr":"127.0.0.1:17011"}}' \
  localhost:17001 ginnungagap.v1.AdminService/AddLearner

# Add node 3 as a learner
grpcurl -plaintext \
  -d '{"node":{"node_id":3,"client_addr":"127.0.0.1:17020","cluster_addr":"127.0.0.1:17021"}}' \
  localhost:17001 ginnungagap.v1.AdminService/AddLearner
```

Then promote all three to voting members:

```bash
grpcurl -plaintext \
  -d '{"node_ids":[1,2,3]}' \
  localhost:17001 ginnungagap.v1.AdminService/ChangeMembership
```

> **Note:** `AddLearner` and `ChangeMembership` currently return `UNIMPLEMENTED` — the gRPC membership-management handlers are not yet wired up. Once complete, the flow above will be the standard way to grow or shrink the cluster without a restart.

With [grpcurl](https://github.com/fullstorydev/grpcurl) (server reflection is enabled, no `--proto` needed):

```bash
# List services
grpcurl -plaintext localhost:17000 list

# Put
grpcurl -plaintext \
  -d '{"key":"hello","value":"d29ybGQ="}' \
  localhost:17000 ginnungagap.v1.KvService/Put

# Get
grpcurl -plaintext \
  -d '{"key":"hello"}' \
  localhost:17000 ginnungagap.v1.KvService/Get

# Split shard 0 at key "m"
grpcurl -plaintext \
  -d '{"shard_id":0,"split_key":"m"}' \
  localhost:17001 ginnungagap.v1.AdminService/SplitShard

# List all shards
grpcurl -plaintext \
  localhost:17001 ginnungagap.v1.AdminService/ListShards
```

> Note: ports 7000/7001 conflict with a macOS system service; use 17000/17001.

## Testing

```bash
cargo test --workspace    # 45 tests across all crates
```

Key test suites:
- `ggap-storage` — 35 unit tests covering log storage, state machine, snapshots, key encoding, and shard map
- `ggap-consensus` — single-node Raft smoke test
- `ggap-server` — 3-node cluster integration (leader election, failover) and single-node split tests (data partitioning, post-split routing, cascading splits)

## Configuration

Configuration is layered: embedded defaults → TOML file → `GINNUNGAGAP_*` env vars → CLI flags. See [`config/default.toml`](config/default.toml) for all knobs.

## CI

GitHub Actions runs `cargo fmt --check`, `cargo clippy`, and `cargo test` on every pull request.

## License

MIT
