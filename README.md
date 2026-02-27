# Ginnungagap

> **Experimental / AI-generated** — This project is a learning exercise built almost entirely with [Claude Code](https://claude.ai/claude-code). It is not production software. Architecture decisions, code quality, and completeness reflect an iterative AI-assisted design process rather than a hardened engineering effort.

A CP-by-default distributed key-value store in pure Rust, with a gRPC interface and Raft consensus. Named after the primordial Norse void from which order emerges.

## Goals

- **CP by default** — Raft consensus guarantees linearizable reads and writes
- **Configurable consistency** — per-request read mode (`linearizable` / `sequential` / `eventual`) and write quorum (`majority` / `all`)
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
├── ggap-storage/     # LogStorage + StateMachineStore traits + fjall impls
├── ggap-consensus/   # openraft TypeConfig, RaftNetwork, RaftNode facade
├── ggap-server/      # tonic service impls, serve_client / serve_cluster
└── ggap-node/        # binary: CLI, config loading, startup/shutdown
```

## Status

| Phase | Description | Status |
|---|---|---|
| 1 | Skeleton — workspace, protos, domain types, CLI + config | ✅ Done |
| 2 | gRPC layer — KvService wired to StubRaftNode; server reflection | ✅ Done |
| 3 | Storage — `MemLogStorage` / `MemStateMachine`, then `Fjall*` impls | Pending |
| 4 | Consensus — real `RaftNode`; swap out `StubRaftNode` | Pending |
| 5 | Advanced — Watch, MVCC reads, snapshots, TTL GC | Pending |
| 6 | Hardening — chaos tests, metrics, TLS, tracing | Pending |

Multi-raft (Phase 7) is explicitly deferred. The storage key layout and `RaftNode` type already carry a `ShardId` so it remains an additive change when the time comes.

## Running

```bash
cargo run -p ggap-node -- \
  --node-id 1 \
  --client-addr 127.0.0.1:17000 \
  --cluster-addr 127.0.0.1:17001
```

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
```

> Note: ports 7000/7001 conflict with a macOS system service; use 17000/17001.

## Configuration

Configuration is layered: embedded defaults → TOML file → `GINNUNGAGAP_*` env vars → CLI flags. See [`config/default.toml`](config/default.toml) for all knobs.

## License

MIT
