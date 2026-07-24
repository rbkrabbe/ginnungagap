# CLAUDE.md — Ginnungagap

Decisions anchored here to avoid re-discussion.

## Hard Constraints

- **Pure Rust only** — `fjall` for storage, never RocksDB or any C FFI crate.
- **`ggap-types` has no gRPC dependency** — all crates import domain types from here; proto types never leak inward.
- **All storage keys are prefixed with `be_u64(shard_id)`** — multi-shard is live (shards are created by splits), so this prefix is load-bearing, not a placeholder. Never remove it "for simplicity".
- **`RaftNode` always carries `ShardId`** — a node hosting multiple shards is `HashMap<ShardId, RaftNode>` (realized via `ShardRouter`). Keep `ShardId` threaded through every Raft-facing type.
- **Use `tokio::time` everywhere, never `std::time::Instant`** — `tokio::time` can be paused and advanced by simulation harnesses. Direct use of `std::time` breaks deterministic simulation testing. Applies to `TtlGcTask`, `LeaseManager`, timeouts, and any other time-dependent code.

## Tech Stack (settled)

| Concern    | Choice                          |
|-----------|---------------------------------|
| Consensus | `openraft 0.9`                  |
| Storage   | `fjall 3`                       |
| gRPC      | `tonic 0.12` + `prost 0.13`    |
| Runtime   | `tokio 1`                       |
| Config    | `figment` (TOML → env → CLI)    |
| Storage serialization | `bincode 2`         |
| Errors    | `thiserror` in libs, `anyhow` in binary |

## Crate Layout

```
ggap-proto / ggap-types / ggap-storage / ggap-consensus / ggap-server / ggap-node
```

Each crate has a `docs/<crate>.md` describing its scope.

## Pre-Push Checklist

Before every `git push` or PR creation, run all of the following and fix any errors:

```
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets
cargo test --all --quiet
```

CI enforces all four checks — a push that skips them will fail.

## Current State

The system is a working multi-shard, multi-raft KV store. **Multi-shard is a
present-day reality, not a future feature** — design new code to work for any shard, not
just `ShardId(0)`. What exists today:

- Storage is shard-prefixed (`fjall`-backed), with MVCC reads and TTL GC.
- `RaftNode` carries `ShardId`; `ShardRouter` routes per shard; `ShardMap` persists shard metadata.
- `SplitCoordinator` + `run_split_handler` create new shards via `KvCommand::Split`.
- gRPC (Kv + Admin) is shard-aware; Watch, snapshots, metrics, and tracing are wired.
- Consensus is exercised by deterministic simulation tests in `ggap-consensus/tests/`.

**Known gap:** there is no cluster-wide membership/placement view, so a node can only
report Raft status for the shards it hosts locally; `AdminService` "zeroes out" (rather
than fabricates) consensus fields for non-hosted shards. Closing this needs a gossip /
shard-registry layer — see the open issue tracking it. A placement driver (`ggap-pd`) for
automatic rebalancing is also still future work.
