# CLAUDE.md — Ginnungagap

Decisions anchored here to avoid re-discussion. Full rationale in PLAN.md.

## Hard Constraints

- **Pure Rust only** — `fjall` for storage, never RocksDB or any C FFI crate.
- **`ggap-types` has no gRPC dependency** — all crates import domain types from here; proto types never leak inward.
- **All storage keys are prefixed with `be_u64(shard_id)`** — bootstrapped at `ShardId(0)`; additional shards are created by `KvCommand::Split`. Never remove this prefix "for simplicity".
- **`RaftNode` always carries `ShardId`** — multi-raft is `HashMap<ShardId, RaftNode>`, not a rewrite.
- **Use `tokio::time` everywhere, never `std::time::Instant`** — `tokio::time` can be paused and advanced by simulation harnesses. Direct use of `std::time` breaks deterministic simulation testing (Phase 6). Applies to `TtlGcTask`, `LeaseManager`, `HybridClock`, timeouts, and any other time-dependent code.
- **HLC external consistency assumes wall-clock skew < 500 ms** across cluster nodes (Phase 7+). Operators are responsible for NTP; `hlc_skew_observed_ms` metric surfaces violations. Same caveat as CockroachDB without TrueTime.

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

See PLAN.md § Crate Responsibilities for each crate's scope.

## Pre-Push Checklist

Before every `git push` or PR creation, run all of the following and fix any errors:

```
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets
cargo test --all
```

CI enforces all four checks — a push that skips them will fail.

## Phase Discipline

Implement in phase order. Do not build Phase N+1 features while Phase N is incomplete. Within Phase 7, follow the 7.0 → 7.9 increment order in PLAN.md — each builds on the previous.

Phases:
1. Skeleton — workspace, protos, `ggap-types`, CLI + config
2. gRPC layer — service stubs wired to a `StubRaftNode`; enables `grpcurl` testing immediately
3. Storage — `Mem*` impls first, then `Fjall*`; fjall replaces mem impls in `ggap-node`
4. Consensus — real `RaftNode` impl; swap out `StubRaftNode`
5. Advanced features — Watch, MVCC reads, snapshots, TTL GC
6. Hardening — chaos tests, metrics, TLS, tracing
7. Cross-shard consistent reads — HLC, on-demand `AdvanceClock`, `ConsistentScan`, split + HLC continuity, `WriteIntent` foundation for Phase 8 (see PLAN.md § Phase 7)
