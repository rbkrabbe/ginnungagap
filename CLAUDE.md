# CLAUDE.md — Ginnungagap

Decisions anchored here to avoid re-discussion. Full rationale in PLAN.md.

## Hard Constraints

- **Pure Rust only** — `fjall` for storage, never RocksDB or any C FFI crate.
- **`ggap-types` has no gRPC dependency** — all crates import domain types from here; proto types never leak inward.
- **All storage keys are prefixed with `be_u64(shard_id)`** — always `ShardId(0)` in Phases 1–6. Never remove this prefix "for simplicity"; it makes Phase 7 multi-raft additive.
- **`RaftNode` always carries `ShardId`** — multi-raft is `HashMap<ShardId, RaftNode>`, not a rewrite.

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

## Phase Discipline

Implement in phase order (1 → 6). Do not build Phase N+1 features while Phase N is incomplete. Phase 7 (multi-raft) is explicitly deferred.

Phases:
1. Skeleton — workspace, protos, `ggap-types`, CLI + config
2. gRPC layer — service stubs wired to a `StubRaftNode`; enables `grpcurl` testing immediately
3. Storage — `Mem*` impls first, then `Fjall*`; fjall replaces mem impls in `ggap-node`
4. Consensus — real `RaftNode` impl; swap out `StubRaftNode`
5. Advanced features — Watch, MVCC reads, snapshots, TTL GC
6. Hardening — chaos tests, metrics, TLS, tracing
