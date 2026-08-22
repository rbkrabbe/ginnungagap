# CLAUDE.md — Ginnungagap

Decisions anchored here to avoid re-discussion.

## Hard Constraints

- **Pure Rust only** — `fjall` for storage, never RocksDB or any C FFI crate.
- **`ggap-types` has no gRPC dependency** — all crates import domain types from here; proto types never leak inward.
- **All storage keys are prefixed with `be_u64(shard_id)`** — multi-shard is live (shards are created by splits), so this prefix is load-bearing, not a placeholder. Never remove it "for simplicity".
- **`RaftNode` always carries `ShardId`** — a node hosting multiple shards is `HashMap<ShardId, RaftNode>` (realized via `ShardRouter`). Keep `ShardId` threaded through every Raft-facing type.
- **Per-node addresses live in Raft membership, never in gossip** — both
  addresses ride inside `GgapNode`, so a change is an ordered, committed
  `change_membership`. `ShardRegistry`'s directory is a *cache* of that, derived
  from `raft.metrics()`; no node originates its own entry. A new per-node fact
  belongs in membership too — gossiping one reintroduces the races this replaced.
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

## Comments

State what a reader needs to understand the code as it stands. No historical
context — no "used to", no "before tk-xxxx", no narration of what changed. The
rationale behind a decision belongs in the task file, which `tk show` surfaces.
A task id is welcome where a fix is still owed (e.g. "`bootstrap_members` is
cluster-only (tk-10b7)") — that points at work, not at history.

Design docs under `docs/` may carry more context than source comments, but not
a diff's worth of justification.

## Pre-Push Checklist

Before every `git push` or PR creation **that touches code**, run all of the
following and fix any errors:

```
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets
cargo test --all --quiet
```

CI enforces all four checks — a push that skips them will fail.

A change that touches no code — only `.tasks/*.md` or other markdown — skips
the checklist. CI applies the same rule and reports success without running
them, so `ci` stays usable as a required status check. If a change mixes code
and task files, it touches code: run all four.

## Current State

The system is a working multi-shard, multi-raft KV store. **Multi-shard is a
present-day reality, not a future feature** — design new code to work for any shard, not
just `ShardId(0)`. What exists today:

- Storage is shard-prefixed (`fjall`-backed), with MVCC reads and TTL GC.
- `RaftNode` carries `ShardId`; `ShardRouter` routes per shard; `ShardMap` persists shard metadata.
- `SplitCoordinator` + `run_split_handler` create new shards via `KvCommand::Split`.
- gRPC (Kv + Admin) is shard-aware; Watch, snapshots, metrics, and tracing are wired.
- Consensus is exercised by deterministic simulation tests in `ggap-consensus/tests/`.

- Node addresses are carried by Raft membership; the directory in `ShardRegistry`
  is derived from it and cached, and gossip only copies entries between nodes
  that share no shard. The directory is also persisted to the `meta` keyspace
  and restored at startup, so a restart resolves peers without waiting to be
  dialled; it is a cache, so a corrupt record starts the node empty.

**Known gap:** *placement* has no cluster-wide view. Addresses are solved —
membership carries them, so any node reports both for every peer in a shard it
hosts, with the gossip task stopped. What is still eventually-consistent is
which node hosts which shard: `AdminService` answers for a non-hosted shard from
gossiped `ShardEntry` copies, ageing rather than fabricating, and zeroes out
consensus fields when it has never heard of a shard at all. Copied *directory*
entries are still last-write-wins (tk-c4fc adds the membership `(term, index)`
stamp). A placement driver (`ggap-pd`) for automatic rebalancing is future work.
# Task tracking

All work is tracked in `.tasks/` via the `tk` CLI. This file is loaded into every
session, so it holds only what is true every time. Procedures live in skills.

- Start work by running `tk ready`, then `tk claim <id>`. Hooks will refuse edits
  to guarded source paths until a task is claimed.
- Never invent or guess a task id. Ids come from `tk ready`, `tk list` or `tk new`.
- A request to build something is not yet a task. Size it first with the
  **size-the-work** skill, even when it is phrased as small or urgent. If it is
  days of work rather than hours, use **plan-an-epic** instead.
- Shared design belongs on the epic, not copied into its children — `tk show`
  gives a child its epic's design, non-goals, vocabulary and settled decisions.
  Anything still true after the epic ships belongs in this file instead.
- When a choice has two defensible answers and outlives the code, it is the
  human's: `tk ask <id> "<question>" -o "<option — consequence>" -o "..."`. Record
  their answer with `tk answer`. Do not proceed on an assumption you could check.
- File work you discover instead of doing it inline:
  `tk new "title" --discovered-from <current-id>`. Small and specific beats big.
- Tasks sized `l` need a human-approved spec before they can be claimed. Use the
  **spec-a-task** skill. You may not run `tk approve-spec` — that is the human's.
- Tasks sized `m` or `l` need a passing adversarial review before commit. Dispatch
  the `adversarial-reviewer` subagent, then commit.
- Commit `.tasks/*.md` in the same commit as the code it describes, so reverting
  the code reverts the task state with it.
- If a review comes back `fail`, fix the code. Do not re-run the reviewer hoping
  for a different verdict.
