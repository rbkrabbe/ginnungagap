# CLAUDE.md — Ginnungagap

Decisions anchored here to avoid re-discussion.

## Hard Constraints

- **Pure Rust only** — `fjall` for storage, never RocksDB or any C FFI crate.
- **`ggap-types` has no gRPC dependency** — all crates import domain types from here; proto types never leak inward.
- **All keys in shard-scoped keyspaces are prefixed with `be_u64(shard_id)`** — multi-shard is live (shards are created by splits), so this prefix is load-bearing, not a placeholder. Never remove it "for simplicity". The `node` keyspace is the single exception: it holds state describing *this node* rather than any shard (the shard map, the persisted directory, the boot counter), keyed by bare label. A fact with no shard belongs there, never under a fake shard id.
- **`RaftNode` always carries `ShardId`** — a node hosting multiple shards is `HashMap<ShardId, RaftNode>` (realized via `ShardRouter`). Keep `ShardId` threaded through every Raft-facing type.
- **Consensus carries identity; the directory resolves it** — membership is a
  set of node ids (`GgapNode` is empty), and every address is resolved through
  `ShardRegistry`'s directory at send time. The reason is multi-raft: with
  addresses in membership, moving one node means a committed membership change
  in every shard it hosts. Each node publishes its own descriptor, ordered by an
  incarnation from a persisted boot counter, and gossip carries it; no node
  authors another's address. A new per-node fact belongs in the descriptor, not
  in membership.
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
cargo test --all --all-features --quiet
```

CI enforces all four checks — a push that skips them will fail.

`--all-features` is load-bearing on the clippy and test lines: `test-utils` is
the only feature in the workspace, and `split_crash_bugs` is
`required-features = ["test-utils"]`. Without it cargo skips that target
silently — no skip line, no warning, an all-green run. `cargo build` stays
feature-free so the crash-injection helpers never reach a production build.

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

- Node addresses live in `ShardRegistry`'s directory, which every node publishes
  its own entry into and gossip carries everywhere. Membership holds ids alone,
  in bootstrap, `AddLearner` and a split's `source_members` alike. The directory
  is persisted to the `node` keyspace and restored at startup, so a restart
  resolves peers without waiting to be dialled; a corrupt record starts the node
  empty, since peers re-seed it within a gossip round.
  A node publishes its own descriptor at an incarnation taken from a boot
  counter in the same keyspace, incremented each start, so a restart at a new
  address outranks every copy of the old one. A node that starts *below* the
  rank its peers hold can never win back authorship of its own address, so an
  unusable counter recovers its rank from the persisted directory's self-entry
  and fails the boot if that is unreadable too. Wiping the data dir loses both
  records: an address change made across a wipe needs a fresh node id.
  `GgapNetwork` resolves its target through the directory on **every** RPC, so a
  node that moves is dialled on the next send with no new client; an id the
  directory cannot resolve fails the RPC, which openraft treats as an
  unreachable peer and retries. The registry is therefore built before any Raft
  group in `ggap-node/src/main.rs`. `AddLearner` carries the joining node's
  addresses over the wire — nothing could dial it otherwise — and writes them
  into the leader's directory at incarnation 0, which the node's own first
  publication supersedes.

**Known gap:** *placement* has no cluster-wide view. Which node hosts which
shard is eventually consistent: `AdminService` answers for a non-hosted shard
from gossiped `ShardEntry` copies, ageing rather than fabricating, and zeroes out
consensus fields when it has never heard of a shard at all. Addresses are
eventually consistent too, by design — a node that moves is reachable once its
descriptor has propagated, and nothing removes a departed node's entry (tk-c47e).
A placement driver (`ggap-pd`) for automatic rebalancing is future work.
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
