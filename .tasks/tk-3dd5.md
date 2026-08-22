+++
id = "tk-3dd5"
title = "A snapshot-recovered replica misses a split: stale shard map, missing Raft group"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-08-22T07:06:53+00:00"
spec_approved = false
review = "none"
touched = []
+++
## Context

A node that is down across a split recovers correctly *if* it replays the log:
`KvCommand::Split` goes through Raft, and `apply` (`crates/ggap-storage/src/fjall.rs:415`)
commits data movement, `last_applied`, both shard map records and
`bootstrap_members` in one `db.batch()` — all or nothing. `run_split_handler`
then bootstraps the new shard, or `main.rs` does it from `bootstrap_members`
after a restart.

It does not recover if it is far enough behind that the leader has snapshotted
past the split and purged the entry. `install_snapshot`
(`crates/ggap-storage/src/fjall.rs:954`) writes only `data`, `history`,
`ttl_index`, `last_applied` and `membership`, all for the source shard. No shard
map record, no `bootstrap_members`, no `SplitApplied` signal.

The recovered replica is then wrong in two ways, neither self-healing:

1. **Stale shard map.** Its source-shard record still claims the pre-split full
   range, while the snapshot it just installed has the upper half removed.
   `lookup_shard` routes upper-half keys to the source shard, which no longer
   holds them: reads return not-found for keys that exist, writes land in the
   wrong shard. Silent wrong answers, not an error.
2. **Missing Raft group.** It is in the new shard's `source_members`, so that
   shard's leader replicates to it — and `raft_service.rs:28` answers
   `not_found` to every AppendEntries. The new shard permanently runs a replica
   short.

Nothing re-derives the shard map from a snapshot and no retry path creates the
missing group, so both persist until an operator intervenes.

**Reachability.** `snapshot_policy` is `LogsSinceLast(50_000)` with
`max_in_snapshot_log_to_keep: 200` (`crates/ggap-consensus/src/config.rs:85`), so
the replica must miss roughly 50k entries before the split entry is purged. A
long outage on a busy shard — rare in testing, ordinary in production.

**Not a one-liner.** A snapshot carries a shard's data and says nothing about the
shard topology that produced it. See Q1.

Related: `ggap-storage/tests/split_crash_bugs.rs` is where a regression test
belongs, and it currently never runs (tk-d9a8).

## Acceptance

- [ ] A replica that recovers by snapshot across a split ends up with the same
      shard map and the same set of hosted shards as one that replayed the log.
- [ ] It serves upper-half keys correctly rather than not-found.
- [ ] It joins the new shard's Raft group, so the shard reaches full replica
      count without operator action.
- [ ] Covered by a test that recovers a replica *by snapshot*, with the split
      entry purged — not by log replay, which already works.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.

## Acceptance

- [ ] TODO

### Q1 [open] A snapshot describes one shard's data and says nothing about the topology that produced it. How does a snapshot-recovered replica learn it must narrow the source shard and host a new one?
- a) Snapshots carry the shard map — install_snapshot writes the sender's shard topology alongside the data, in the same batch. Self-contained and needs no extra round trip; grows SnapshotMeta and makes every snapshot carry cluster-wide state that only the split case reads
- b) Rebuild the shard map from Raft membership on demand — the node asks the source shard's leader for the current topology after installing a snapshot. Keeps snapshots about data only; adds a recovery-time RPC and a failure mode when the leader is unreachable mid-recovery
- c) Make the split discoverable after the fact — a periodic reconciliation compares the local shard map against the ranges the cluster reports and repairs drift. Fixes this and any future divergence, not just splits; a background repair loop is a new moving part with its own correctness burden
> unanswered
