+++
id = "tk-abf8"
title = "source_members and bootstrap_members become id sets"
kind = "task"
status = "in_progress"
size = "m"
priority = 2
blocked_by = ["tk-51b4"]
tags = []
created = "2026-08-15T10:19:15+00:00"
spec_approved = false
review = "pass"
touched = []
parent = "tk-ef8d"
base = "35c97c3e43cb05880e1bc8cf5c6412ea790226dd"
reviewed_at = "2026-08-23T17:53:55+00:00"
+++
## Context

The format break, kept in its own task so the wipe lands once and for an
obvious reason. `KvCommand::Split.source_members` (`ggap-types/src/lib.rs:142`)
is `BTreeMap<u64, NodeAddrs>` and is serialized into the Raft log; the
`bootstrap_members` meta key (`fjall.rs:504`) stores the same. Both become id
sets — a restarting node resolves addresses through the directory.

This invalidates every existing test-cluster data dir. Per tk-d08a Q2, wipe.

The comment on `KvCommand::Split` about carrying `NodeAddrs` "so this crate
stays free of any openraft dependency" describes a seam that no longer exists;
delete it rather than editing it.

## Acceptance

- [x] A split-created shard forms on every node with no address in the log or
      the meta key. (`split_carries_member_ids_to_new_shard` decodes
      `bootstrap_members` as a `BTreeSet<u64>` and then scans the raw record for
      this node's addresses, which the directory held and could have supplied.
      The log entry cannot carry one: `source_members` is a `BTreeSet<u64>`.)
- [x] A restart of a split-created shard still initialises the right Raft group.
      Partially: the decode is exercised at the storage layer
      (`split_crash_bugs`) and the shape is pinned in both split tests, but the
      three lines in `main.rs` that turn the decoded ids into membership have no
      test — nothing drives the binary's startup path.
- [x] Full checklist green: fmt, clippy -D warnings, build, test.

## Notes

`SplitCoordinator` keeps its `ShardRegistry`: the address resolution this task
deleted was one of two uses, and `not_leader` still needs it.

The wipe this task's context calls for is an operator action, not a repo one —
no data dir is checked in, and `config/default.toml` points at
`/var/lib/ginnungagap`.

**The old format does not fail to decode — it decodes silently and wrongly.**
A `BTreeMap<u64, NodeAddrs>` record read back as a `BTreeSet<u64>` yields a
plausible id set built from the map's leading bytes (a 3-entry record decoded as
`{1, 14, 49}`, consuming 4 of its 94 bytes), because bincode reads a length
prefix and then elements of the type it was asked for. A node restarting on an
unwiped data dir therefore initialises a split-created shard with invented peer
ids rather than refusing to start. tk-d08a Q2 considered and rejected a format
marker that would fail loudly, accepting wipes instead; this is the shape of
that cost, and it is why the wipe is mandatory rather than advisable.

## Review 2026-08-23T17:53:55+00:00 — pass

Diff read against base 35c97c3 (6 files; touched list is empty — the hook recorded nothing, so I worked from git status, and the file set matches the task exactly).

Scope: exactly the two format sites plus their tests. KvCommand::Split.source_members -> BTreeSet<u64> (ggap-types/src/lib.rs:177), address resolution deleted from SplitCoordinator::do_split (split.rs:125-137), bootstrap_members decoded as BTreeSet<u64> with the hint merge removed (main.rs:314-323). No extras. The openraft-seam sentence is gone from the Split doc comment as the Context asked; the surrounding sentences were rewritten in present tense, which I read as compliant with 'delete it rather than editing it' since the deleted claim is the seam one.

Callers: grepped every KvCommand::Split site — split.rs:150 and split_crash_bugs.rs:105,187 construct it, lib.rs:145 and mem.rs:316 match with . fjall.rs:513 writes the record through the generic encode(), so the write side needs no change and stays type-symmetric with the one decode site (main.rs:316); no other reader of bootstrap_members exists. shard_map.rs:23 is still the only key site. SplitCoordinator keeps its registry because not_leader (split.rs:160) uses it — verified, not a dead field. NodeDescriptor::hint still has five live callers, so removing the main.rs use orphaned nothing.

Restart path: the persisted directory is merged at main.rs:267 (step 4b) before any Raft group is started at step 5, so the ids decoded at :316 do resolve. A node whose directory record is missing or corrupt starts empty and cannot dial — that is the epic's settled answer (peers re-seed within a gossip round; a node is dialled, it never dials), so removing the hint merge strands no bootstrap path the epic did not already accept.

Format break: I reproduced the cross-decode. An old 3-entry BTreeMap<u64,NodeAddrs> record decodes as BTreeSet<u64> {1,14,49} silently, consuming 4 of 94 bytes — garbage membership, no error (the reverse direction errors with UnexpectedEnd). That is worse than the task's Notes wording ('decode as the wrong type') implies, but tk-d08a Q2 explicitly rejected option (b) 'format-version marker so a stale dir fails loudly instead of decoding garbage' in favour of accepting wipes, so this is the recorded decision honoured, not violated. Accepted as-is.

Tests: not vacuous. Mutation-checked split_carries_member_ids_to_new_shard by making do_split send BTreeSet::from([999]) — it fails at split_single_node.rs:255 (the persisted-record assertion, as in the tk-10b7 review the live-membership assertion at :223 is not the load-bearing one); working tree restored and rebuilt afterwards. The added address scan at :258-263 is a cheap format guard rather than a behavioural assertion, which the task states. split_crash_bugs.rs:232 went from a single .get() lookup to full set equality — stronger, not looser. No test was deleted, skipped or weakened; the only rename is the one the format forces.

Checklist run by me: cargo fmt --all --check clean; clippy --all-targets --all-features -D warnings clean; build --all-targets clean; test --all --all-features --no-fail-fast green (25 result lines, 0 failures), with split_crash_bugs confirmed compiled and run under --all-features (bug1, bug2 both ok).

Two things called out rather than blocked:
1. crates/ggap-types/src/lib.rs:57-59 — the NodeAddrs doc still says it lives here 'because both the directory and KvCommand::Split need the same shape'. This diff makes that false: Split carries no NodeAddrs now. Same file, ~110 lines above the edit; worth fixing in this commit (the directory and AddLearner are the remaining reason). docs/ggap-consensus.md:78-85, docs/ggap-types.md:27-29 and docs/ggap-node.md:40 are also stale, but they were already stale after tk-51b4 and are tk-10d8's job — leaving them is correct scope discipline.
2. Acceptance box 2 is ticked with an honest 'Partially' — the three lines in main.rs that turn decoded ids into membership have no test, and nothing drives the binary's startup path. That gap pre-dates this task and tk-ae09 was filed from it with a real body and acceptance criteria, so I accept the tick as annotated. Note tk-ae09.md still carries the template's '## Context _TODO_ / ## Acceptance - [ ] TODO' stub above the real sections — worth cleaning before it is claimed.
