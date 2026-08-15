+++
id = "tk-abf8"
title = "source_members and bootstrap_members become id sets"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = ["tk-51b4"]
tags = []
created = "2026-08-15T10:19:15+00:00"
spec_approved = false
review = "none"
touched = []
parent = "tk-ef8d"
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

- [ ] A split-created shard forms on every node with no address in the log or
      the meta key.
- [ ] A restart of a split-created shard still initialises the right Raft group.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.

