+++
id = "tk-ae09"
title = "Nothing tests ggap-node's startup membership initialisation"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-08-23T17:47:35+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-abf8"
resized_from = "m"
+++
## Context

`ggap-node/src/main.rs` decides a shard's initial Raft membership three ways:
from a decoded `bootstrap_members` id set (split-created shard), from `--seed`
(single-voter bootstrap), or not at all (a non-seed fresh node waits to be
dialled). None of the three is exercised by a test — nothing drives the binary's
startup path, so the logic is only ever run by hand or in a deployment.

tk-abf8 changed the decode under it (`BTreeMap<u64, NodeAddrs>` ->
`BTreeSet<u64>`) and could tick its restart criterion only partially for this
reason: the storage layer proves what is written, and the split tests pin the
format, but the step that turns those ids into membership is unverified.

The obstacle is that the logic lives inline in `main()`. Extracting it into a
function that takes the meta bytes and the seed flag and returns
`Option<BTreeMap<u64, GgapNode>>` would make all three branches testable without
starting a node.

## Acceptance

- [ ] The three bootstrap branches are decided by a testable function, not
      inline in `main()`.
- [ ] A test covers each: decoded id set, seed, and neither.
- [ ] A `bootstrap_members` record that fails to decode still fails the boot
      loudly rather than silently starting an uninitialised shard.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
