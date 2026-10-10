+++
id = "tk-d2bb"
title = "The retired-node boot guard is inline in main() and so has no test"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-10-10T15:42:37+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-8c62"
resized_from = "m"
+++
## Context

`ggap-node/src/main.rs` refuses to start a node whose own entry in the
persisted directory is a tombstone: a retired id is never reused, so a node
that came up anyway would run as one the cluster has already forgotten. It is
the reason tk-8c62 made `retire` write the tombstone through.

Nothing tests it. The check is inline in `main()`, between loading the
directory and merging it into the registry, so reaching it means starting the
binary. tk-8c62 could pin the record the guard reads — the tombstone is on
disk before `RemoveNode` answers — but not the guard's own behaviour.

"A decision inline in `main()` cannot be reached by a test" is how this was
first written, and it is too quick. `ggap-node` has a bin target, so a test
under `crates/ggap-node/tests/` gets `env!("CARGO_BIN_EXE_ggap-node")`: it can
write a tombstoned directory record into a temp data dir, run the binary, and
assert a non-zero exit and the "retired id is never reused" message. That
covers the guard *as an operator meets it*, including the exit status, which an
extracted function would not.

So there are two routes, and they are not equivalent:

- **Extract the decision** into a function over the loaded directory and this
  node's id. Fast, deterministic, and shares the remedy with [[tk-ae09]],
  which needs the same treatment for the three bootstrap-membership branches a
  few lines below. Does not test that `main` acts on the answer.
- **Drive the binary.** Slower and coarser, but it is the only thing that
  proves the node actually refuses to start, and it opens the same door for
  every other startup refusal (`tk-ae09`'s decode failure, the boot counter's
  two).

Worth deciding which, or both, when this is picked up. The extraction pairs
naturally with tk-ae09; the binary harness is the more valuable of the two on
its own and nothing else in the repo has one yet.

## Acceptance

- [ ] Tests cover: a self-tombstone refuses the boot; a tombstone for another
      node does not; no entry at all does not.
- [ ] Whichever route is taken, the refusal is observed rather than inferred —
      if the decision is extracted, `main` acting on it is still unproven, so
      say so on this task rather than ticking the box past it.
- [ ] The refusal still names the node id and says a fresh one is needed.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
