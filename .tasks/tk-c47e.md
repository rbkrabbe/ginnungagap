+++
id = "tk-c47e"
title = "Nothing removes a departed node from the directory, and now the removal outlives a restart"
kind = "task"
status = "open"
size = "m"
priority = 3
blocked_by = []
tags = []
created = "2026-08-21T21:37:16+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-c593"
+++
## Context

Node retirement and directory removal are an explicit tk-ef8d non-goal, which
says the problem is filed separately — but no task existed, so this is it.

Nothing removes a node from `ShardRegistry`'s directory. Every peer keeps
gossiping the entry, so a retired node stays a gossip target and a resolvable
address for as long as the cluster runs. tk-c593 persists the directory, which
sharpens it in one specific way: the entry now survives every restart of every
node that ever heard it, so there is no longer a whole-cluster bounce that
clears it. The cost is small (a failed dial per round) and it is not new, but
the escape hatch is gone.

Needs a removal rule that gossip cannot undo — a tombstone ordered by
incarnation like the descriptors themselves, rather than a delete that the next
peer to gossip resurrects. Whatever drives it (an admin RPC, or `ggap-pd`) is
part of the question.

**This now gates tk-8d80**, which is why it moved off P3. That task refuses an
`AddLearner` when the directory already maps a different id to the address being
dialled — the check that catches a node restarted under a new id at the same
address, which would otherwise become a second voter for the same process. With
no way to retract an entry, that refusal is permanent: an operator reusing a
decommissioned node's address has no remedy short of never using it again. The
alternative was a force flag whose only purpose is bypassing a safety check, and
that was rejected.

So removal is no longer only about a failed dial per gossip round. It is the
escape hatch that lets a safety check refuse without becoming a trap.

## Acceptance

- [ ] A removed entry stays removed: no peer's gossip resurrects it, including a
      peer that was partitioned for the whole removal.
- [ ] Removal survives a restart of every node, since the directory is persisted.
- [ ] An address freed by a removal can be reused by a different node id.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
