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

## Acceptance

- [ ] TODO
