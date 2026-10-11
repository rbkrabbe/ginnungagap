+++
id = "tk-d08b"
title = "do_remove_node's two retire failure paths can now be one"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-10-11T14:51:56+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-4822"
resized_from = "m"
+++
## Context

`do_remove_node` handles a failed tombstone write differently in its two
branches (`ggap-server/src/admin_service.rs`):

- Tombstoning an unreachable node on its behalf: `warn!` and carry on, because
  the next gossip round will persist it.
- Retiring itself: `error!` with "do not restart this node under this id", and
  continue anyway.

The second wording and its severity exist because the node was about to
`exit(0)`, past a point of no return it could not report from. tk-4822 removed
the exit: the node now stays up and keeps gossiping, so its next round persists
the write exactly as the on-behalf path relies on. The reason the two differed
is gone; the code still differs.

Left out of tk-4822 on purpose — a behaviour-neutral tidy-up in a file that
change otherwise did not touch is better as its own small edit than as a
rider. Noted there under "Two things deliberately not done".

## Acceptance

- [ ] One code path handles a failed `retire` write, with a message that is
      true now that the node does not exit.
- [ ] Nothing still tells an operator not to restart a node whose only fault
      was an unpersisted tombstone it will rewrite on its next gossip round.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
