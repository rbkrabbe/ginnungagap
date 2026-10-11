+++
id = "tk-d08b"
title = "do_remove_node's two retire failure paths can now be one"
kind = "task"
status = "done"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-10-11T14:51:56+00:00"
spec_approved = false
review = "pass"
touched = []
discovered_from = "tk-4822"
resized_from = "m"
reviewed_at = "2026-10-11T19:31:44+00:00"
base = "d83b5fa0d36ced3b2877e606d9a03f58686c8ded"
closed = "2026-10-11T19:44:07+00:00"
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

- [x] One code path handles a failed `retire` write, with a message that is
      true now that the node does not exit — `retire_locally`, called from
      both sites.
- [x] Nothing still tells an operator not to restart a node whose only fault
      was an unpersisted tombstone it will rewrite on its next gossip round.
- [x] Full checklist green: fmt, clippy -D warnings, build, test.

## Outcome (2026-10-11)

Done with tk-1dfe; same function. The two branches had already converged on
behaviour when tk-4822 removed the exit — this only removed the duplicate
code and the two different messages for one outcome. `retire_locally` states
the reason once: the node doing the writing keeps running either way, so its
next gossip round persists the directory.

## Review 2026-10-11T19:31:44+00:00 — pass

Extraction verified faithful. Both former branches warned-and-continued after tk-4822 removed the exit; retire_locally (admin_service.rs:300) keeps that exact behaviour and the on-behalf path's wording, which is the one true for both callers now that neither caller exits. Checked no caller lost anything: the only two call sites are admin_service.rs:256 (on-behalf, inside the forward_removal Err arm, still answers ok:true/confirmed_by_node:false) and admin_service.rs:384 (self, past the delivery gate). Grepped the tree: nothing still tells an operator not to restart a node over an unpersisted tombstone, so acceptance 2 holds. Full checklist re-run here: fmt clean, clippy --all-targets --all-features -D warnings clean, cargo test --all --all-features --no-fail-fast = 25 result lines, 0 failures. Accepted as-is: the helper's doc sentence about retire writing through is background rather than a statement about this function, but it is true.
