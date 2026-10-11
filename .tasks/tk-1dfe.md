+++
id = "tk-1dfe"
title = "RemoveNode still refuses when no peer heard, but a quiesced node would deliver it anyway"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-10-11T14:59:50+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-4822"
resized_from = "m"
+++
## Context

`do_remove_node` refuses a self-retirement that reached no peer:

    if delivered == 0 && peers > 0 { ... "it stays in the cluster" }

The premise was that a tombstone nobody else heard would be lost when the
process ended, and the process was about to end. Neither half holds now.
tk-8c62 made the tombstone durable before `RemoveNode` answers, and tk-4822
stopped the node exiting — it quiesces and keeps gossiping, so a locally
recorded tombstone reaches peers as soon as any of them is reachable.

So the refusal is now conservative rather than necessary. That may still be
what we want: it keeps "nobody received it" meaning "it did not happen", which
is a clean contract for an irreversible operation, and it keeps the spread
attended rather than happening minutes later with nobody watching. But the
reason has changed, and the code should not keep a precondition whose stated
justification is false.

Deliberately not changed under tk-4822: loosening a precondition on an
irreversible operation is a behaviour change, not a comment fix. The comment
there now points here.

## Acceptance

- [ ] Decided whether the delivery precondition stays, and the comment states
      the reason that actually applies.
- [ ] If it stays, a test pins it for the current reason rather than the old
      one; if it goes, the tombstone's propagation through gossip is what the
      test asserts instead.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
