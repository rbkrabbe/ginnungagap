+++
id = "tk-1a8d"
title = "Record the derived-directory model in docs and CLAUDE.md"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = ["tk-11b6"]
tags = []
created = "2026-07-30T19:07:52+00:00"
spec_approved = false
review = "none"
touched = []
parent = "tk-d08a"
+++
## Context

The epic changes a fact stated in several places, and a stale statement about
where addresses come from is exactly the kind that gets believed.

- `docs/ggap-consensus.md:72-90` currently explains the *two-feed asymmetry* and
  the field-wise merge rule that tk-c507 added. After this epic that text is not
  merely incomplete, it is wrong — there is one feed.
- `CLAUDE.md § Current State` describes the known gap as "no cluster-wide
  membership/placement view". Membership now carries addresses, which narrows but
  does not close that gap; state the new boundary precisely rather than deleting
  the caveat.
- `CLAUDE.md § Hard Constraints` should gain the rule that makes this stick:
  addresses live in Raft membership, and the directory is a cache of it. Without
  that line the next person to need a per-node fact will gossip it, which is the
  mistake this epic exists to undo.
- `docs/ggap-types.md` — `NodeAddrs` now lives there.

Runs in parallel with tk-c4fc.

## Acceptance

- [ ] No doc still describes `client_addr` as gossip-originated or explains the
      field-wise merge as load-bearing.
- [ ] CLAUDE.md carries the constraint that per-node addresses belong in
      membership, not gossip.
- [ ] CLAUDE.md's Current State reflects what the directory is now.
- [ ] Markdown-only, so the checklist is skipped per CLAUDE.md.
