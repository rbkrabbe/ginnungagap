+++
id = "tk-1a8d"
title = "Record the derived-directory model in docs and CLAUDE.md"
kind = "task"
status = "done"
size = "s"
priority = 2
blocked_by = ["tk-11b6"]
tags = []
created = "2026-07-30T19:07:52+00:00"
spec_approved = false
review = "none"
touched = ["docs/ggap-consensus.md", "docs/ggap-types.md", "crates/ggap-types/src/lib.rs", "CLAUDE.md"]
parent = "tk-d08a"
base = "4061c944cab000799f8137e4a84a173cb0be8684"
closed = "2026-08-13T19:49:50+00:00"
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

- [x] No doc still describes `client_addr` as gossip-originated or explains the
      field-wise merge as load-bearing. `docs/ggap-consensus.md` now states the
      derived-directory model, the derived-vs-copied distinction, whole-value
      merging and the seed-peers separation.
- [x] CLAUDE.md carries the constraint that per-node addresses belong in
      membership, not gossip — phrased to generalise, since the trap is the
      *next* per-node fact someone gossips.
- [x] CLAUDE.md's Current State reflects what the directory is now, and narrows
      the known gap to placement rather than deleting it.
- [ ] ~~Markdown-only, so the checklist is skipped per CLAUDE.md.~~ Not
      markdown-only after all — see below. Checklist run and green.

## Note: not markdown-only

`NodeAddrs::cluster_only`'s doc comment said "the shape of an entry for a peer
whose client address has not reached this node yet", which is the two-feed model
this epic removed. Leaving it would have put a stale claim in the source while
the docs stated the opposite, so it is fixed here — making this a code change by
CLAUDE.md's rule. fmt, clippy `-D warnings`, build and test all run and green.

`docs/ggap-types.md` gained a `NodeAddrs` section: the advertise/bind pairing,
the membership-not-gossip rule, and what `cluster_only` means now.

Second pass caught the one that mattered most: `NodeAddrs`'s *type-level* doc
still said "an empty field means 'not known here', never 'known to be absent'".
That is the exact conflation whole-value merging removed, sitting on the type
every crate imports — a stale claim in source outranks a correct one in docs.
Grepping for the phrasing rather than the concept is what missed it the first
time; "gossip directory" appeared in three more source comments for the same
reason.
