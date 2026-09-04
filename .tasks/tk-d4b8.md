+++
id = "tk-d4b8"
title = "merge_directory gives ties to the incoming entry, so a node can forward a stale address about itself"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-09-04T18:52:18+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-ef8d"
+++
## Context

`ShardRegistry::merge_directory` (`registry.rs:174-177`) computes `outranked`
as `existing.incarnation > incoming.incarnation`, so an entry at an *equal*
incarnation replaces the one in hand. Two consequences the epic did not intend:

- `BootCounter::advance` only warns when the counter write fails
  (`boot_counter.rs:74`), on the stated reasoning that "the self-publication
  wins ties back on the following tick". It does not — the tie goes to the
  incoming entry, and `gossip_request` re-snapshots per exchange, so between
  its own ticks the node forwards a peer's stale descriptor *about itself*.
  That is precisely the state the module doc at `boot_counter.rs:14` calls
  unrecoverable.
- Wipe-plus-move at equal rank oscillates rather than losing cleanly. tk-ef8d
  Q1 accepted that a wiped-and-moved node cannot outbid a higher incarnation;
  it did not accept a node that flips between two addresses indefinitely.
  `boot_incarnation.rs:176` pins only the strictly-higher case, so no test
  catches this.

The fix in the comparison is one character. Which way it goes is not obvious,
which is why Q1 below exists.

## Blast radius

`crates/ggap-consensus/src/registry.rs` (the comparison and its comment),
`crates/ggap-consensus/src/boot_counter.rs:74` (the reasoning in the warning,
which is wrong whichever way Q1 lands), and a case in
`crates/ggap-consensus/tests/boot_incarnation.rs` pinning equal-rank
behaviour.

## Acceptance

- [ ] Equal-incarnation merge behaviour is decided, implemented and pinned by a
      test that fails under the opposite rule.
- [ ] `boot_counter.rs:74`'s comment states a reason that is true.
- [ ] A node never republishes a peer's descriptor about itself in preference
      to its own at the same rank.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.

### Q1 [open] At an equal incarnation, which descriptor wins?
- a) Incumbent — keep what is held, so a node's own entry survives a peer's stale copy at the same rank and the tie-break is stable everywhere; a genuinely new descriptor at a reused rank (the wipe-plus-move case) is then never adopted at all
- b) Incoming, as today — last writer wins, which converges only because gossip keeps re-delivering; leaves the boot-counter warning's reasoning false and lets a node forward a peer's address about itself
- c) Incumbent, except a node's own entry always wins over any copy of itself at any rank — sole authorship becomes absolute rather than rank-ordered, but a node that cannot advance its counter can then pin a wrong address forever
> unanswered
