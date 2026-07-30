+++
id = "tk-c4fc"
title = "Stamp gossiped directory entries with their membership (term, index)"
kind = "task"
status = "open"
size = "m"
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

Closes tk-1bf0, by the route the epic's Q1 chose: order copied entries by the
committed membership version they came from, rather than by an invented counter.

After tk-11b6 a node's directory entries for shards it *hosts* are derived from
membership and authoritative. Entries for shards it does not host are still
gossiped copies, and still last-write-wins — so a peer holding an older copy can
overwrite a newer one. Stamping fixes the ordering without new machinery:
`ShardEntry::incoming_wins` already leads on `term` for exactly this reason.

- `GossipNode` gains the membership `(term, index)` the addresses came from.
- `merge_directory` orders on that stamp; a derived entry always beats a copied
  one, since the local node has membership and the sender does not.
- Note the asymmetry to get right: two nodes hosting *different* shards each hold
  authoritative membership for their own, so "derived beats copied" must be
  per-entry, not per-node.

Close **tk-1bf0** when this lands — it is the same problem and must not survive
as a stale duplicate.

## Acceptance

- [ ] A stale copied entry cannot overwrite a fresher one; there is a test that
      fails without the stamp comparison.
- [ ] A derived entry is never overwritten by any copied entry.
- [ ] An address change made via `change_membership` reaches a node that hosts no
      shard in that membership, and stays changed across several gossip rounds.
- [ ] tk-1bf0 is closed with a pointer here.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
