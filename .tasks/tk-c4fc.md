+++
id = "tk-c4fc"
title = "Stamp gossiped directory entries with their membership (term, index)"
kind = "task"
status = "dropped"
size = "m"
priority = 2
blocked_by = ["tk-11b6"]
tags = []
created = "2026-07-30T19:07:52+00:00"
spec_approved = false
review = "none"
touched = []
parent = "tk-d08a"
base = "4061c944cab000799f8137e4a84a173cb0be8684"
+++
## Context

## Dropped 2026-08-22 — superseded by tk-ef8d

The `(term, index)` stamp is not built, and will not be: epic tk-ef8d removes
addresses from Raft membership entirely, so there is no membership version for a
gossiped directory entry to be stamped with.

This task's own Q1 is what produced that epic. Asking how to order two derived
entries whose stamps come from unrelated Raft groups exposed the real defect —
not that the stamp was missing, but that a node-scoped fact was being ordered by
N shard-scoped clocks. tk-ef8d's answer is one clock per node: the node is the
sole author of its own descriptor and stamps it with a persisted boot counter.
Q1 is recorded above as moot rather than answered.

What replaces it, both shipped:

- **tk-8978** — `merge_directory` orders by incarnation, highest wins.
- **tk-98e9** — the boot counter that makes the incarnation actually advance
  across a restart.

Closing this early, ahead of tk-0ae1, which was going to do it at the end of the
tk-ef8d chain: leaving it `open` for five more tasks meant `tk ready` could offer
it as live work. tk-0ae1 keeps the remaining half of its scope (re-reading
tk-805c and tk-2cb2).

Everything below is the pre-supersession draft, kept for provenance.

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

### Q1 [answered 2026-08-22T10:04:42+00:00] A (term, index) stamp is only comparable within one Raft group. After a split the new shard inherits the source's member set (split.rs:121-145,194), so a node derives entries for the same peer from several shards' memberships in a single refresh_local tick — stamps from unrelated clocks. How should two derived entries for the same node be ordered?
- a) Derived entries are all equally authoritative; stamp orders copied-vs-copied only — simplest, keeps one entry per node; a peer in two shards can flap between their views while a change_membership is applied shard-by-shard
- b) Key the directory by (node_id, shard_id) so stamps are only ever compared within a shard — exact, no incomparable comparisons; changes the directory shape and every reader (admin_service, forwarder tk-2cb2) must pick a shard or reconcile
- c) Compare within a shard, deterministic tiebreak across shards (lowest shard_id wins) — convergent and cheap; can pin a stale address when the tiebreak shard lags behind a shard that already applied the change
> Moot — tk-ef8d deletes the premise. The question was how to order two derived entries whose (term, index) stamps come from unrelated Raft groups; the epic's answer is that no entry is derived from membership at all. A node is the sole author of its own descriptor and orders it with a node-scoped boot counter, so there is one clock per node instead of N per node, and cross-shard comparability stops being a question rather than getting an answer.

None of options (a), (b) or (c) is chosen. (b) — keying the directory by (node_id, shard_id) — is worth remembering as the shape this would have needed had the stamp survived: it was the only exact option, and its cost (every reader picks a shard or reconciles) is a good measure of what the stamp was going to cost overall.

Recorded per tk-0ae1.
