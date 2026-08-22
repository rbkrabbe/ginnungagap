+++
id = "tk-1bf0"
title = "Directory entries have no version, so a changed address may not converge"
kind = "task"
status = "dropped"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-07-30T17:57:10+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-c507"
+++
## Context

## Dropped 2026-08-22 — the defect is fixed, by neither route proposed here

**The bug described below is closed.** `ShardRegistry::merge_directory` is no
longer last-write-wins: it orders by incarnation, highest wins (tk-8978), and
tk-98e9 made the incarnation a boot counter persisted in the data dir, so it
actually advances across a restart. A stale copy can no longer overwrite a fresh
one, and the node that owns an address outranks every copy of it in flight.

Dropped rather than done because neither fix written here is what shipped. The
origin-stamped counter in the original text was superseded by tk-c4fc's
membership `(term, index)` stamp, and that in turn was superseded by tk-ef8d,
which removes addresses from membership so there is no stamp to take. tk-c4fc is
dropped alongside this.

Do not re-file this defect. If a stale address does overwrite a fresh one now,
it is a bug in the incarnation rule (tk-8978) or the counter (tk-98e9), not a
missing version.

Closed early, ahead of tk-0ae1, so `tk ready` stops offering it as live work.

Everything below is the pre-supersession draft, kept for provenance.

Found by adversarial review of tk-c507.

`ShardRegistry::merge_directory` is last-write-wins with no version or timestamp on directory entries. That is fine for address *discovery* — the case the directory was built for — but not for address *change*.

If a node's address changes, peers holding the old value keep echoing it back through gossip. There is no monotonicity rule that makes the newer value win, so a stale address can overwrite a fresh one on any node that has already learned the new one. The origin node repairs its own entry every tick via the self-seed in `refresh_local`, but peers can circulate stale copies among themselves indefinitely. Convergence happens in practice by fanout, not by construction.

Not a regression: the pre-tk-c507 directory was equally version-free. It matters more now for two reasons — `client_addr` has only one originator (Raft membership re-derives `cluster_addr` every tick, which masks the problem for that field), and tk-2cb2 will dial these addresses and cache a channel per node id.

`ShardEntry` already solves the same problem for shard records with the (term, version, leader-origin, last_applied, origin_id) rule in `incoming_wins`. The directory needs something analogous — probably an origin-stamped counter, since there is no term to lean on.

Worth doing before or alongside tk-2cb2, since a forwarder dialling a stale address is the visible symptom.

## Superseded, pending approval of tk-d08a (2026-07-30)

**tk-c4fc** fixes this, by a better route than the origin-stamped counter
suggested above: once Raft membership carries both addresses (epic tk-d08a),
gossiped directory entries can be stamped with the membership `(term, index)`
they came from. That reuses Raft's clock instead of inventing a second one that
must be persisted and must never regress.

Left open deliberately rather than dropped: tk-d08a is not approved yet, and this
is a real defect in code that ships today. If the epic is approved, close this
with a pointer to tk-c4fc. If it is not, implement this as originally written.

**Do not work both.** Whichever starts first, close the other.

## Acceptance

Superseded — see tk-c4fc. If implemented standalone instead:

- [ ] A stale directory entry cannot overwrite a fresher one.
- [ ] The stamp survives a node restart without regressing.
