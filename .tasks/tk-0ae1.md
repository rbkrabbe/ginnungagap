+++
id = "tk-0ae1"
title = "Re-read tk-805c and tk-2cb2 against the epic's address model"
kind = "task"
status = "done"
size = "s"
priority = 2
blocked_by = ["tk-10d8"]
tags = []
created = "2026-08-15T10:19:15+00:00"
spec_approved = false
review = "none"
touched = []
parent = "tk-ef8d"
base = "48ade17c857af87ad7f8624086e615a162b679be"
closed = "2026-09-04T18:13:15+00:00"
+++
## Context

**Half of this is done (2026-08-22).** tk-c4fc and tk-1bf0 are dropped, each with
its reasoning written on it, and tk-c4fc Q1 is recorded as moot. They were closed
early rather than at the end of the chain: sitting `open` behind five blocked
tasks meant `tk ready` kept offering superseded work as available.

What remains is the re-read. tk-805c and tk-2cb2 both consume addresses, and both
were written when membership carried them. Confirm they still say something true,
and update them where they do not.

- **tk-805c** — validates advertised addresses when a listen override is set.
  Written against addresses that entered membership; check what it is validating
  now that they enter the directory instead, and when.
- **tk-2cb2** — the `LeaderForwarder`, which resolves a leader's `client_addr`
  from the registry and caches a channel per node id. Per-node channel caching is
  the pattern tk-cc52 has to invert on the Raft path for exactly the reason the
  epic exists; check whether tk-2cb2 inherits the same defect.

## Acceptance

- [x] tk-c4fc and tk-1bf0 closed as superseded, each with reasoning.
- [x] tk-805c and tk-2cb2 re-read; either still accurate or updated.

## Outcome (2026-09-04)

- **tk-805c** — still worth doing; Q1's syntactic-only answer survives
  unchanged. Its stakes did not: the Context claimed the advertised address
  goes "into Raft membership", where a typo was unfixable. It now goes into
  this node's descriptor, so it reaches every peer's persisted directory
  (wider blast radius) but is corrected by a restart at a higher incarnation
  (no longer permanent). Rewritten to say so, and demoted from a durability
  fix to a diagnosability one. Also repaired a duplicated `## Context` and a
  stray `## Acceptance / TODO` stanza in the file.
- **tk-2cb2** — inherits the defect. Its `LeaderForwarder` cached a channel
  per node id and its Risks section blessed the resulting stale entry as
  "acceptable while addresses are StatefulSet-stable" — exactly the premise
  tk-ef8d removes. Rewritten to resolve `client_addr` on every forward and key
  the held channel by the address it was dialled at, mirroring
  `GgapNetwork::connect`/`needs_redial` from tk-cc52. Added an acceptance
  criterion that a moved target is re-dialled at its new address, and replaced
  the stale-cache risk with the one that remains: a leader that moved and has
  not republished resolves to the old address and fails fast.
- **tk-3846** (tk-2cb2's epic) — its Vocabulary called the directory "never
  persisted", false since tk-c593, and `tk show` inherits that text into every
  child. Corrected there rather than in the child.
- Discovered: tk-5a4b, two source comments that still say membership carries
  addresses.

