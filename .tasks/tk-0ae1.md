+++
id = "tk-0ae1"
title = "Re-read tk-805c and tk-2cb2 against the epic's address model"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = ["tk-10d8"]
tags = []
created = "2026-08-15T10:19:15+00:00"
spec_approved = false
review = "none"
touched = []
parent = "tk-ef8d"
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
- [ ] tk-805c and tk-2cb2 re-read; either still accurate or updated.

