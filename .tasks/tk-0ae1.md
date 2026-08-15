+++
id = "tk-0ae1"
title = "Close out tk-c4fc, tk-1bf0 and the superseded ordering work"
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

Both are premised on ordering *copies of membership*, which this epic deletes as
a concept. They are superseded, not completed — close them saying so, with a
pointer here, rather than silently dropping them.

- tk-c4fc: the `(term, index)` stamp. Its open Q1 (cross-shard comparability) is
  the question that produced this epic; record that outcome on it.
- tk-1bf0: closed by the incarnation rule from the first child, not by a stamp.

Also re-read tk-805c and tk-2cb2, which both consume addresses, and confirm they
still say something true.

## Acceptance

- [ ] tk-c4fc and tk-1bf0 closed as superseded, each with reasoning.
- [ ] tk-805c and tk-2cb2 re-read; either still accurate or updated.

