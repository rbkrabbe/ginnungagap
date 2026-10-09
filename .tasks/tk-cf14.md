+++
id = "tk-cf14"
title = "Two membership byte-scans pass because the needle is longer than the haystack"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-10-09T18:03:20+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-92e0"
resized_from = "m"
+++
## Context

Two tests assert that no address rode along into consensus by encoding
`membership_config` with bincode and scanning the bytes for each address:

- `ggap-server/tests/three_node_cluster.rs:552-577`
  (`a_restarted_member_at_a_new_address_rejoins_the_cluster`)
- `ggap-server/tests/three_node_cluster.rs:955-970`
  (`replicated_membership_carries_no_address`)

Measured while reviewing tk-92e0: the real encoded `StoredMembership` is
**13 bytes**, and an address needle like `127.0.0.1:53083` is **15**. So
`encoded.windows(15)` is an empty iterator and `.any(..)` is false for a
length reason rather than a content one.

The logic is still sound — an encoding too short to contain the needle does
not contain it — and a positive control confirms the scan bites: giving
`GgapNode` an address field and having the harness bootstrap carry it grows
the encoding and turns both assertions red. But as written neither loop
*exercises* the comparison it is built around, so a future encoding change
could leave them passing for the wrong reason without anyone noticing.

A needle that fits inside the haystack fixes it: the host alone
(`127.0.0.1`, 9 bytes) is a genuine content check at the current encoding
size, and asserting the window iterator is non-empty before trusting the
result makes the weakness impossible to reintroduce.

## Acceptance

- [ ] Both loops perform a comparison that actually runs at the current
      encoded size, not a vacuous one.
- [ ] A positive control (an address deliberately placed in membership) still
      turns both red.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
