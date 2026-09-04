+++
id = "tk-5a4b"
title = "Two comments still say membership carries addresses"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-09-04T18:12:44+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-0ae1"
resized_from = "m"
+++
## Context

Two comments survived tk-51b4 and tk-abf8 and now describe a system that does
not exist. Both are load-bearing for a reader trying to learn the address
model from the code.

- `crates/ggap-consensus/src/network.rs:56` — `new_client`'s doc comment says
  "Membership still carries addresses, but the network path takes none of
  them". `GgapNode` is `{}` since tk-51b4; membership carries no addresses at
  all. The second half of the sentence is the whole truth now.
- `crates/ggap-node/src/main.rs:192-195` — "Needed before any Raft group
  starts, because seed bootstrap puts the advertised address into the initial
  membership". tk-abf8 made `bootstrap_members` an id set. `self_addrs` is
  still needed before the registry is built, but for the descriptor this node
  publishes, not for membership.

Comments only; no behaviour changes.

## Acceptance

- [ ] Neither comment claims membership carries an address.
- [ ] `main.rs`'s comment states the real reason `self_addrs` is built early.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
