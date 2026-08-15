+++
id = "tk-cc52"
title = "GgapNetwork resolves the target address per RPC through the registry"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = ["tk-c593"]
tags = []
created = "2026-08-15T10:17:29+00:00"
spec_approved = false
review = "none"
touched = []
parent = "tk-ef8d"
+++
## Context

`network.rs:52` — `new_client(&mut self, _target_id, node)` — already ignores the
target id and reads `node.cluster_addr()`, caching it in `GgapNetwork.addr`.
Invert both halves: hold the registry and the target id, resolve at send time.
Caching defeats the point, since the address changing is the feature.

Requires the registry to exist before Raft. `ggap-node/src/main.rs` builds it at
step 7b, after the router and every `OpenRaftNode`; move it above step 6.

Q3 settles what a failed resolution does.

## Acceptance

- [ ] No address is cached across RPCs; a changed directory entry is dialled on
      the next send without rebuilding the client.
- [ ] Membership still carries addresses at this point and is ignored by the
      network path — assert that, so the next task is a deletion not a fix.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.

