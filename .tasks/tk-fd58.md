+++
id = "tk-fd58"
title = "add_learner stops discarding NodeInfo.client_addr"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = ["tk-441f"]
tags = []
created = "2026-07-30T19:07:52+00:00"
spec_approved = false
review = "none"
touched = []
parent = "tk-d08a"
+++
## Context

The payoff child: the first point at which a client address reaches committed
consensus state.

`admin_service.rs:134` passes `node_info.cluster_addr` into `add_learner` and
drops `node_info.client_addr` on the floor. The proto has carried both since the
beginning (`NodeInfo`, types.proto:14-18) — nothing on the wire changes here, the
server just stops discarding half of what it is given.

- `node.rs:160` — `add_learner(node_id, addr: String)` becomes
  `add_learner(node_id, addrs: NodeAddrs)`.
- `admin_service.rs:132-135` — pass both fields through.
- `main.rs` — cluster bootstrap constructs `GgapNode` with both addresses.
- Reject an `AddLearner` whose `client_addr` is empty, or accept it? An empty one
  is now a node nothing can forward to, and it was silently normal until this
  child. Decide in the task, and prefer loud.

## Acceptance

- [ ] A learner added via `AddLearner` appears in Raft membership with both
      addresses.
- [ ] With the gossip task never started, a node reports a peer's client address
      correctly from membership alone. This is the assertion the whole epic is
      for — it must not rely on gossip having run.
- [ ] An `AddLearner` carrying an empty `client_addr` behaves as the task decides,
      and a test pins that behaviour.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
