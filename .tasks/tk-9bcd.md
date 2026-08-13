+++
id = "tk-9bcd"
title = "No CLI flag populates ShardRegistry seed_peers"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-08-02T19:33:13+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-11b6"
+++
## Context

tk-11b6 gave `ShardRegistry::new` a `seed_peers: (node_id, cluster_addr)`
parameter — bootstrap gossip targets that are deliberately kept out of the
directory. `main.rs` passes `[]`. The only caller that passes anything is the
observer harness in `ggap-server/tests/three_node_cluster.rs`.

That is correct for every node reachable today: a node joins by `AddLearner`
from a running member, and the membership it then replicates tells it about the
whole shard, so it needs no dial hint. But it means the mechanism exists with no
production entry point, which is the kind of thing that rots.

Two ways out, and the choice is not obvious:

- Add a `--seed-peers` (or `--join`) CLI flag and wire it through. Buys a node
  that hosts no shard the ability to observe the cluster — which is exactly the
  drained/awaiting-placement case tk-7664 punted to `ggap-pd`, and exactly what
  the observer harness simulates. Under that reading this is a `ggap-pd`
  prerequisite, not a loose end.
- Delete `seed_peers` and let the test harness reach in some other way. Smaller,
  and honest about the fact that nothing needs it yet.

Do not decide this without the `ggap-pd` placement story in view; picking the
second option and then needing the first is the worse order.

## Acceptance

- [ ] Either `seed_peers` has a production entry point, or it is gone.
- [ ] If it stays, a node started with only a seed and hosting no shard is
      covered by a test that does not construct `ShardRegistry` by hand.
