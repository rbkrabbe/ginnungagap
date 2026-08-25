+++
id = "tk-c47e"
title = "Nothing removes a departed node from the directory, and now the removal outlives a restart"
kind = "task"
status = "in_progress"
size = "m"
priority = 3
blocked_by = []
tags = []
created = "2026-08-21T21:37:16+00:00"
spec_approved = false
review = "pass"
touched = []
discovered_from = "tk-c593"
base = "bd91826c0f7f539690186149185896ea4effa49f"
reviewed_at = "2026-08-25T19:50:46+00:00"
+++
## Context

Node retirement and directory removal are an explicit tk-ef8d non-goal, which
says the problem is filed separately — but no task existed, so this is it.

Nothing removes a node from `ShardRegistry`'s directory. Every peer keeps
gossiping the entry, so a retired node stays a gossip target and a resolvable
address for as long as the cluster runs. tk-c593 persists the directory, which
sharpens it in one specific way: the entry now survives every restart of every
node that ever heard it, so there is no longer a whole-cluster bounce that
clears it. The cost is small (a failed dial per round) and it is not new, but
the escape hatch is gone.

Needs a removal rule that gossip cannot undo — a tombstone ordered by
incarnation like the descriptors themselves, rather than a delete that the next
peer to gossip resurrects. Whatever drives it (an admin RPC, or `ggap-pd`) is
part of the question.

**This now gates tk-8d80**, which is why it moved off P3. That task refuses an
`AddLearner` when the directory already maps a different id to the address being
dialled — the check that catches a node restarted under a new id at the same
address, which would otherwise become a second voter for the same process. With
no way to retract an entry, that refusal is permanent: an operator reusing a
decommissioned node's address has no remedy short of never using it again. The
alternative was a force flag whose only purpose is bypassing a safety check, and
that was rejected.

So removal is no longer only about a failed dial per gossip round. It is the
escape hatch that lets a safety check refuse without becoming a trap.

## Acceptance

- [x] A removed entry stays removed: no peer's gossip resurrects it, including a
      peer that was partitioned for the whole removal.
- [x] Removal survives a restart of every node, since the directory is persisted.
- [x] An address freed by a removal can be reused by a different node id.
- [x] Full checklist green: fmt, clippy -D warnings, build, test.

### Q1 [answered 2026-08-25T19:18:59+00:00] What drives a directory removal?
- a) New AdminService.RemoveNode RPC — any node accepts it, writes a tombstone, gossip carries it; ggap-pd can call the same RPC later
- b) Piggyback on ChangeMembership — no new RPC, but a drained-before-replacement node is retired by accident
- c) Wait for ggap-pd — defines the type and merge rule but leaves tk-8d80 with no operator remedy
> New AdminService.RemoveNode RPC. Any node accepts it, writes a tombstone into its own directory, and gossip carries it everywhere. Self-contained and testable now, and it gives tk-8d80's reverse-address refusal the escape hatch it depends on; ggap-pd calls the same RPC later.

### Q2 [answered 2026-08-25T19:18:59+00:00] How does a tombstone rank against a live descriptor for the same id?
- a) Absolute — outranks every live descriptor at any incarnation, forever; the id is retired for the cluster's lifetime and reusing the hardware needs a fresh node id
- b) Incarnation-ordered — tombstone at highest-known+1, so a peer holding an unseen higher copy resurrects the entry, contradicting the first acceptance criterion
- c) Absolute plus an explicit un-remove RPC at a tombstone-generation counter — recovers from operator error, costs a second ordering dimension
> Absolute: a tombstone outranks every live descriptor for that id at any incarnation, forever. Unresurrectable by a peer holding a copy nobody else saw and by the node itself restarting. The id is retired for the cluster's lifetime — reusing the hardware means a fresh node id, which already matches the data-dir-wipe rule.

### Q3 [answered 2026-08-25T19:24:50+00:00] Should RemoveNode refuse a node still present in a shard's membership?
- a) Refuse, naming the shard ids — best-effort over gossiped entries, catches the obvious mistake
- b) Remove unconditionally — the directory is an address cache, not consensus
> Self-addressed, with a dial-fallback for a node that does not answer. RemoveNode reaches any node, which forwards it to the target; the target checks its own memberships authoritatively, refuses if it still hosts a shard, otherwise authors its own tombstone, gossips it to at least one peer and exits. Only when the target does not answer does the receiving node write the tombstone on its behalf. A follow-up tracks descriptor freshness so that fallback can later be gated on the node being demonstrably dead rather than merely unreachable right now.

## Review 2026-08-25T19:50:46+00:00 — pass

Read the full diff against bd91826 plus the two new task files. Verified checklist myself: fmt --check clean, clippy --all-targets --all-features -D warnings clean, build clean, cargo test --all --all-features green (all suites, 0 failures).

Spec/decisions: matches Q1 (new AdminService.RemoveNode, tombstone gossiped), Q2 (absolute ordering — merge_directory registry.rs:158-176 has Removed win in both directions, nothing else writes the directory: every other path, incl. add_learner's hint at node.rs:194 and network.rs, goes through merge_directory), Q3 (forward-to-target, target checks its own membership from cluster_status voters+learners, hands the tombstone over via broadcast_removal *before* retire() locally, on-behalf fallback only when the target does not answer, follow-up filed as tk-ad1d).

Test quality checked by mutation, not by reading: (1) letting a descriptor overwrite a tombstone in merge_directory fails registry::a_tombstone_beats_every_descriptor_for_its_id, directory_persistence::a_tombstone_survives_a_restart_and_the_gossip_that_follows_it and three_node_cluster::a_drained_node_retires_itself_and_the_cluster_forgets_it; (2) removing the still-a-member guard (admin_service.rs:296) fails remove_node_is_refused_while_the_target_is_still_a_member; (3) removing the delivered==0 guard (admin_service.rs:312) fails a_node_that_cannot_hand_over_its_tombstone_stays_in_the_cluster. Files restored afterwards; tree byte-identical (1117 insertions).

No test was deleted, skipped or loosened; the one dropped assertion (gossip.rs incarnation==0) is subsumed by the whole-value assert that replaced it. All callers of the changed signatures updated (serve_cluster/serve_cluster_with_listener -> ClusterServiceConfig in main.rs, benches/kv_write, cors_preflight, rpc_metrics, trace_propagation, three_node_cluster; DirectoryStore save/load and node_to_proto/node_from_proto call sites).

Accepted as-is, with reservations worth knowing:
- registry.rs node_id_at() has no production caller; its doc says 'the join path uses it', which is only true after tk-8d80. Unused public API + a present-tense comment that is currently false. Fix the comment when tk-8d80 lands.
- admin_service.rs:238 forward_removal has no self-address guard and no hop limit (gossip.rs:191 has the equivalent guard). A directory entry pointing an id at the serving node's own address — reachable via an AddLearner hint with a mistyped cluster_addr — makes RemoveNode recurse into itself until the 2s timeout collapses it. Bounded, but noisy.
- Half-delivered removal: if a peer merges the tombstone but the response is lost, broadcast_removal counts 0, the target refuses with 'stays in the cluster', yet the cluster holds the tombstone. Operator sees a refusal for a node that is in fact retired. Inherent to at-most-once RPC; the opposite ordering is worse.
- main.rs:449 std::process::exit(0) 500ms after shutdown.cancel() skips trace_guard.shutdown() and the FjallStore drop, and races the graceful path's own exit code. A retired node is in no membership by construction, so the data at risk is dispensable.
- A node tombstoned on its behalf while merely partitioned keeps running as a zombie until it is restarted; the boot refusal (main.rs:275) catches it only on the next start, once the gossip task has persisted the tombstone it learned about itself. tk-ad1d is the right home for that.
- The persisted directory format changed (NodeDescriptor -> DirectoryEntry). Old records fail to decode; load() degrades to empty as documented, but BootCounter::try_load treats an undecodable record as an unrankable boot, so an upgrade over a corrupt counter would bail. Pre-existing behaviour, no migration expected at this stage.
- Tooling, not code: .tasks/tk-ad1d.md and .tasks/tk-9a52.md each have a duplicated '## Context' heading and a trailing '## Acceptance / - [ ] TODO' stub from the tk template. Also the tk-c47e acceptance boxes are still all unticked.
