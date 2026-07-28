+++
id = "tk-3cd2"
title = "NotLeader carries the leader's node id, not just an address"
kind = "task"
status = "in_progress"
size = "l"
priority = 2
blocked_by = []
tags = []
created = "2026-07-25T10:14:05+00:00"
spec_approved = true
review = "pass"
touched = ["crates/ggap-types/src/lib.rs", "crates/ggap-consensus/src/node.rs", "crates/ggap-consensus/src/split.rs", "crates/ggap-server/src/convert.rs", "crates/ggap-server/tests/three_node_cluster.rs", "docs/ggap-consensus.md", "docs/ggap-types.md"]
parent = "tk-3846"
resized_from = "m"
reviewed_at = "2026-07-28T15:30:55+00:00"
spec_approved_at = "2026-07-28T15:22:26+00:00"
closed = "2026-07-28T15:35:23+00:00"
base = "39d8fee97c407147d349571d8439678d3b6fee0a"
+++
## Context

## Problem

GgapError::NotLeader carries only Option<String> — the address openraft embedded
in the ForwardToLeader error. A forwarder needs the leader's *identity* so it can
look the current address up in the gossip directory, rather than trusting an
address that was correct when the error was constructed. Without this, forwarding
would dial a stale addr with no way to notice.

## Changes

- crates/ggap-types/src/lib.rs:188 — NotLeader gains `leader_id: Option<u64>`
  alongside the existing `leader: Option<String>`.
- crates/ggap-consensus/src/node.rs — the four forward_to_leader() call sites
  (:107 ensure_linearizable_or_lease, :157 add_learner, :185 change_membership,
  :221 propose) populate leader_id from `fwd.leader_id`.
- crates/ggap-consensus/src/split.rs:150 — same, if it constructs NotLeader.
- crates/ggap-server/src/convert.rs:46 — emit the id as a `ggap-leader-id`
  metadata entry beside the existing ggap-leader-addr, so the hint is complete
  for callers that can act on it.

## Acceptance

- [x] NotLeader carries both leader_id and leader; all five construction sites set both.
- [x] ggap_to_status emits ggap-leader-id alongside ggap-leader-addr.
- [x] Existing follower tests in three_node_cluster.rs assert the id is present
      and matches the address the same response carries.
- [x] Full checklist green: fmt, clippy -D warnings, build, test.

## Risks

Every match on GgapError::NotLeader must be updated — grep says convert.rs is the
only consumer today, but the struct variant means a missed site is a compile
error, not a silent bug.

## Outcome

Sized `s` originally; the scope hook caught that the spec above already
enumerates five files, so it was resized to `m` (`resized_from = "s"`). No scope
was absorbed — the label was just wrong.

All five sites had the same five-line mapping, so it lives in one place now:
`node::not_leader(&ForwardToLeader<u64, BasicNode>)` (pub(crate), used by
split.rs too). Future construction sites should call it rather than rebuilding
the struct, so the two halves cannot drift apart.

The two metadata entries are emitted independently, not as a pair. openraft can
report a `leader_id` with no `leader_node`, and a forwarder that resolves ids
through gossip can act on the id alone — so gating the id on the address being
present would have thrown away the more useful half. Covered by
`convert::tests::not_leader_emits_id_without_addr`.

`assert_leader_hint_consistent` in three_node_cluster.rs checks the two halves
*agree* (look up the node named by leader_id, compare its cluster_addr to the
reported addr) rather than pinning them to whichever node was leader when the
test started — the leader can move mid-test, which would make the stricter
assertion flaky.

Other consumer of the hint, left alone deliberately: `benches/kv_write.rs:254`
reads `ggap-leader-addr` to redirect the load generator. It has no gossip
directory, so dialing the address directly is the right behaviour there, and the
address is still emitted unchanged.

### Post-review fixes (review 1 came back `fail`)

Both findings were correct and are fixed:

1. `change_membership_on_follower_returns_not_leader` had been left on the old
   `matches!` assertion while its two siblings were converted. It is the only
   coverage of the `change_membership` construction site, so `leader_id` being
   populated there was asserted nowhere. Now uses
   `assert_leader_hint_consistent`, like the other two.
2. `docs/ggap-consensus.md` and `docs/ggap-types.md` both stated the old
   `NotLeader { leader }` signature as normative contract. Updated to the
   two-field form, including which half is stable and which may be stale.

Fixing the docs took the task from 6 to 7 files, over the `m` limit — resized to
`l` (`resized_from` chain: s -> m -> l). The docs were not separable: they
specify the exact type this task changes, and shipping them stale was itself a
review finding.

## Review 2026-07-28T15:19:02+00:00 — fail

Checked: all NotLeader construction now funnels through the single pub(crate) not_leader() helper (ggap-consensus/src/node.rs:27); grep confirms no other construction site in the workspace, and the three matches!() consumers in ggap-server/src/admin_service.rs:143,173,203 are variant-only so they still compile and behave. Metadata key ggap-leader-id matches the existing ggap-shard-id/ggap-leader-addr convention. leader_id and leader both come from the same ForwardToLeader value so they cannot disagree at the source, and the new test helper is strictly stronger than the assertions it replaced (let-else + expect on both halves + cross-check of id->cluster_addr against TestCluster). Ran fmt --check, clippy --all-targets --all-features -D warnings, and cargo test --all: all green, including the three follower NotLeader tests.

Findings:

1. crates/ggap-server/tests/three_node_cluster.rs:805-808 — change_membership_on_follower_returns_not_leader still asserts only matches!(err, GgapError::NotLeader { .. }) and never looks at leader_id, even though assert_leader_hint_consistent is defined 160 lines above and the other two follower tests were converted. Acceptance bullet 3 says the existing follower tests assert the id is present and matches the address; this one does not. It is the only coverage of the change_membership call site (node.rs:188), so leader_id being populated there is asserted nowhere. Replace the matches! block with assert_leader_hint_consistent(&err, &cluster).

2. docs/ggap-consensus.md:30-31 — normative contract text still reads 'propose must return Err(GgapError::NotLeader { leader: Option<String> })', which is now wrong; and docs/ggap-types.md:66 still documents the variant as 'NotLeader { leader }' with no mention of leader_id or of the id being the stable, gossip-resolvable half. Both need updating alongside the type change.

Accepted as-is: unticked Acceptance boxes in .tasks/tk-3cd2.md (nothing falsely claimed), the propose() (node.rs:221) and split.rs:150 sites having no leader_id assertion — a missed site there is a compile error, not a silent bug, per the task's own Risks section, and the helper is shared.

## Review 2026-07-28T15:30:55+00:00 — pass

Re-review after review-1 fixes.

Finding 1 (change_membership test) verified fixed and equivalent, not weaker: three_node_cluster.rs:805 now calls assert_leader_hint_consistent(&err, &cluster), byte-for-byte the same call the two sibling follower tests make (:662, :690). The helper (:626-645) is strictly stronger than every assertion it replaced — let-else on the variant, expect() on leader_id and leader, plus a cross-check that the node named by leader_id is the one whose cluster_addr equals the reported address. That cross-check has teeth: a leader_id sourced from anything other than fwd.leader_id (e.g. self.node_id) would mismatch the addr and fail. Not pinning to the leader observed at test start is the right call given mid-test elections; cluster_status's current_leader is the only stronger option and it is racy for the same reason. All 12 three_node_cluster tests pass, including change_membership_on_follower_returns_not_leader — so the change_membership site at node.rs:188 now has real leader_id coverage.

Finding 2 (docs) verified fixed and accurate. docs/ggap-consensus.md:30-37 and docs/ggap-types.md:66 both state the two-field form and are correct about which half is which: node ids are config-assigned and stable, and ShardRegistry (registry.rs:52) is a gossiped node_id -> cluster_addr directory, so 'resolve leader_id through the gossip directory' describes a directory that actually exists. The 'leader may be stale' claim matches the code: that string comes from BasicNode.addr in the Raft membership snapshot (node.rs:30), which is the same address family the registry holds but is only refreshed by membership changes, so the registry entry can be newer. 'Either half may be absent' matches the Option/Option signature and is exercised by convert::tests::not_leader_emits_id_without_addr and not_leader_with_no_hint_emits_no_metadata. The consensus doc's pointer to node::not_leader() is fine despite pub(crate): the RaftNode impls it addresses all live in ggap-consensus.

Whole-diff re-check: no other NotLeader construction site in the workspace (grep across .rs/.md/.proto); the three matches!() consumers in admin_service.rs:143,173,203 are variant-only and unaffected; benches/kv_write.rs:254 still reads ggap-leader-addr, which is emitted unchanged; GgapError has no Serialize derive and is not persisted, so the added field and the changed Display string break no wire or on-disk format, and nothing parses that message. No test was deleted, skipped or loosened — the three converted assertions all gained coverage. Metadata emission order (id before addr) is independent per half, matching the recorded Outcome rationale.

Ran: cargo fmt --all --check (clean), cargo clippy --all-targets --all-features -D warnings (clean), cargo test --all (all suites green, 0 failures).

Accepted as-is: propose() (node.rs:221) and split.rs:150 still have no direct leader_id assertion — they share the same helper and a missed site is a compile error, per the task's Risks section; docs/ggap-server.md:52 still says 'UNAVAILABLE (with leader hint in message)' without naming either metadata key, but that was already incomplete before this change and is not made staler by it.
