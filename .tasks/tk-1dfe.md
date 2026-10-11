+++
id = "tk-1dfe"
title = "RemoveNode still refuses when no peer heard, but a quiesced node would deliver it anyway"
kind = "task"
status = "done"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-10-11T14:59:50+00:00"
spec_approved = false
review = "pass"
touched = ["../../.claude/projects/-Users-rasmusbachkrabbe-projects-ginnungagap/memory/mutate-the-assertion-not-the-feature.md"]
discovered_from = "tk-4822"
resized_from = "m"
base = "5e81ffbc3c06c9eeace727279a9538c8b9fb10d9"
reviewed_at = "2026-10-11T19:41:13+00:00"
closed = "2026-10-11T19:44:07+00:00"
+++
## Context

`do_remove_node` refuses a self-retirement that reached no peer:

    if delivered == 0 && peers > 0 { ... "it stays in the cluster" }

The premise was that a tombstone nobody else heard would be lost when the
process ended, and the process was about to end. Neither half holds now.
tk-8c62 made the tombstone durable before `RemoveNode` answers, and tk-4822
stopped the node exiting — it quiesces and keeps gossiping, so a locally
recorded tombstone reaches peers as soon as any of them is reachable.

So the refusal is now conservative rather than necessary. That may still be
what we want: it keeps "nobody received it" meaning "it did not happen", which
is a clean contract for an irreversible operation, and it keeps the spread
attended rather than happening minutes later with nobody watching. But the
reason has changed, and the code should not keep a precondition whose stated
justification is false.

Deliberately not changed under tk-4822: loosening a precondition on an
irreversible operation is a behaviour change, not a comment fix. The comment
there now points here.

## Decision (2026-10-11)

**The refusal stays.** Two reasons apply now, neither the one in the code:

1. It keeps the removal **attended**. Without it, an operator retiring a
   partitioned node gets `ok: true` while no other node knows for minutes.
   With it, the outcome is what they see — success, or an error to retry when
   connectivity returns.
2. It **falsifies total partition** before an irreversible act. The
   precondition above it reads this node's Raft state, and a node cut off from
   every peer has by definition a possibly-stale view of whether it is still a
   voter. Requiring one peer to accept the tombstone proves this node is not
   isolated — the condition under which that view is least trustworthy.

   Reviewed wording: "corroborates the membership view" overreached, and is
   not what this does. A peer merges a tombstone blindly; it checks nothing
   about the retiring node's membership. What one acceptance establishes is
   reachability, which falsifies the worst case rather than confirming the
   read.

Rejected: recording and succeeding regardless, letting gossip carry it. It
honours the operator's intent sooner, but trades the abort-on-broken-network
window for acting on an uncorroborated local view.

## Acceptance

- [x] Decided whether the delivery precondition stays, and the comment states
      the reason that actually applies.
- [x] It stays, and `a_node_that_cannot_hand_over_its_tombstone_stays_in_the_cluster`
      now pins it at the strongest observable point — the node is still
      *serving*, not merely missing a tombstone.
- [x] Full checklist green: fmt, clippy -D warnings, build, test.

## Outcome (2026-10-11)

Done with tk-d08b; same function.

The test needed two corrections before it proved anything.

First try asserted the node still served **KV**. It fails as "not the leader"
regardless: the fixture is a drained node in no membership, so a write could
never succeed and the assertion would have passed for a reason unrelated to
retirement. Switched to Admin, which a drained node does serve.

Then the targeted mutation — refuse as now, but cancel the retire token
anyway — still **passed**, which meant the assertion was vacuous. The
harness's in-process `AdminServiceForTesting` had no retire token, so
`do_remove_node` could not reach the gate in this fixture at all: nothing the
code did could have stopped that node from serving. Fixed by giving the
harness's admin service the token, as `ggap-node` does. The mutation now fails
with the gate's own message.

I overstated the harness gap and review corrected it: only an *in-process
self*-removal needs the token, and this is the only test that makes one. Every
other `remove_node` in the file targets a different id, so it forwards to that
node's real gRPC admin, which has carried the token all along via
`build_admin`. Nothing else got weaker or stronger — verified by running the
suite with and without the harness change.

What the harness change does buy, confirmed by mutation: with the targeted
mutation applied *and* `.with_retire_token` removed, the test goes green
again. So the token is what makes the new assertion able to fail at all.

## Review 2026-10-11T19:32:08+00:00 — fail

One finding, and it is a one-line fix.

docs/ggap-consensus.md:197-200 still documents this precondition with the dead premise and, worse, presents the question this task closes as still open: '...is what makes "nobody received it" mean "it did not happen", and a node that reaches none of its peers stays in the cluster instead (tk-1dfe revisits how strict that must remain now that a retired node keeps gossiping)'. tk-1dfe is settled and being committed; a reader following that pointer finds a decided task. It is also the only place a reader learns *why* the refusal exists outside the source, and it gives neither recorded reason (attended, corroborating). CLAUDE.md allows a task id in prose only where a fix is still owed - none is. The code comment in admin_service.rs:345-360 is correct; the doc that explains it to everyone else was not swept.

Everything else checked and sound:

- Mutations verified independently, not taken on trust. (a) 'if false && delivered == 0 && peers > 0' at admin_service.rs:370 turns a_node_that_cannot_hand_over_its_tombstone_stays_in_the_cluster red at three_node_cluster.rs:2078. (b) refuse as now but cancel the retire token anyway turns it red at three_node_cluster.rs:2101, the gRPC admin assertion - so the new assertion is the thing that catches it, not the registry one. I also confirmed the harness change is load-bearing: with mutation (b) applied and .with_retire_token removed from three_node_cluster.rs:274, that test goes green again. The earlier vacuous-assertion diagnosis is correct.
- The harness token's effect on the rest of the file: no other test regressed or went vacuous, because the token only matters for an *in-process* self-removal and this is the only test that makes one. Enumerated every remove_node call in the file - lines 1666, 1897, 1954, 2019, 2132 all target an id other than the serving node, so they go through forward_removal to the target's real gRPC admin, which already carried the token from ClusterServiceConfig.retired via build_admin (lib.rs:264). a_retired_node_serves_gossip_and_refuses_everything_else cancels node.retired by hand and is untouched. start_observer (line 1382) still builds a tokenless admin and never self-removes. Full suite green with and without the change. The Outcome's claim that 'every self-retirement test before this ran against an admin service that could not trigger a retirement's visible effect' overstates it - the forwarded ones always hit a tokened service; only this one did not.
- Corroboration argument: it survives, but it is weaker than the heading claims. A peer that accepts the tombstone does not check the retiring node's membership at all - it merges it blindly - so reaching one peer falsifies 'totally partitioned' rather than corroborating the local Raft read at retire_self (admin_service.rs:321-328). The comment's body says exactly that ('proves one peer is reachable before acting irreversibly on that read'), so it is honest where it is specific; only the bolded label overreaches. Matches the recorded Decision's own wording, so not a contradiction.
- Does the refusal now do harm? No case found. peers_excluding_self excludes tombstoned ids, so retiring the last live node of a shrinking cluster hits peers == 0 and proceeds. An isolated-but-alive host is still retirable from another node through the on-behalf path. The merged-then-failed-to-answer miss leaves the node serving while peers hold its tombstone, which the comment calls out as the deliberate direction to err, and a retry succeeds since the node holds no tombstone for those peers.
- Nothing weakened or deleted. The KV->Admin switch is a strengthening, and the reason given (a drained node in no membership fails a KV write as not-the-leader either way) is right.
- Minor, not the reason for the fail: three_node_cluster.rs:2056 'Settled on tk-1dfe' is a task id pointing at history rather than at owed work, which CLAUDE.md's comment rule disallows.

Checklist re-run here: fmt clean, clippy --all-targets --all-features -D warnings clean, cargo test --all --all-features --no-fail-fast = 25 result lines, 0 failures.

## Review 2026-10-11T19:41:13+00:00 — pass

Re-review of the one finding plus the three points raised in passing.

Finding fixed. docs/ggap-consensus.md:196-205 no longer points at tk-1dfe as open and now carries both recorded reasons (attended; falsifies total partition). Checked against the source comment at admin_service.rs:345-367: same two reasons, same order, no claim in the doc the source does not make. The only remaining tk id in that section is tk-ad1d at line 208, under an explicit 'Still open' heading, and tk-ad1d is status=open - a fix still owed, which CLAUDE.md allows.

Label correction: not underclaimed. 'Falsifies total partition' is exactly what one acceptance establishes, and admin_service.rs:358-360 says outright that the peer does not verify the view and merges blindly, which is the honest limit. The three wordings agree on the claim. Noted and accepted as-is: the 'does not verify / merges blindly' caveat appears only in the source comment and the task Decision, not in the test doc (three_node_cluster.rs:2049-2061) or docs/ggap-consensus.md:200-203, so the message's 'changed in all three places' is two of three for that clause. No inconsistency results, because neither of those two ever asserts verification - they state the falsification claim and stop. Also minor and accepted: the doc's 'A node that reaches none of its peers therefore stays in the cluster' now hangs a 'therefore' off the preceding sentence about a local record spreading on its own, which under the new reasoning is the premise being set aside rather than the reason; it reads fine but the connective is softer than the logic.

Task ids pointing at history: 'Settled on tk-1dfe' is gone. Grepped every added line in the diff for tk-[0-9a-f]{4} - zero hits. Pre-existing ids in the touched files (admin_service.rs:223 tk-8d80, :249 tk-ad1d; three_node_cluster.rs:407, 420, 475, 1619, 1869) are unchanged by this diff and tk-ad1d is open and owed.

Harness-gap wording: the Outcome now records the narrower, correct version - only an in-process self-removal needs the token, this is the only such test, every other remove_node forwards to a tokened gRPC admin via build_admin - and records the with-mutation-and-without-token green run that proves .with_retire_token load-bearing.

Re-verified here rather than taken on trust, since a comment rewording touches the same file as the code: both mutations still red. (a) 'if false && delivered == 0 && peers > 0' fails at three_node_cluster.rs:2079. (b) refuse as now but cancel the retire token fails on the new gRPC admin assertion with 'this node was retired from the cluster and serves gossip only'. Tree restored byte-identical after each (git diff --stat unchanged). Checklist re-run: fmt --check clean, clippy --all-targets --all-features -D warnings clean, cargo test --all --all-features --no-fail-fast = 25 result lines, 0 failures.
