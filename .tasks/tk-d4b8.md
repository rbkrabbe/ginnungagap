+++
id = "tk-d4b8"
title = "merge_directory gives ties to the incoming entry, so a node can forward a stale address about itself"
kind = "task"
status = "done"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-09-04T18:52:18+00:00"
spec_approved = false
review = "pass"
touched = ["CLAUDE.md"]
discovered_from = "tk-ef8d"
base = "4c36e59fa32d2876ea5392602bd36fc3cf68ddd9"
reviewed_at = "2026-10-09T20:34:51+00:00"
closed = "2026-10-09T20:35:37+00:00"
+++
## Context

`ShardRegistry::merge_directory` (`registry.rs:174-177`) computes `outranked`
as `existing.incarnation > incoming.incarnation`, so an entry at an *equal*
incarnation replaces the one in hand. Two consequences the epic did not intend:

- `BootCounter::advance` only warns when the counter write fails
  (`boot_counter.rs:74`), on the stated reasoning that "the self-publication
  wins ties back on the following tick". It does not — the tie goes to the
  incoming entry, and `gossip_request` re-snapshots per exchange, so between
  its own ticks the node forwards a peer's stale descriptor *about itself*.
  That is precisely the state the module doc at `boot_counter.rs:14` calls
  unrecoverable.
- Wipe-plus-move at equal rank oscillates rather than losing cleanly. tk-ef8d
  Q1 accepted that a wiped-and-moved node cannot outbid a higher incarnation;
  it did not accept a node that flips between two addresses indefinitely.
  `boot_incarnation.rs:176` pins only the strictly-higher case, so no test
  catches this.

The fix in the comparison is one character. Which way it goes is not obvious,
which is why Q1 below exists.

## Confirmed from tk-92e0 (2026-10-09)

The tie rule actively masks a dead boot counter, which is stronger than the
"between its own ticks" window described above. tk-92e0 restarts a member of a
live three-node cluster at a new address; with the boot counter mutated to
return a constant 1 — i.e. never advancing — the move still succeeds and the
test still passes. At equal rank the new descriptor wins on arrival, so
nothing anywhere observes that the counter is dead.

Whichever way Q1 lands, that is the case to pin: tk-92e0 cannot assert its own
acceptance criterion "fails if the boot counter stops advancing" until ties
stop going to the incoming entry.

## Blast radius

`crates/ggap-consensus/src/registry.rs` (the comparison and its comment),
`crates/ggap-consensus/src/boot_counter.rs:74` (the reasoning in the warning,
which is wrong whichever way Q1 lands), and a case in
`crates/ggap-consensus/tests/boot_incarnation.rs` pinning equal-rank
behaviour.

## Acceptance

- [x] Equal-incarnation merge behaviour is decided, implemented and pinned by a
      test that fails under the opposite rule.
- [x] `boot_counter.rs:74`'s comment states a reason that is true — the
      warning is gone; the write is fatal and says why.
- [x] A node never republishes a peer's descriptor about itself in preference
      to its own at the same rank.
- [x] Full checklist green: fmt, clippy -D warnings, build, test.

### Q1 [answered 2026-10-09T19:27:05+00:00] At an equal incarnation, which descriptor wins?
- a) Incumbent — keep what is held, so a node's own entry survives a peer's stale copy at the same rank and the tie-break is stable everywhere; a genuinely new descriptor at a reused rank (the wipe-plus-move case) is then never adopted at all
- b) Incoming, as today — last writer wins, which converges only because gossip keeps re-delivering; leaves the boot-counter warning's reasoning false and lets a node forward a peer's address about itself
- c) Incumbent, except a node's own entry always wins over any copy of itself at any rank — sole authorship becomes absolute rather than rank-ordered, but a node that cannot advance its counter can then pin a wrong address forever
> Fix the generator, and let ties lose as a consequence. BootCounter::advance must not return a rank it cannot guarantee is unique to this boot: a counter write it cannot persist becomes fatal, the same posture the module already takes for a rank it cannot establish. With ranks unique per boot, a tie can only hold identical content, so merge_directory's comparison becomes >= (the incumbent keeps its place) and a stale forwarded copy can no longer displace a current entry. Rejected: incumbent-wins alone, which is worse than today — main.rs merges the persisted directory before the first self-publication, so a node's own fresh address would lose to its own stale entry at a tied rank and never install. Also rejected: making authorship absolute for an entry received from its subject, which fixes the comparator but leaves the non-unique rank in place and makes convergence depend on direct contact.

## Outcome (2026-10-09)

Both halves of the answer, as one change:

- `BootCounter::advance` now fails when it cannot persist the rank, instead of
  warning and returning it. A rank is unique to its boot again, which is the
  premise the tie rule needs. Reached in a test through an injected write
  failure, armable only under `cfg(test)` / `test-utils`, mirroring
  `arm_crash_after_phase1`.
- `merge_directory` compares `>=`, so an equal rank keeps the entry in hand.

Three existing registry tests failed on the change and were rewritten rather
than adjusted: `merge_directory_replaces_whole_entry` and the two
field-clearing tests all asserted whole-value replacement using two `hint()`
entries, both at incarnation 0 — so they were exercising the tie rule to prove
something unrelated to it. They now use distinct ranks, which tests
replacement on its own terms.

Payoff beyond the bug: tk-92e0's last acceptance box is now achievable and
ticked. Pinning `BootCounter::advance` to a constant turns that test red,
where before the tie rule let the move succeed with a dead counter.

### What the first attempt got wrong

Review failed it on a regression in the hint path, correctly.

`AddLearner` writes its descriptor at incarnation 0 (`node.rs:193`), and that
is *not* a boot-counter rank — so "a tie can only be two copies of one
descriptor" was false for hints. Under a flat `>=`, a hint lost to an existing
hint, and re-issuing `AddLearner` with a corrected address stopped working.
That is unrecoverable rather than merely annoying: the node a bad hint names
cannot be dialled, so it never publishes a rank of its own, nothing supersedes
the mistake, and the only exit is the tombstone that burns the id for good.

The evidence was in hand and misread. Three registry tests failed on the
change because two `hint()` entries at 0 no longer replaced each other, and
that was filed as test-design noise. The function's own doc comment said
plainly why it mattered — "Ties go to the incoming entry, which keeps a feed
of hints last-write-wins among themselves" — and was not read before the
comparison under it was changed.

Fixed by making **incarnation 0 the absence of a rank** rather than a low one:
a hint replaces a hint, a hint still loses to any ranked descriptor, and
ranked ties keep the incumbent. Both halves are pinned by tests that fail
under the opposite rule — checked by mutation this time, in both directions.

Also replaced `a_peers_stale_copy_cannot_displace_a_node_that_moved`, which
review found passed under the old rule too. Its premise was worse than
vacuous: it described a peer holding a *different* address at an *equal*
rank, which unique ranks make impossible. Dropped for two tests covering the
hint space, which is where the real behaviour lives.

### Three review rounds, and what they were about

Round 1 found the hint regression (above). Rounds 2 and 3 found stale prose —
twice in a file the same change had just edited. Worth recording the cause,
because it was the same both times: the docs were patched by matching the
exact sentence already known to be wrong, so nothing ever surfaced the
paragraph four lines further down. The third round's finding was the
dangerous one — `docs/ggap-node.md` told an operator whose disk had filled to
wipe the data dir and burn the node id, because a recovery paragraph written
for one cause of an error now sat under two. Reading each section whole, and
sweeping every file that mentions the rule, is what the first two rounds
should have done.

Also updated as a result: the two normative descriptions of the ordering rule
that a reader can reach without touching `registry.rs` —
`proto/ginnungagap/v1/cluster.proto`'s `incarnation` field and
`NodeDescriptor` in `ggap-types` — both of which still said only "highest
wins".

### Scope of the second half

A first attempt at this section called ties-lose mere hardening, on the
grounds that unique ranks make an equal-rank conflict unreachable. Review
pointed out that this is the wrong way round, and it is: ties-lose does not
soften the reissued-rank case, it *sharpens* it. Before, a reissued rank
produced a flap that arrival order resolved; now the node loses authorship of
its own entry permanently and the move never installs.

So the two halves are coupled rather than independent. Ties-lose closes the
reinstatement vector — a peer that has not heard of a move can no longer
reinstate the address it holds — and pays for it by making any surviving route
to a reissued rank unrecoverable. The fatal write closes the route this task
reported. tk-19c0 holds the two that remain (a crash before the write reaches
disk; a recovery from a directory that has not yet been persisted), and takes
its severity from this change.

## Review 2026-10-09T19:46:34+00:00 — fail

Verified empirically: full suite 25 result lines / 0 failures; registry+boot_counter unit tests stable over 8 repeats; merge_directory_keeps_the_entry_it_holds_at_an_equal_rank is RED under the old '>' rule; the claimed payoff reproduces (pinning the harness incarnation to a constant 1 fails a_restarted_member_at_a_new_address_rejoins_the_cluster at three_node_cluster.rs:507, then reverted); arm_write_failure is cfg(any(test, feature="test-utils")) and no crate enables test-utils outside dev-deps, so it is unreachable in a feature-free build; the wipe-plus-move regression is sanctioned by Q1 and documented in CLAUDE.md.

FINDINGS

1. registry.rs:134 and :144 still document the OLD rule, in the very function that changed: 'ties resolved in favour of the incoming entry' and 'Ties go to the incoming entry, which keeps a feed of hints last-write-wins among themselves'. The code at :184 and the new inline comment at :173-181 say the opposite. The task's blast radius named 'the comparison and its comment'; acceptance box 2 is ticked with a rustdoc 50 lines above the code that contradicts it.

2. The premise is false, and it costs a production path. registry.rs:176 ('a tie can only be two copies of the same descriptor'), boot_counter.rs:30-36, docs/ggap-consensus.md:95-101 and docs/ggap-storage.md:72-78 all rest on ranks being unique per boot. Incarnation-0 hints are not boot-counter ranked: node.rs:193 writes NodeDescriptor::hint(addrs) at 0, and under '>=' a hint loses to ANY existing entry (0 >= 0), where before it won a tie against another hint. So AddLearner issued with a wrong cluster_addr can no longer be corrected by re-issuing it: the leader keeps the wrong address; the learner boots with an empty directory and no seeds (main.rs:268 ShardRegistry::new(cli.node_id, [])) and gossip only goes to peers it can already resolve (gossip.rs:189 exchange_round over peers_excluding_self), so it is never dialled and never publishes its own rank>=1 descriptor to supersede the hint, which meanwhile gossips and persists. The only exit is RemoveNode, an irreversible tombstone that burns the id (admin_service.rs:163 then rejects re-adding it). Q1's reasoning covers boot-counter ranks and the persisted-directory self-entry only; it says nothing about hint-vs-hint. node.rs:179-184's doc ('addrs ... is written into this node's directory as a hint') is now conditionally false, and no test covers a re-issued AddLearner — three_node_cluster.rs:1034 uses a fresh id 99 with no prior entry. Either make the hint path immune (it is written by the leader locally, about a node that by definition has not published) or record the lost capability as an accepted consequence in the task and the docs.

3. registry.rs:535-552 a_peers_stale_copy_cannot_displace_a_node_that_moved pins nothing: ranks 7 and 8 are strictly ordered, and I confirmed it passes unchanged under the old '>' rule, despite its doc claiming 'at the rank that peer recorded ... the case the rule exists for'.

4. Minor, for the record: boot_counter.rs:172 writes through store.node.insert with no persist/fsync, so a power-loss after a successful write can still reissue a rank. Making the write fatal closes the API-error route, not the durability route — and ties-lose means that case no longer self-heals. Worth a discovered-from task rather than a fix here.

## Review 2026-10-09T20:05:47+00:00 — fail

Re-review of the four fixes. Verified by running: full suite 25 result lines / 0 failures; mutation A (ranked '>=' -> '>') turns registry.rs:538 merge_directory_keeps_the_entry_it_holds_at_an_equal_rank red and nothing else; mutation B (drop the rank-0 exemption arm) turns registry.rs:555 a_later_hint_replaces_an_earlier_one red and nothing else in the whole workspace; pinning the harness incarnation to 1 in three_node_cluster.rs:105 turns a_restarted_member_at_a_new_address_rejoins_the_cluster red with the exemption in place; fmt and clippy --all-targets --all-features -D warnings clean; no test present at base 4c36e59 was deleted or weakened (three added in registry.rs, two in boot_counter.rs); the vacuous a_peers_stale_copy_cannot_displace_a_node_that_moved was working-tree-only and is gone. Findings 1, 3 and 4 are properly addressed; the rank-0 exemption holds up under attack (the only rank-0 producers are node.rs:199 and its gossip/persistence copies; a hint can never unseat a ranked entry, so no reinstatement vector is reopened, and hint-vs-hint flap is transient because the corrected hint lets the learner be dialled, after which its rank-1 publication supersedes every hint).\n\nFINDING (fail, same class as last time's finding 2)\n\n1. docs/ggap-storage.md:103-105 still documents the OLD behaviour, in the same file the fix updated: 'A counter that cannot be *written* is only a warning: this boot is still correctly ranked, and the next one re-uses this incarnation - a tie, which the self-publication wins back on the following tick, not a deficit.' The code at boot_counter.rs:97-112 makes that write fatal, and the new paragraph this change added 30 lines above at docs/ggap-storage.md:73-79 says the opposite ('advance fails rather than return one it could not persist'). A reader hitting the later paragraph is told the exact reasoning tk-d4b8 exists to delete. Acceptance box 2 ('states a reason that is true') cannot stand with it there. Also stale alongside it: the decision table at docs/ggap-storage.md:91-98 is introduced as what advance does and 'fails the boot rather than return a rank it knows to be unfounded', but has no row for the write-failure outcome.\n\nNOTES, not blocking\n\n2. On the 'honest scope' framing: it is right that nothing observable changes on a correct system, but 'ties-lose is hardening / bounds the damage if a rank is reissued' is the wrong way round. At a reissued rank with a changed address, ties-go-to-incoming produced a flap; ties-lose makes the new address permanently uninstallable and the node permanently loses authorship of its own entry - the 'starting low is worse than not starting' mode. That is exactly the consequence Q1 accepted ('never adopted at all') and CLAUDE.md records for the wipe case, so it is sanctioned, but it means the two halves are coupled rather than independent: ties-lose is what makes rank uniqueness load-bearing, and so what sets tk-19c0's severity. Worth saying that way round in the task rather than 'bounds the damage'.\n\n3. A third reissue route is not covered by tk-19c0's fsync framing: boot_counter.rs:149-170 recover_from_directory reads the rank from the persisted directory, which the gossip task only writes after its first round (gossip.rs:114), so a boot that publishes and then dies before that round, combined with a later unreadable counter, recovers a rank one boot stale and reissues it. Same consequence class as tk-19c0; fold it into that task's context rather than filing a fourth.\n\n4. The claim that ties-lose stops 'a peer that has not heard of a move reinstating the address it still holds' (registry.rs:144-145, :184-188, CLAUDE.md:97-99) describes a state unique ranks make unreachable - the strictly-higher rule already covers the live move. The comments read as the normal case what the task itself calls hardening.\n\n5. node.rs:188-192: the inserted sentences leave 'Publishing before the membership' hanging on an over-long line; cosmetic, fmt does not rewrap doc comments.\n\n6. Tooling, not the change: tk-d4b8's touched list is ['CLAUDE.md'] only, though the session edited registry.rs, node.rs, main.rs, boot_counter.rs and three docs. The hook is under-recording.

## Review 2026-10-09T20:13:08+00:00 — fail

Third pass. Code confirmed unchanged since the 20:05 review (mtimes: registry.rs 22:02, node.rs/boot_counter.rs 21:50, main.rs 21:35 local — all before the 22:05 local review; only docs/ggap-storage.md, docs/ggap-node.md and the two task files are newer), so the previous mutation verification stands. Re-ran cargo test --all --all-features --no-fail-fast: all result lines pass, 0 failures. Previous blocking finding is fixed: docs/ggap-storage.md:73-79 and :104-112 now both say the write failure fails the boot, and the decision table at :90-100 has the 'any of the above / the rank cannot be written' row.

Swept every file that mentions the counter, the incarnation, the directory ordering rule or the AddLearner hint: CLAUDE.md, docs/ggap-storage.md, docs/ggap-consensus.md, docs/ggap-node.md, docs/ggap-types.md, docs/ggap-server.md, README.md (no mention), proto/ginnungagap/v1/cluster.proto, crates/ggap-types/src/lib.rs, crates/ggap-storage/src/{boot_counter.rs,directory.rs}, crates/ggap-consensus/src/{registry.rs,node.rs,gossip.rs}, crates/ggap-node/src/main.rs, and the test modules in boot_incarnation.rs / directory_persistence.rs / gossip_self_publication.rs / three_node_cluster.rs. One blocking hit.

FINDING

1. docs/ggap-node.md:78-81 is stale against the section directly above it, introduced by this round's edit. The 'cannot establish this node's incarnation' section now documents two causes (:64-72), but the recovery paragraph still covers only the first: 'Both records live in the node keyspace, so losing both points at the data dir rather than at one key. Recover by treating it as a wipe: clear the data dir and start the node under a fresh --node-id, removing the old id from each shard's membership.' For the second cause — the rank could not be written back — that advice is wrong and destructive. The counter and the directory are both intact and readable; nothing was published at the refused rank (boot_counter.rs:226-247 a_rank_that_cannot_be_persisted_fails_the_boot asserts the key is still unwritten, and a_boot_after_a_failed_write_still_advances asserts the next boot takes the rank normally), so the fix is to make the data dir writable and restart — no wipe, no fresh node id, no membership surgery. The paragraph tells an operator whose disk is full to burn the node id irreversibly. The pronoun makes it worse: :74 uses 'Both' for the two causes and :78 uses 'Both records' for the two records two lines later, so a reader hitting :78 after :68-76 reads the wipe as the recovery for the message, not for one of its causes. Fix: scope the wipe recovery to the first cause and give the second its own line (restore write access to the data dir and restart; the refused rank was never issued).

NOTES, not blocking

2. proto/ginnungagap/v1/cluster.proto:153 ('Orders descriptors for one node: highest wins.') and crates/ggap-types/src/lib.rs:98-99 ('Highest incarnation wins') are the normative docs on the wire type and the domain type, and neither says the comparison is strict or that 0 is exempt. Not a contradiction — 'highest wins' stays true — but they are now the two places a reader can learn the ordering rule without learning the part this task changed. A clause each would close it.

3. crates/ggap-consensus/src/node.rs:192 still leaves 'Publishing before the membership' hanging past the comment width after the inserted sentences; cosmetic, carried from last round's note 5, fmt does not rewrap doc comments.

4. boot_counter.rs:36-38 states rank uniqueness flatly ('That is what makes a rank unique to one boot') while tk-19c0 now records two live routes that break it. That is sanctioned — tk-19c0's third acceptance box explicitly owes the residual-window note at exactly this spot and in docs/ggap-storage.md — so it is not a finding here, only a pointer that the claim is currently stronger than the system.

5. Accepted as-is and re-checked rather than re-verified: the rank-0 exemption reproduces base behaviour exactly for hints (old rule outranked = held > 0 for desc.incarnation == 0, new rule the same arm), so gossip.rs:451-453's comment about a pre-incarnation peer decoding to a hint is still accurate. .tasks/tk-19c0.md absorbs the recover_from_directory route and the severity coupling; .tasks/tk-d4b8.md's 'Scope of the second half' now states the coupling the right way round. .tasks/tk-92e0.md's newly ticked box matches the mutation I ran last round.

## Review 2026-10-09T20:23:13+00:00 — fail

Fourth pass. The docs/ggap-node.md restructure is correct and I accept it: read as an operator, cause 1 (:66-77) names the two unreadable records and owns the wipe recovery with the reason the id is burned; cause 2 (:79-90) opens 'Do not wipe anything', states both records intact and nothing published at the refused rank, and prescribes restoring write access and restarting. The two recoveries cannot be swapped — each sits under its own bold numbered cause, and cause 2 negates the wipe in its first three words. Verified cause 2 against the code: main.rs:234 wraps advance() with that context so both causes do surface under this heading; boot_counter.rs:97-112 fails without writing, and boot_counter.rs:233-249 / :252-266 assert the key is left unwritten and the next boot takes the rank. Proto change is comment-only (git diff shows no non-comment lines) and nothing generated is checked in, so nothing was invalidated: re-ran fmt --check clean, clippy --all-targets --all-features -D warnings clean, cargo test --all --all-features --no-fail-fast = 25 result lines / 0 failures, and both mutations still pin — ranked '>=' -> '>' reds only registry.rs merge_directory_keeps_the_entry_it_holds_at_an_equal_rank, and dropping the rank-0 exemption arm reds only a_later_hint_replaces_an_earlier_one. Notes 2 and 3 from last round are properly addressed (cluster.proto:153-158 and ggap-types/src/lib.rs:96-108 now both state strictness and that 0 is the absence of a rank ordered by arrival; node.rs:187-193 reflows within width).

FINDING

1. crates/ggap-consensus/tests/boot_incarnation.rs:55-57 documents the OLD tie rule, in the acceptance test for the behaviour this task inverted: 'A counter that stopped incrementing would leave both at incarnation 1, where ties go to the incoming entry and the stale copy wins at both ends.' registry.rs:184 and :173-181, docs/ggap-consensus.md:100 and boot_counter.rs:101-102 all now say the opposite. Both halves of the sentence are false under the implemented rule: with the counter frozen at 1, the peer's old@1 entry is the incumbent and the node's new@1 loses there, while at the node's own end its own entry is the incumbent and the peer's stale copy loses — so the stale copy wins at one end, not both, and it wins by the incumbent rule rather than by arrival. This is the fourth consecutive round of prose that contradicts the code, and this file was named in the task's own blast radius ('a case in crates/ggap-consensus/tests/boot_incarnation.rs pinning equal-rank behaviour') and was claimed as swept in the third review. The test body and its assertions are correct; only the rationale needs rewriting to the incumbent rule.

NOTES, not blocking

2. cluster.proto:155 now carries the flat 'unique to that boot' claim as well, so tk-19c0's owed residual-window note has a third site alongside boot_counter.rs:36-38 and docs/ggap-storage.md. Worth adding to that task's list rather than qualifying it here.

3. The ggap-node.md section tells an operator the two causes but not how to tell them apart from the log: the inner anyhow cause differs ('cannot persist the boot counter at incarnation N' vs the directory decode error) and quoting it under each heading would make the triage mechanical. Cosmetic.

4. Left as sanctioned: boot_counter.rs:36-38's uniqueness claim (tk-19c0), the wipe-plus-move regression (Q1, recorded in CLAUDE.md), and tk-d4b8's touched list still being ['CLAUDE.md'] only — hook under-recording, not the change.

## Review 2026-10-09T20:34:51+00:00 — pass

Fourth-round finding is fixed, and the new rationale is correct — verified by running it, not by reading it. A throwaway integration test against merge_directory with both ranks frozen at 1 printed PEER = old:1 and NODE = new:1, which is exactly what boot_incarnation.rs:55-62 now claims (incumbent keeps its entry at both ends; the move never reaches the peer; no flap, no arrival order). Test body and assertions untouched; the frozen case would also still turn the test red at the 'second > first' assertion, so the pin holds.

Re-verified independently of the implementer's summary: mutation A (registry.rs:197 '>=' -> '>') turns registry::tests::merge_directory_keeps_the_entry_it_holds_at_an_equal_rank red and nothing else in ggap-consensus, then reverted (confirmed restored byte-for-byte at :196-197). Full suite cargo test --all --all-features --no-fail-fast: 25 result lines, 0 failures. fmt --check clean, clippy --all-targets --all-features -D warnings clean. Ran my own prose sweep over every *.rs/*.md/*.proto outside target/ and .tasks/ for the old-rule phrases plus 'equal rank', 'equal incarnation', 'incarnation 1': every live hit states the implemented rule (CLAUDE.md:98, registry.rs:134/:533, main.rs:230, node.rs:186, boot_counter.rs:7, docs/ggap-consensus.md:93/:100/:109, docs/ggap-node.md:54/:78, docs/ggap-storage.md:77/:108). Round 3's destructive-advice finding stays fixed: docs/ggap-node.md:76-86 now gives cause 2 its own 'Do not wipe anything' recovery.

Epic pointer section checked against git and the task files rather than the summary, and it is accurate: tk-5a4b status=done, landed as ace0ce2 (#86); tk-92e0 status=done review=pass, landed as 4c36e59 (#87); tk-8c62 status=open with Q1 '> unanswered'; tk-19c0 and tk-9cdd both status=open, both discovered_from=tk-d4b8, and tk-19c0 does carry the second (recover_from_directory) route the earlier review asked be folded in. The quoted 'incarnation, highest wins' is verbatim tk-ef8d.md:39, the refinement described matches registry.rs:196-197, and the diff only appends — the approved Target design text is unmodified, which is the right way to record a refinement to an approved spec. tk-92e0's last box flipping to [x] is justified: the previous round verified the frozen-counter mutation now fails that test.

Accepted as-is, non-blocking: the section calls them 'the review's four findings' where the 2026-09-04 review had three numbered plus the unnumbered 'stated plainly' paragraph that became tk-92e0 — the mapping is right, the count is generous. It says tk-d4b8 'Landed' while this change is still uncommitted, which is conventional here since the task file commits with its code. tk-d4b8's touched list is still ['CLAUDE.md'] alone despite edits to registry.rs, node.rs, main.rs, boot_counter.rs, lib.rs, cluster.proto and three docs — a hook under-recording, not a fault in the change. boot_counter.rs:36-38 still states rank uniqueness more strongly than the system guarantees, which tk-19c0's third acceptance box explicitly owes.
