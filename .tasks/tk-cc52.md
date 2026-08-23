+++
id = "tk-cc52"
title = "GgapNetwork resolves the target address per RPC through the registry"
kind = "task"
status = "in_progress"
size = "m"
priority = 2
blocked_by = ["tk-c593"]
tags = []
created = "2026-08-15T10:17:29+00:00"
spec_approved = false
review = "pass"
touched = []
parent = "tk-ef8d"
base = "454cb05d990b59fd8bef90353dbc3c4c69c7d906"
reviewed_at = "2026-08-23T14:33:09+00:00"
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

- [x] No address is cached across RPCs; a changed directory entry is dialled on
      the next send without rebuilding the client.
- [x] Membership still carries addresses at this point and is ignored by the
      network path — assert that, so the next task is a deletion not a fix.
- [x] Full checklist green: fmt, clippy -D warnings, build, test.


## Notes

Q3's "fail the RPC" answer made the change small, but it moved a constraint onto
every harness: a multi-node test with no gossip task can no longer elect a
leader, because nothing populates the directory Raft now dials through.
`three_node_cluster`'s `start_with_gossip` and `benches/kv_write` therefore seed
each node's directory before `initialize()` — with *cluster* addresses only.
That keeps `client_addr_comes_from_membership_without_gossip` honest: its old
"the directory is empty" assertion became "no directory entry carries a client
address", which proves the same thing about where the client addresses came
from without pretending Raft could work on an empty directory.

The registry moved to step 4b in `main.rs`, above the Raft groups. It depends on
nothing but `node_id` and the store, so the move is free; the persisted
directory load moved with it, which is what lets a restarted node resolve peers
on its very first send rather than on the first gossip tick.

A node added by `AddLearner` stays unresolvable on the leader until the gossip
task's next `refresh_local` derives its membership entry into the directory —
up to one gossip interval. openraft retries through it. tk-51b4 carries the real
fix (`AddLearner` writes the directory at incarnation 0) and is already blocked
on this task, so nothing new was filed.

## Review 2026-08-22T14:30:20+00:00 — fail

Category 2 (untested claim), proven by mutation.

FINDING — Acceptance box 1 ("No address is cached across RPCs; a changed directory entry is dialled on the next send without rebuilding the client") is ticked with no test behind it. crates/ggap-consensus/src/network.rs:105 connect() is the only place that composes resolve() + needs_redial(), and nothing in the workspace asserts that composition. I reintroduced exactly the forbidden behaviour at network.rs:106 — cache the address from self.connected and only call resolve() when it is None, i.e. a moved peer is never re-dialled — and ran cargo test --all --all-features --no-fail-fast: 100% green, 0 failures, including the four new unit tests. The three unit tests at network.rs:295/312/337 pin the halves in isolation (resolve() reads the directory live; needs_redial() compares strings) and the comment at network.rs:330 admits the earlier test "stops one step short", but nothing closes the step. Closing it is cheap: stand up a real RaftService on an ephemeral port, connect() once (Ok, connected=addr A), merge a descriptor for the same target pointing at a dead port, connect() again and assert it errors — a cached resolution returns the live client and the assert fails.

Checked and accepted as-is:
- Callers: every GgapNetworkFactory::new and run_split_handler call site is updated (main.rs:289/393, split.rs:257, 7 server tests, 2 consensus tests, kv_write.rs:73); no stragglers.
- Lock hazards: directory_addr() clones the String and drops the read guard before returning, so the dial in connect() holds nothing; registry.rs holds no guard across an await anywhere. No lock-order inversion with gossip's merge_directory.
- Startup order: registry + DirectoryStore::load at main.rs:255-274 depend only on node_id and store, and BootCounter (main.rs:229) builds its own DirectoryStore, so the move above the Raft groups is safe and is what makes a restart resolve peers on the first send.
- Error path: a failed dial leaves the stale (addr, channel) in place, but the next call re-resolves and re-dials, so a moved peer is never sent to at its old address.
- Test weakening: client_addr_comes_from_membership_without_gossip (three_node_cluster.rs:729) still proves its claim — the seeding at :239 injects cluster addresses only, so "no directory entry carries a client address" plus the per-peer client_addr equality at :755 keeps membership the only source. Same for the kv_write.rs:150 seeding. Nothing deleted or skipped.
- AddLearner gap: reasoning holds — gossip.rs:169 derives learners as well as voters into the directory, so the leader resolves a new learner within one interval and openraft retries through it.

The first review failed this task for a real hole: acceptance box 1 was ticked
on two unit tests that pinned `resolve()` and `needs_redial()` in isolation,
and nothing exercised their composition in `connect()`. Caching the address
inside `connect()` — the exact bug the task exists to prevent — left the suite
green. The fix is two tests that call `connect()` twice across a directory
change, against a minimal `RaftService` served on an ephemeral port: one
asserting the second dial lands on the new address, one asserting a move to an
unreachable address fails rather than silently reusing the old channel. Both
kill that mutation. Pinning the halves of a decision is not the same as pinning
the decision.

## Review 2026-08-23T14:33:09+00:00 — pass

Re-review of the fail finding (untested acceptance box 1), plus a fresh pass over the rest.

Finding closed, verified by re-running the mutation myself, not by report. Two mutations, each restored from a byte-copy backup afterwards:
1. The exact caching mutation from the first review (connect() at network.rs:105 prefers self.connected's address and only calls resolve() when None): connect_follows_the_target_to_its_new_address FAILS ('left: 127.0.0.1:54467, right: 127.0.0.1:54468') and a_moved_target_is_not_served_by_the_old_channel FAILS ('the old channel was reused for a target that has moved'). Both kill it.
2. needs_redial() at network.rs:100 ignoring the address (self.connected.is_none()): kills three tests, the two new ones plus a_channel_is_reused_only_for_the_address_it_was_dialled_at.
The composition connect() = resolve() + needs_redial() is now pinned, not just its halves.

Flakiness/environment: the six network:: tests were run five times single-threaded and three times at default parallelism, plus the whole ggap-consensus lib suite; green every time. No shared state between them - each #[tokio::test] gets its own runtime, serve_raft() binds 127.0.0.1:0 per test and the address is read back from local_addr(), so no fixed port and no order dependence. connect_follows... asserts assert_ne!(first, second) before relying on the two servers being distinct. The 127.0.0.1:1 dial in a_moved_target_is_not_served_by_the_old_channel is sound on CI: port 1 is privileged, so nothing can be bound there by an unprivileged test runner, and a loopback connect to an unbound port is an immediate ECONNREFUSED on both Linux and macOS. A bind-then-drop-listener would be marginally tighter but is not more correct - accepted as-is.

Test scaffolding drags in nothing new: EchoRaft and serve_raft use tonic and tokio-stream, both already non-dev dependencies of ggap-consensus; no Cargo.toml in the workspace changed. EchoRaft is #[cfg(test)] only and implements the three RaftService methods as echoes - it never reaches a production build and nothing outside network.rs's test module can name it.

Rest of the change, re-read hostile:
- Scope: the diff is the inversion (target_id + registry replacing addr), its call sites, and the main.rs step 7b -> 4b move the Context asked for. The CLAUDE.md paragraph is the settled-state update this repo expects. Nothing else.
- Callers: grepped GgapNetworkFactory::new and run_split_handler across crates/ - 12 factory sites and 5 run_split_handler sites, all updated, no stragglers.
- Decision Q3 ('fail the RPC, openraft retries') is honoured at network.rs:105 and pinned by an_unresolvable_target_fails_the_send; directory_addr (registry.rs:166) filters the empty string, so an addressless descriptor is an unresolvable id rather than a dial to ''.
- Error paths: a failed dial leaves the stale (addr, channel) pair in place, but the next connect() re-resolves and re-dials, so a moved peer is never sent to at its old address - which is exactly what the second new test asserts. Resolve holds no lock across the dial.
- Weakened tests: only client_addr_comes_from_membership_without_gossip (three_node_cluster.rs:729), whose 'directory is empty' became 'no directory entry carries a client address' because Raft now needs cluster addresses seeded to elect at all. The seeding at :242 injects cluster addresses only and the per-peer client_addr equality at :755 is untouched, so the test still proves membership is the only source. Explained in the task Notes. Nothing deleted or skipped.
- Full checklist re-run here: fmt --check clean, clippy --all-targets --all-features -D warnings clean, cargo test --all --all-features --no-fail-fast green with zero failures.
