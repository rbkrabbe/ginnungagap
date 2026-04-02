# DST Review: Findings and Improvement Recommendations

## Context

Phase 5 and 6 are complete and merged. This document reviews `crates/ggap-consensus/tests/sim_cluster.rs` — whether the tests are realistic, what confidence they provide, and whether including the gRPC layer would be worthwhile.

---

## Assessment Summary

The current 5 tests form a **solid foundation** but provide **moderate confidence** — they catch gross consensus failures (split brain, lost writes, stalled elections) but cannot detect subtle ordering bugs, stale reads, or concurrency anomalies.

---

## What the Tests Do Well

1. **Deterministic harness** — seeded RNG + `start_paused` = fully reproducible. This is the correct foundation and matches industry practice (FoundationDB, TigerBeetle).
2. **In-process SimNetwork** — standard for DST. Bypassing gRPC is the right call (see gRPC section below).
3. **Covers the critical Raft transitions** — election, leader failure + reelection, partition + heal, snapshot catchup. These are the scenarios most likely to cause real data loss.
4. **Write-through-Raft path** — writes go through `client_write()` → openraft → `apply()` → fjall. This exercises the real commit path, not a mock.

## What the Tests Miss

### 1. The "linearizability checker" is not a linearizability checker

`check_read_after_write` only verifies: "if we wrote key X, a later FSM read sees it." This is a **durability/visibility check**, not linearizability. It runs writes sequentially (no concurrency), reads from the leader's local FSM (no network), and has no concept of operation ordering or happens-before.

A real linearizability checker (Jepsen's Knossos, or porcupine in Go) would:
- Track wall-clock intervals `[invoke, return]` for every operation
- Accept concurrent operations from multiple clients
- Search for a valid sequential history consistent with the partial order
- Detect stale reads, lost updates, and ordering inversions

**Verdict:** The name overpromises. As a read-after-write test it's fine, but it cannot catch the bugs linearizability testing is designed to find.

### 2. No concurrent client operations

All 5 tests issue operations sequentially from a single "client." Real distributed systems break under concurrency — two clients writing the same key, a read racing with a write, pipelined requests where the first hasn't committed when the second arrives. None of this is exercised.

### 3. Only symmetric partitions

`partition(a, b)` blocks both directions. Asymmetric partitions (A→B fails, B→A works) cause more subtle bugs — a leader that can send heartbeats but can't receive votes, a follower that sees writes but can't acknowledge them. These are common in production (unidirectional packet loss, firewall misconfiguration).

### 4. No crash-recovery testing

Nodes are killed by removing from the registry — their in-memory state vanishes. No test verifies that a node can restart from its fjall data directory and rejoin the cluster with its Raft log intact. This is arguably the most important durability property.

### 5. No cross-node state comparison

`test_leader_failure_and_reelection` writes `k1` to the original leader, kills it, but never checks whether `k1` survived on the new leader. `test_partition_and_heal` does check cross-node state (good), but the others don't. A "check all nodes agree on final state" assertion after every test would catch replication bugs.

### 6. Snapshot test may pass without snapshots

`test_snapshot_catchup` writes 20 keys with threshold 10, then adds a new node. But if log replication alone delivers all 20 entries before compaction, the test passes without any snapshot being built or installed. There's no assertion that a snapshot was actually created or transferred.

---

## On Including the gRPC Layer

**Recommendation: Keep it out of DST. Add a separate integration-level fault test instead.**

**Why omitting gRPC is standard practice for DST:**
- DST's value is **deterministic reproduction** of consensus bugs. gRPC adds non-deterministic timing (TCP retransmits, HTTP/2 flow control, TLS handshakes) that breaks reproducibility.
- FoundationDB, TigerBeetle, and CockroachDB all separate their simulation layer (in-process, no real network) from their integration/e2e tests (real network, real gRPC/HTTP).
- The SimNetwork already exercises the exact same openraft code paths that production uses — `append_entries()`, `vote()`, `install_snapshot()`. The only layer skipped is serialization + transport.

**What gRPC-layer testing would catch that DST won't:**
- Proto serialization bugs (field order, missing fields, backward compat)
- tonic error mapping (gRPC status codes, `ForwardToLeader` → `Status::unavailable`)
- Connection lifecycle bugs (stream resets, keepalive failures)
- Request size limits and backpressure

**Recommendation:** These are better tested with the existing `three_node_cluster.rs` integration tests (which do use real gRPC). If you want fault injection at the gRPC layer, consider a separate test that wraps tonic channels with a drop/delay interceptor — but keep that separate from the DST harness.

---

## Recommended Improvements (prioritized)

Each item below is designed to be implemented as an independent PR.

### P0 — High confidence gains, moderate effort

#### 1. Cross-node state assertion after every test
Add a helper `assert_all_nodes_agree(cluster, keys)` that reads each key from every surviving node's FSM and asserts they all match. Apply after `test_leader_failure_and_reelection` (check `k1` on new leader), and after `test_partition_and_heal`.

#### 2. Verify snapshot was actually installed
In `test_snapshot_catchup`, after adding node 4, check that node 4's last applied log index jumped (indicating snapshot install) rather than incrementing one-by-one (indicating log replication). Can query `raft4.metrics().borrow().last_applied`.

#### 3. Membership change under partition
Start a membership change (add node 4), then partition the leader mid-joint-consensus. Verify the cluster recovers to a consistent membership state.

### P1 — Meaningful coverage expansion

#### 4. Crash-recovery test
Stop a follower, drop its `Arc<GgapRaft>` (but keep its `TempDir`), recreate log storage + state machine from the same directory, create a new `GgapRaft` instance, re-register in the node registry. Verify it catches up and has all previously committed data.

#### 5. Asymmetric partition test
Modify `FaultController` to support unidirectional partitions (block A→B but not B→A). Test: partition leader's outbound to one follower. Leader can't replicate to that follower, but the follower still sees the leader's heartbeats (so no election). Heal and verify the follower catches up.

#### 6. Rename and strengthen the linearizability test
Rename `check_read_after_write` to exactly that — don't call it a linearizability checker. For actual linearizability checking, record `(op_type, key, value, start_time, end_time)` for all operations and implement a Wing & Gong style checker, or integrate the `porcupine` algorithm pattern. This is the highest-effort item but provides the strongest guarantees.

### P2 — Nice to have

#### 7. Cascading leader failure
Elect leader A, write, kill A, wait for B to become leader, immediately kill B. Verify the remaining node C either becomes leader (if quorum allows) or correctly refuses writes.

---

## Confidence Assessment

| Property | Current confidence | After P0 fixes | After P0+P1 |
|----------|-------------------|-----------------|--------------|
| Leader election works | High | High | High |
| Leader failover preserves data | Medium (no cross-check) | High | High |
| Partition doesn't cause data loss | Medium-High | High | High |
| Writes are durable after commit | Medium (no crash test) | Medium | High |
| Reads are linearizable | Low (sequential only) | Medium | Medium-High |
| Snapshot mechanism works | Medium (may not trigger) | High | High |
| System recovers from crashes | None | None | High |

**Overall: The tests provide ~60% of the confidence a production-grade DST suite would. Implementing P0 (items 1-3) would bring it to ~75%. Adding P1 (items 4-6) would reach ~90%.**
