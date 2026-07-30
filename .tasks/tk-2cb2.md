+++
id = "tk-2cb2"
title = "LeaderForwarder: replay leader-required KV requests at the shard leader"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = ["tk-c507"]
tags = []
created = "2026-07-30T08:11:48+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-4f29"
parent = "tk-3846"
+++
## Problem

A write or linearizable read landing on a follower returns UNAVAILABLE plus a
leader hint the caller cannot act on. With tk-c507 the receiving node now knows
the leader's client address; this makes it use it — replay the request once at
the leader and return the leader's response verbatim.

## Approach

`LeaderForwarder` resolves the target from `GgapError::NotLeader.leader_id`
(tk-3cd2 put it there precisely so the address is looked up fresh rather than
trusted from the error), maps it through `registry.client_addr()`, dials a
channel cached per node id, and replays once with `ggap-forwarded` set. Unknown
leader_id, unknown client_addr, or a NotLeader from the target: fail fast with
the original status (Q2).

The forward decision lives in the `KvService` trait impls, not inside the `do_*`
methods: each leader-required method clones its message, calls `do_*` as today,
and hands the error to one shared helper. Rejected: threading `Request<T>` into
the five `do_*` methods — it would put the same match in five places, which the
tk-4f29 draft already flagged as the failure mode here. The cost is a clone of
the request message on the leader-required paths (bounded by `max_value_bytes`,
1 MiB); the clone is skipped when `ggap-forwarded` is already set.

Scan is the one method that changes internally: `do_scan` hops across shards, so
per Q2 it only surfaces NotLeader when hop 0 collected nothing. Once entries
exist it breaks the loop and returns the partial page plus a continuation token —
already a legal response, and the client's resume routes by key to the right
shard.

## Changes

- `crates/ggap-server/src/forward.rs` (new) — `LeaderForwarder`: registry +
  `RwLock<HashMap<u64, Channel>>`, lazy connect mirroring network.rs:71; sets
  `ggap-forwarded`, copies the inbound `grpc-timeout` verbatim, injects trace
  context via the `MetadataInjector` pattern at network.rs:135.
- `crates/ggap-server/src/kv_service.rs` — one `maybe_forward` helper plus its
  use in `put`, `delete`, `compare_and_swap`, `get` and `scan` (trait-impl layer,
  ~line 360-403). `do_scan` gains the hop-0-only NotLeader rule.
- `crates/ggap-server/src/metrics.rs` — forwarded requests counted separately
  from locally-served ones.
- `crates/ggap-server/src/lib.rs` — `serve_client` / `serve_client_with_listener`
  take the registry and build the forwarder; `crates/ggap-node/src/main.rs:396`
  passes it.

## Out of scope

- Watch (Q3), Admin forwarding (Q8, → tk-f7b6), multi-hop and server-side retry
  (Q2, Q4).
- Subtracting elapsed time from the forwarded deadline. The inbound timeout is
  copied as-is; fail-fast means at most one hop's slack.

## Acceptance

- [ ] A Put sent to a follower succeeds and the value reads back on the leader.
- [ ] A linearizable Get sent to a follower returns the committed value.
- [ ] A request arriving with `ggap-forwarded` set is never forwarded again: a
      follower receiving one returns NotLeader.
- [ ] Leader's client_addr absent from the directory ⇒ fails fast with
      UNAVAILABLE and the hint; no retry, no hang. Same for `leader_id: None`.
- [ ] A Scan whose first hop is a follower forwards; a Scan that already
      collected entries returns a partial page with a continuation token instead,
      and resuming from that token returns the rest.
- [ ] Repeated forwards to one target dial once — assert the cache, not just the
      result.
- [ ] Forwarded requests are distinguishable from local ones in metrics/traces.
- [ ] Watch still serves locally.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.

## Risks

- Registry staleness: `leader_id` comes from the error, `client_addr` from
  gossip, so a just-elected leader may have no directory entry. Fail-fast makes
  that a visible client retry; the empty-entry case needs its own test, not just
  the happy path.
- The message clone is on the write path. Check `benches/kv_write.rs` before and
  after; if it registers, that is a reason to revisit the trait-impl-layer choice
  above, not to widen scope silently.
- The channel cache is never invalidated. A leader that restarts leaves a dead
  entry; tonic reconnects a broken `Channel` on its own, but a node whose
  client_addr *changed* would be dialled at the old one until process restart.
  Acceptable while addresses are StatefulSet-stable — say so in a comment rather
  than leaving it implicit.
- `serve_client`'s signature changes; `three_node_cluster.rs` and
  `trace_propagation.rs` both construct it.
