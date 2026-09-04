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
trusted from the error), maps it through `registry.client_addr()` **on every
forward**, and replays once with `ggap-forwarded` set. Unknown leader_id,
unknown client_addr, or a NotLeader from the target: fail fast with the
original status (Q2).

Resolution happens per forward, never once per node id. A channel may be held
between forwards, but only as a re-dial saver keyed by the address it was
dialled at: when the directory answers with a different address for the same
id, the held channel is discarded and the new address dialled. `GgapNetwork`
in `ggap-consensus/src/network.rs` is the worked example — `connect()` calls
`resolve()` unconditionally and `needs_redial()` compares the answer against
the address in hand. Mirror that shape rather than a bare
`HashMap<u64, Channel>`: after tk-ef8d a node's address changes across a
restart as a supported operation, so a cache keyed by id alone pins the
forwarder to a dead address for the life of the process.

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
  `RwLock<HashMap<u64, (String, Channel)>>` keyed by id and holding the address
  dialled, lazy connect mirroring `GgapNetwork::connect`/`needs_redial`; sets
  `ggap-forwarded`, copies the inbound `grpc-timeout` verbatim, injects trace
  context via `MetadataInjector`, both in `ggap-consensus/src/network.rs`.
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
- [ ] A target whose directory address has changed is re-dialled at the new one
      on the next forward, with no new forwarder and no process restart.
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
- Stale directory data outlives a re-dial. Re-resolving per forward handles a
  changed address, but a leader that moved and has not yet republished its
  descriptor resolves to the old one and the forward fails — the same
  fail-fast the client already retries through, not a new failure mode. Do not
  reintroduce the id-keyed cache to smooth it over.
- `serve_client`'s signature changes; `three_node_cluster.rs` and
  `trace_propagation.rs` both construct it.
