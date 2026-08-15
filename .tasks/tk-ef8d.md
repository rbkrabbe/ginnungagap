+++
id = "tk-ef8d"
title = "Addresses leave consensus: membership carries ids, the directory resolves addresses"
kind = "epic"
status = "open"
size = "l"
priority = 1
blocked_by = []
tags = []
created = "2026-08-15T10:11:08+00:00"
spec_approved = true
review = "none"
touched = []
spec_approved_at = "2026-08-15T10:42:30+00:00"
+++
## Goal

An operator can change a node's address by restarting it with a new
`--cluster-addr` / `--client-addr`. Today that is impossible: addresses live in
Raft membership, `change_membership` carries only ids, `add_learner` goes through
openraft's `AddNodes` which "**WONT** replace existing node", and `SetNodes` is
unused — so a node's address is fixed for the life of the shard, and a typo at
join time is unfixable without destroying it.

## Target design

**Membership carries ids. The directory resolves addresses.**

- `GgapNode` keeps existing (openraft needs a `Node`) but carries no addresses —
  it becomes the home for future consensus-only fields, nothing more.
- `GgapNetwork` resolves the target's address **per RPC** through
  `ShardRegistry`, not once at `new_client`. `network.rs:52` already ignores
  `_target_id` and reads `node.cluster_addr()`; that is the line to invert. Per-RPC
  is what makes a changed address take effect without a new client.
- Every node **publishes its own descriptor** — `(node_id, cluster_addr,
  client_addr, incarnation)` — into its registry each tick, and gossip carries
  it. The node is the sole author of its own addresses.
- `merge_directory` orders by **incarnation, highest wins**. Addresses come from
  CLI flags, so they can only change across a restart: a boot-scoped counter is a
  sufficient clock and no per-write sequence is needed.
- `not_leader()` (`node.rs:24`) resolves the leader's address through the
  registry rather than reading it out of openraft's `ForwardToLeader`.
- `AdminService` reads addresses from the registry for **every** shard, hosted or
  not, collapsing `ids_to_node_infos`' non-hosted special case into the one path.
- **`AddLearner` writes the new node's addresses into the leader's directory, at
  incarnation 0.** It keeps carrying them over the wire — they are how the
  cluster first learns where the new node is — but they stop entering membership.
  Incarnation 0 is what keeps sole authorship intact: the node's own first
  publication starts at 1 and immediately supersedes the hint written on its
  behalf.

### How a node joins, end to end

Worth stating once, because three children touch a piece of it and the shape is
load-bearing: **a joining node is dialled; it never dials.**

1. The operator calls `AddLearner(N, addrs)` on the leader, which writes N's
   incarnation-0 descriptor into its directory.
2. Gossip spreads that descriptor, so every peer now has N in
   `peers_excluding_self`.
3. Peers dial N. Because `gossip_service.rs:22` returns the responder's full
   view, the first inbound `Exchange` hands N the entire directory.
4. In parallel the leader dials N with AppendEntries, resolving N's address
   through the directory. `main.rs:264` already leaves a non-seed fresh node's
   Raft uninitialized precisely until this happens.

N therefore needs no bootstrap configuration, which is why this epic adds no
`--join` flag (Q2). The same mechanism re-seeds a restarted node whose directory
is empty.

### Why, and what was rejected

The alternative is etcd's: keep addresses in membership and add a member-update
operation (`SetNodes`) to change them. Rejected because we are multi-raft. etcd
has one Raft group, so one address change is one operation; here a node is in
every shard's membership, so it would be N operations against N leaders, with a
window where shards disagree — and ordering those copies needs a clock, of which
`(term, index)` gives us N unrelated ones for a single node-scoped fact.

CockroachDB's shape avoids all of it: a range replica is `(NodeID, StoreID,
ReplicaID)`, and `node_id -> host:port` lives in gossip, republished by each node
at startup. One node, one address, one place, one clock.

### What this reverses

Stated plainly, because most of it merged in the last three weeks:

- **tk-fd58, tk-10b7, tk-d049, tk-11b6** moved both addresses *into* membership
  and made the directory derived from it. This epic moves them back out. The
  self-seed tk-11b6 deleted is deliberately restored — with the ordering rule it
  lacked the first time, which was the actual defect.
- **The CLAUDE.md hard constraint from tk-1a8d** — "Per-node addresses live in
  Raft membership, never in gossip" — becomes exactly wrong. Invert it; do not
  soften it.
- **tk-d08a's settled decision** that "no incarnation counter is needed anywhere"
  is reversed. Here the counter is load-bearing: without it this design *is*
  tk-1bf0.

Kept from that work: `NodeAddrs` as a shared domain type, `ShardRegistry` and the
gossip transport, the advertise/bind symmetry from tk-d049, and `seed_peers` from
tk-11b6 — which stops being vestigial and becomes required.

## Non-goals

- **`ggap-pd` / rebalancing.** Node mobility becomes possible; nothing drives it.
  Placement stays manual.
- **Node retirement and directory removal.** Nothing removes a departed node from
  membership or the directory today, and nothing here changes that. Filed
  separately; do not fix it in a child.
- **On-disk compatibility.** Format-breaking children wipe the data dir, per
  tk-d08a Q2. No migration path.
- **TLS / auth on gossip.** Making the directory authoritative for addresses
  means a bad actor who can inject gossip can redirect Raft traffic. Real, and
  out of scope — no child should half-address it.

## Vocabulary

- **Descriptor** — a node's self-published `(cluster_addr, client_addr,
  incarnation)`. Authored by exactly one node: the one it describes.
- **Incarnation** — the monotonic counter that orders descriptors for a node.
  Scoped to a node, not a shard, which is the whole point.
- **Directory** — `ShardRegistry`'s `node_id -> descriptor` map. After this epic
  it is the *source of truth* for addresses, not a cache of membership.
- **Resolution** — turning a `node_id` into a dialable address at send time.
  `GgapNetwork` and `not_leader()` both do it; nothing caches the result.

### Q1 [answered 2026-08-15T10:35:06+00:00] What clock orders a node's descriptors?
- a) Boot counter persisted in the data dir, incremented each start — monotonic without trusting any clock; costs one more piece of local durable state, and a wiped data dir restarts the counter (harmless: peers see a lower incarnation and keep the old address until the node outlives it, so a wipe needs a fresh node id)
- b) Wall-clock start time — no extra state and naturally increasing; wrong under clock skew or a backwards NTP step, which is exactly when an address change also needs to win
- c) Raft-independent logical counter in the ShardMap — reuses existing durable state; couples the directory to storage that a node hosting no shard may not have
> Boot counter persisted in the data dir, incremented each start; no refutation. A node's own first publication starts at 1, so the incarnation-0 descriptor that AddLearner writes on its behalf is immediately superseded by the node itself — preserving sole authorship. Wipe-plus-move is the only case this cannot recover (a wiped node restarts at a low incarnation and cannot outbid peers holding a higher one), and it is narrow: a wipe alone is harmless because the peers' stale entry still points at the right address. Document that changing a node's address at the same time as wiping its data dir requires a fresh node id.

### Q2 [answered 2026-08-15T10:35:06+00:00] How does a node reach the cluster before the directory is populated? Raft cannot dial a peer whose address it has not yet learned, and after this epic membership no longer carries one.
- a) Both: a --join seed list for cold start, and the directory persisted to fjall for restarts — matches CockroachDB; two mechanisms to build and keep consistent
- b) --join seeds only — one mechanism, nothing new on disk; every restart re-learns the cluster by gossip, so a node whose seeds are all down cannot rejoin even though it knew the addresses yesterday
- c) Persisted directory only — a restart is self-sufficient and needs no operator input; a genuinely fresh node has no way in, so cluster bootstrap needs a separate path anyway
> Persist the directory to fjall; no --join flag. AddLearner already bootstraps a joining node without it: main.rs:264 leaves a non-seed fresh node's Raft uninitialized until the leader dials it, and gossip_service.rs:22 returns the responder's full view, so a single inbound Exchange hands a new node the whole directory. It is dialled; it never dials. Persistence is therefore not needed for capability — peers re-seed a restarted node within a gossip round — but is kept so a restart resolves peers immediately rather than waiting to be dialled, which matters for a node that restarts and is elected before any peer has gossiped to it. seed_peers survives only for nodes in no membership: the observer harness and future ggap-pd.

### Q3 [answered 2026-08-15T10:35:06+00:00] What should Raft do when the registry has no address for a target node id?
- a) Fail the RPC and let openraft back off and retry — matches how an unreachable peer already behaves, so no new state machine; a cold node logs resolution failures until gossip converges
- b) Block the send until the address is known, with a timeout — fewer spurious failures in the seconds after boot; risks holding openraft's send path on a lock or channel, which is a worse failure than a retry
> Fail the RPC and let openraft back off and retry. An unresolvable node id behaves exactly like an unreachable peer, which openraft already handles, so this adds no new state machine and never holds its send path on a lock or channel.
