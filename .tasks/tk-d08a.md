+++
id = "tk-d08a"
title = "Raft membership carries both node addresses; the gossip directory becomes derived"
kind = "epic"
status = "open"
size = "l"
priority = 2
blocked_by = []
tags = []
created = "2026-07-30T19:05:50+00:00"
spec_approved = true
review = "none"
touched = []
spec_approved_at = "2026-07-30T19:14:33+00:00"
+++
## Goal

Any node can name another node's client address and be right, without that fact
having travelled through an unordered channel. Afterwards a forwarder, an admin
RPC, or a future placement driver can ask "where do I reach node 7's KV API" and
get an answer backed by committed consensus state rather than by whichever gossip
round spoke last.

## Why now

The information already arrives and is thrown away. `NodeInfo` (types.proto:14)
has carried both `client_addr` and `cluster_addr` since the beginning, and every
`AddLearner` sends both — but `admin_service.rs:134` passes only `cluster_addr`
into `add_learner`, because openraft's `BasicNode` has exactly one address field
and nowhere to put the other. So the cluster is told authoritatively, at join
time, a fact it then discards and spends a background protocol re-acquiring out
of band and unversioned.

That is the root of tk-1bf0. Everything downstream — the derivation helper in
tk-c507, the field-wise merge rule, the incarnation counter tk-1bf0 would have
added — is compensation for a lossy boundary. Widening the boundary removes the
need for all of it.

## Target design

`GgapTypeConfig::Node` becomes `GgapNode`, carrying a cluster address and a
client address. Consequences that define the end state:

- **Membership is the source of truth for both addresses.** `refresh_local`
  derives the whole directory entry from `raft.metrics()`, not just half of it.
  Address changes ride `change_membership` — an ordered, committed, durable
  operation — instead of a gossip race.
- **The directory keeps existing, but only as a cache for shards this node does
  not host.** Those entries are copies of committed state and carry the
  membership `(term, index)` they came from; higher wins (Q1). This mirrors how
  `ShardEntry::incoming_wins` already leads on `term` rather than inventing a
  clock, and it is why no incarnation counter is needed anywhere.
- **`client_addr` stops being gossip-originated.** No node self-seeds it, so
  `derive_client_addr` and the field-wise-merge rule both go away. Empty stops
  being a meaningful state.
- **Advertise and bind become symmetric.** `--client-addr` becomes an advertised
  address and `--client-listen-addr` its bind, mirroring the existing
  `--cluster-addr` / `--cluster-listen-addr` pair. The asymmetry between them is
  the reason an address ever had to be derived.

### Rejected

- **An incarnation counter on directory entries** (tk-1bf0 as originally filed).
  Works, but invents a second clock that must be persisted and must never
  regress, alongside the perfectly good one Raft already maintains. Closing the
  lossy boundary instead makes the counter unnecessary rather than correct.
- **Collapsing config to `--announce-host` + two ports.** Would delete the
  derivation just as effectively, but makes one host for both listeners a law
  rather than a default, and forecloses NAT-style port mapping. Rejected to keep
  the two addresses independent.
- **Dropping the gossiped directory entirely** (Q1 option c). Correct by
  construction and nothing left to version, but it breaks `ListShards` /
  `ClusterStatus` for non-hosted shards, which works today.

## Non-goals

- **The forwarder itself.** tk-2cb2 is unblocked by this and unchanged by it.
- **Console or UI work.** Admin RPCs will report both addresses; nothing renders
  them.
- **NAT / port-mapping topologies.** Keeping advertise and bind separate makes
  advertise-port ≠ bind-port expressible; no work to test or document it.
- **Multi-homing.** Independent addresses make split public/private networks
  possible; no work to support or test that.
- **On-disk backwards compatibility.** Only test clusters exist. Format-breaking
  children ship independently and a stale data dir is wiped (Q2).

## Vocabulary

- **`GgapNode`** — the openraft `Node` impl carrying both addresses. Lives in
  `ggap-consensus`, because `ggap-types` has no openraft dependency and must keep
  none.
- **Directory** — `ShardRegistry`'s `node_id -> addresses` map. After this epic
  it is a *cache of committed state*, not an independent source.
- **Derived entry** — a directory entry produced from local Raft membership.
  Authoritative for the shards this node hosts.
- **Copied entry** — a directory entry received by gossip about a shard this node
  does not host. Ordered by the membership `(term, index)` stamp it carries.

## Sequencing

The type flip is unavoidably atomic — a type parameter cannot be half-changed —
so the shape is: shrink it beforehand, harvest afterwards. tk-4b21 and tk-9e77
carry no `Node` change at all and can land first and independently; tk-2f5c is
the single mechanical flip; everything after it is a separate, reviewable payoff.

## Acceptance

- [ ] `add_learner` no longer discards `NodeInfo.client_addr`.
- [ ] A node reports a peer's client address from Raft membership alone, with the
      gossip task stopped.
- [ ] No code path derives one address from the other.
- [ ] tk-1bf0 is closed — as done or as obsolete, but not as forgotten.

### Q1 [answered 2026-07-30T19:06:37+00:00] A node only has Raft membership for shards it hosts. Addresses for nodes in other shards still arrive as gossiped copies. How are those ordered?
- a) Stamp with the membership (term, index) the address came from; higher wins — mirrors ShardEntry::incoming_wins leading on term, invents no clock, closes tk-1bf0
- b) Accept last-write-wins cross-shard and close tk-1bf0 as won't-fix — cheapest, but leaves the brittleness this epic exists to remove
- c) Drop the gossiped directory entirely — correct by construction, but breaks ListShards/ClusterStatus for non-hosted shards
> Stamp with the membership (term, index). Copied entries are ordered by the committed membership version they came from, so no incarnation counter is needed and tk-1bf0 closes rather than being mitigated.

### Q2 [answered 2026-07-30T19:06:44+00:00] Several children independently change on-disk format (Raft log entries, snapshots, bootstrap_members). Each invalidates an existing test cluster's data dir. How does that land?
- a) Accept several wipes — each child ships when ready; developers wipe data dirs as needed; no extra machinery
- b) Add a storage format-version marker first, so a stale data dir fails loudly instead of decoding garbage
- c) One flag day — batch every format-breaking change into a single PR, recreating the big-bang change this sequencing exists to avoid
> Accept several wipes. Only test clusters exist; children stay small and independent rather than being batched to protect data nobody has.
