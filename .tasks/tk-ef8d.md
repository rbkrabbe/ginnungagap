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
review = "fail"
touched = []
spec_approved_at = "2026-08-15T10:42:30+00:00"
reviewed_at = "2026-09-04T18:23:21+00:00"
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

## Review 2026-09-04T18:23:21+00:00 — fail

Whole-epic re-read of 71478ef..HEAD (11 commits, incl. the adjacent tk-c47e and tk-239e/tk-d9a8). Checklist run myself and green: fmt --check clean, clippy --all-targets --all-features -D warnings clean, cargo test --all --all-features all suites 0 failures. The epic's design contract is met — GgapNode is empty (config.rs:18), bootstrap/AddLearner/source_members all carry ids (main.rs:319-329, node.rs:190, split.rs:126-138), GgapNetwork re-resolves per RPC and only caches a channel keyed by the address it was dialled at (network.rs:96-133), not_leader resolves through the directory (node.rs:38-48), ids_to_node_infos is one path for hosted and non-hosted (admin_service.rs:124-140), Q3's fail-the-RPC and Q1's boot counter are both pinned by tests. Nothing anywhere reads or reconstructs an address out of membership. Failing on the following.

1. Three source comments still assert the inverted invariant, which falsifies tk-10d8's ticked acceptance box 'No doc describes the directory as derived from membership':
   - crates/ggap-consensus/src/network.rs:57 — 'Membership still carries addresses, but the network path takes none of them'. Written by tk-cc52 when that was true; tk-51b4 (35c97c3) deleted the addresses and left the comment. This is the first file a reader opens to understand resolution, and it states the opposite of the epic.
   - crates/ggap-node/src/main.rs:194 — 'Needed before any Raft group starts, because seed bootstrap puts the advertised address into the initial membership.' Seed bootstrap builds BTreeMap<u64, GgapNode{}> at main.rs:326; no address enters it. The stated reason for the ordering is wrong (the real reason is the registry, given correctly 60 lines later at main.rs:250-256).
   - crates/ggap-server/tests/three_node_cluster.rs:55-56 — 'The advertised form of client_addr — what goes into Raft membership and, derived from it, the directory.' This is the only surviving instance of the exact 'derived from membership' phrasing tk-10d8 claims to have eliminated; grep for it returns this line alone.

2. A node that retires itself gracefully never persists its own tombstone, so the boot guard written for that case cannot fire on its primary path. admin_service.rs:326 calls registry.retire(self_id) then cancels the retire token; main.rs:428-441 reacts by calling shutdown.cancel() and exiting 500ms later. GossipTask::run (gossip.rs:104-117) selects on that cancel token before its next tick, so persist_directory never runs after the retire and DirectoryStore never sees DirectoryEntry::Removed for self. Restart that node and the check at main.rs:276-286 — 'A node whose own entry is a tombstone was retired... Refuse the start' — reads a persisted directory with no self-tombstone and lets it boot as a zombie: in nobody's directory, resolvable by nobody, refused only on the restart *after* gossip has re-taught it its own tombstone. tk-c47e's reviewer flagged this shape for the tombstoned-on-its-behalf path and parked it on tk-ad1d; it also applies to the self-retirement path, which is the one the guard was written for. Either persist before cancelling, or the guard is decoration. No test covers main.rs:276 at all.

3. boot_counter.rs:74-88: advance() only warns when the counter write fails, on the reasoning that 'the self-publication wins ties back on the following tick'. merge_directory (registry.rs:170-174) gives ties to the *incoming* entry, and gossip_request re-snapshots the registry per exchange (gossip.rs:243), so between ticks the node forwards a peer's stale address about itself — which the module doc at boot_counter.rs:14-25 calls the unrecoverable failure that justifies failing the boot. Narrow (needs counter-write failure plus an address change), but the comment claims a guarantee the merge rule does not give. The same tie hole makes the wipe-plus-move caveat oscillate rather than simply lose when the peer's rank is equal; boot_incarnation.rs:176 only pins the strictly-higher case.

Stated plainly, not a fail on its own: the epic's actual goal — an operator restarts a node at a new --cluster-addr and the cluster follows — is covered only in pieces. boot_incarnation.rs drives real FjallStore reopens and the real BootCounter but merges into bare ShardRegistry objects with no Raft; network.rs:409 proves one client re-dials a moved target against a live EchoRaft; three_node_cluster proves addresses are reported out of the directory. Nothing composes them: no test restarts a member of a running Raft cluster at a new address and shows replication resume. Given main.rs's startup path is untested by construction (tk-abf8's own acceptance box admits this for bootstrap_members), the operator-visible feature is inferred from three separately-verified halves.

Accepted as-is: the storage-format break (KvCommand::Split source_members, shard map moving to the node keyspace, DirectoryEntry) is covered by the epic's on-disk-compatibility non-goal; the tombstone's absolute ordering, the delivered==0 refusal and the self-address guard in do_remove_node all match tk-c47e Q2/Q3; no test was deleted, skipped or loosened (split_carries_member_ids_to_new_shard gained a raw-bytes scan, client_addr_comes_from_membership_without_gossip was replaced by a stronger directory-sourced assertion); registry.rs:209 node_id_at still has no production caller but now earns its keep as tk-c47e's address-reuse assertion.
