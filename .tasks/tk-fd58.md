+++
id = "tk-fd58"
title = "add_learner stops discarding NodeInfo.client_addr"
kind = "task"
status = "done"
size = "m"
priority = 2
blocked_by = ["tk-441f"]
tags = []
created = "2026-07-30T19:07:52+00:00"
spec_approved = false
review = "pass"
touched = ["crates/ggap-consensus/src/node.rs", "crates/ggap-consensus/src/config.rs", "crates/ggap-server/src/admin_service.rs", "crates/ggap-node/src/main.rs", "crates/ggap-consensus/src/gossip.rs", "crates/ggap-server/tests/three_node_cluster.rs", "docs/ggap-consensus.md", "deploy/k8s/bootstrap/job.yaml", "deploy/README.md"]
parent = "tk-d08a"
base = "eaaed87db6e9016c0dd6e23275f8af7aad417ccb"
reviewed_at = "2026-07-31T18:38:13+00:00"
closed = "2026-07-31T19:13:46+00:00"
+++
## Context

The payoff child: the first point at which a client address reaches committed
consensus state.

`admin_service.rs:134` passes `node_info.cluster_addr` into `add_learner` and
drops `node_info.client_addr` on the floor. The proto has carried both since the
beginning (`NodeInfo`, types.proto:14-18) — nothing on the wire changes here, the
server just stops discarding half of what it is given.

- `node.rs:160` — `add_learner(node_id, addr: String)` becomes
  `add_learner(node_id, addrs: NodeAddrs)`.
- `admin_service.rs:132-135` — pass both fields through.
- `main.rs` — cluster bootstrap constructs `GgapNode` with both addresses.
- Reject an `AddLearner` whose `client_addr` is empty, or accept it? An empty one
  is now a node nothing can forward to, and it was silently normal until this
  child. Decide in the task, and prefer loud.

## Decision: an empty `client_addr` is rejected

`AdminService::add_learner` returns `InvalidArgument("client_addr must not be
empty")`, alongside the `node_id` and `cluster_addr` checks already there. Now
that membership is the source of truth for the client address, admitting a
learner without one produces a member nothing can forward a client request to,
and nothing later fills the gap in — no gossip self-seed reaches consensus
state. Failing the join is the only loud moment available; accepting it defers
the surprise to whoever first tries to forward.

Validation stays at the RPC boundary. `OpenRaftNode::add_learner` takes whatever
`NodeAddrs` it is handed, matching how the other two argument checks are layered.

The seed holds itself to the same rule. `ggap-node` now *fails startup* when
`derive_client_addr` returns `None`, where it previously warned and carried on
with an empty string — which, since this task, would put a member with no client
address into the initial membership and leave it there. The only way to reach
that path is a malformed `--cluster-addr` / `--client-addr`, so a working
deployment cannot notice.

## Acceptance

- [x] A learner added via `AddLearner` appears in Raft membership with both
      addresses.
      → `add_learner_rpc_puts_both_addresses_in_membership`
- [x] With the gossip task never started, a node reports a peer's client address
      correctly from membership alone. This is the assertion the whole epic is
      for — it must not rely on gossip having run.
      → `client_addr_comes_from_membership_without_gossip`, which first asserts
      the follower's directory holds no gossiped client address for any peer.
- [x] An `AddLearner` carrying an empty `client_addr` behaves as the task decides,
      and a test pins that behaviour.
      → `add_learner_rpc_rejects_empty_client_addr`
- [x] Full checklist green: fmt, clippy -D warnings, build, test.

## Notes

- `GossipTask::refresh_local` now merges full `NodeAddrs` from membership rather
  than cluster-only entries. The field-wise merge in `merge_directory` still
  earns its keep: a split-created shard's `bootstrap_members` remains
  cluster-only until tk-10b7.
- `main.rs` derives the advertised client address before any Raft group starts,
  since seed bootstrap now needs it. tk-d049 replaces the derivation itself.
- The k8s bootstrap Job was the only automated `AddLearner` caller and sent no
  `clientAddr`; under `set -eu` the new rejection would have aborted the Job
  before `ChangeMembership`. `deploy/k8s/bootstrap/job.yaml` now sends both
  addresses, taking the client address from the same pod host with port 17000
  (the advertised form of `--client-addr 0.0.0.0:17000` in the ggap ConfigMap).
  The console's `AddLearner` form already collects both; a blank field surfaces
  the server's `InvalidArgument` as a toast.
- Discovered, filed, not fixed here: tk-cb4e (deploy docs name ggap-0 as the
  seed; it is ggap-2) and tk-b282 (root README still calls the membership RPCs
  UNIMPLEMENTED).

## Review 2026-07-31T18:32:34+00:00 — fail

Category 3 (callers): the new empty-client_addr rejection breaks the repo's only automated AddLearner caller.

- crates/ggap-server/src/admin_service.rs:143-145 now returns InvalidArgument when node.client_addr is empty. deploy/k8s/bootstrap/job.yaml:51 sends -d "{\"node\":{\"nodeId\":${id},\"clusterAddr\":\"${addr}\"}}" — no clientAddr at all. The script runs under 'set -eu', so both AddLearner calls now fail and the Job aborts before ChangeMembership; the documented k8s bootstrap (deploy/README.md:95) never forms a 3-voter cluster. Root README.md:137/142 already passes client_addr and is fine; job.yaml is the one that was missed. It needs the pod's advertised client address (ggap-${ord}.ggap-headless.ginnungagap.svc.cluster.local:17000, matching --client-addr 0.0.0.0:17000 in deploy/k8s/ggap/configmap.yaml:23) added to the payload.

Secondary, not the reason for the fail but worth deciding on:
- crates/ggap-node/src/main.rs:189-199 — when derive_client_addr returns None the code warns and seeds initial membership with an empty client_addr (GgapNode::from(self_addrs) at main.rs:288-291). The task's decision is that a member with no client address is a loud failure; a seed node admits itself with one silently-empty and nothing later fills it in. Either fail startup or record why the seed is exempt.

Checked and accepted: fmt/clippy -D warnings/build/test --all all green locally; the three new tests (add_learner_rpc_puts_both_addresses_in_membership, client_addr_comes_from_membership_without_gossip, add_learner_rpc_rejects_empty_client_addr) run and genuinely exercise the claims — the no-gossip variant asserts the follower's directory is empty for peers first, so membership is the only path. All Rust callers of the changed signatures (OpenRaftNode::add_learner, ClusterStatus.voters/learners) were re-checked: gossip.rs:208-228, admin_service.rs:67/71/264-265, three_node_cluster.rs, rpc_metrics.rs:294 (still asserts InvalidArgument via node_id 0). No tests deleted, skipped or loosened; touched list matches the diff exactly.

## Review 2026-07-31T18:38:13+00:00 — pass

Re-review after both findings fixed. Base eaaed87.

Primary finding closed. deploy/k8s/bootstrap/job.yaml:46-60 now sends {"nodeId":N,"clusterAddr":"HOST:17001","clientAddr":"HOST:17000"}. Verified by extracting data.bootstrap.sh from the ConfigMap and running 'sh -n' on it (clean) plus reading the expanded payload: 'clientAddr'/'clusterAddr' are the correct proto3 JSON names for NodeInfo.client_addr/cluster_addr (proto/ginnungagap/v1/types.proto:14-18), which is what grpcurl accepts. The id/ord mapping is self-consistent with deploy/k8s/ggap/configmap.yaml:11 (NODE_ID=ORDINAL+1), so id=1 -> ggap-0 and id=2 -> ggap-1 really are those nodes' own addresses, not another pod's. Swept every other AddLearner caller in the repo, not just Rust: console/src/screens/Membership.tsx:85 already sends clientAddr from a prefilled form field (line 32/302); README.md:137/142 already carried client_addr; crates/ggap-server/tests/rpc_metrics.rs:294 still asserts InvalidArgument (via node_id 0). Nothing else issues the RPC.

Secondary finding closed. crates/ggap-node/src/main.rs:197-204 now fails startup with context instead of defaulting to an empty string, and the reason is recorded in the task's Decision section. Confirmed nothing legitimate reaches it: --client-addr is already parsed as a SocketAddr at main.rs:166-169 and --cluster-addr already goes through lookup_host at main.rs:174-178 unless --cluster-listen-addr is set, so the only inputs that reach the new bail are a malformed --cluster-addr paired with a listen override, or an empty host like ':17001'. Defaults (0.0.0.0:17000 / 0.0.0.0:17001) derive fine, and gossip.rs:409-415 pins that wildcard case.

Checklist re-run green: fmt --check, clippy --all-targets --all-features -D warnings, test --all (exit 0), and three_node_cluster 17/17 including the three acceptance tests.

Accepted as-is: the fatal-startup path has no test, because ggap-node is a binary with no test harness and the path is unreachable with well-formed arguments; the behaviour is recorded in the Decision section instead. The two filed follow-ups are correctly out of this commit — tk-b282 (README:153 UNIMPLEMENTED note) is untouched by this change, and tk-cb4e covers the deploy-doc seed drift, which also covers deploy/README.md:95 still naming node_id 2/3 and pods ggap-1/ggap-2 where the Job actually adds node_id 1/2 for ggap-0/ggap-1. That line was edited here, so it is worth doing tk-cb4e soon, but pulling it in would mix an unrelated doc correction into an address-plumbing commit.
