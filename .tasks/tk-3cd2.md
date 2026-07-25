+++
id = "tk-3cd2"
title = "NotLeader carries the leader's node id, not just an address"
kind = "task"
status = "open"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-07-25T10:14:05+00:00"
spec_approved = false
review = "none"
touched = []
parent = "tk-3846"
+++
## Context

## Problem

GgapError::NotLeader carries only Option<String> — the address openraft embedded
in the ForwardToLeader error. A forwarder needs the leader's *identity* so it can
look the current address up in the gossip directory, rather than trusting an
address that was correct when the error was constructed. Without this, forwarding
would dial a stale addr with no way to notice.

## Changes

- crates/ggap-types/src/lib.rs:188 — NotLeader gains `leader_id: Option<u64>`
  alongside the existing `leader: Option<String>`.
- crates/ggap-consensus/src/node.rs — the four forward_to_leader() call sites
  (:107 ensure_linearizable_or_lease, :157 add_learner, :185 change_membership,
  :221 propose) populate leader_id from `fwd.leader_id`.
- crates/ggap-consensus/src/split.rs:150 — same, if it constructs NotLeader.
- crates/ggap-server/src/convert.rs:46 — emit the id as a `ggap-leader-id`
  metadata entry beside the existing ggap-leader-addr, so the hint is complete
  for callers that can act on it.

## Acceptance

- [ ] NotLeader carries both leader_id and leader; all five construction sites set both.
- [ ] ggap_to_status emits ggap-leader-id alongside ggap-leader-addr.
- [ ] Existing follower tests in three_node_cluster.rs assert the id is present
      and matches the address the same response carries.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.

## Risks

Every match on GgapError::NotLeader must be updated — grep says convert.rs is the
only consumer today, but the struct variant means a missed site is a compile
error, not a silent bug.

## Acceptance

- [ ] TODO
