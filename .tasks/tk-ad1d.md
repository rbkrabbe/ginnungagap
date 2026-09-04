+++
id = "tk-ad1d"
title = "Track descriptor freshness, and gate removal-on-behalf on a node being demonstrably dead"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-08-25T19:25:09+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-c47e"
+++
## Context

tk-c47e makes `RemoveNode` self-addressed: the receiving node forwards it to the
target, which checks its own memberships and authors its own tombstone. When the
target does not answer, the receiving node writes the tombstone on its behalf —
the only path that can retire failed hardware, and the one exception to sole
authorship of a descriptor.

That fallback fires on a single failed dial. A node partitioned for one second
is indistinguishable from one that is gone, and the tombstone tk-c47e writes is
absolute: a live-but-partitioned node retired this way never comes back under
its own id.

The directory records no freshness. Each entry is `node_id -> NodeDescriptor`
with an incarnation and nothing about when the entry was last confirmed — unlike
`ShardEntry`, which carries `last_updated` and reports `age_ms`. Gossip merges a
descriptor it already holds without recording that it heard it again.

## Shape

Record per directory entry when this node last received the descriptor from
gossip (`tokio::time::Instant`, never `std::time`, per CLAUDE.md), the way
`ShardEntry::last_updated` already works. A node whose descriptor has not been
re-heard for longer than a threshold — several gossip intervals — is presumed
dead. Then gate the removal-on-behalf fallback on that: an unreachable node that
peers heard from moments ago is refused, not retired.

Note the freshness must come from gossip about the node, not from dialling it,
so a node reachable by its peers but not by the node serving `RemoveNode` is
still protected.

## Acceptance

- [ ] Directory entries carry a last-heard timestamp, refreshed when gossip
      re-delivers a descriptor at an equal or higher incarnation.
- [ ] `RemoveNode` refuses the on-behalf fallback for a node whose descriptor is
      fresh, naming how recently it was heard.
- [ ] The threshold is configurable, and defaults to several gossip intervals.
- [ ] A node genuinely gone for longer than the threshold is still removable.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
