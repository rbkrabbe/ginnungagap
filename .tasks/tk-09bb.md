+++
id = "tk-09bb"
title = "A retired node's state has no metric, only a log line"
kind = "task"
status = "dropped"
size = "s"
priority = 2
blocked_by = []
tags = []
created = "2026-10-11T14:51:56+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-4822"
resized_from = "m"
+++
## Context

A node retired by tk-4822 quiesces instead of exiting, which means it keeps
passing its liveness probe, keeps its PVC and keeps its StatefulSet ordinal. A
cluster with one retired node still looks like N nodes to Kubernetes.

Today an operator can tell only from the startup/retire log line.
`docs/ggap-node.md` says to look there, which is honest but weak: logs rotate,
and the one line that matters may be hours old on a pod that has been idle
since.

The metrics endpoint is the right place and already works — it is on neither
gRPC listener, so quiescing does not affect it. What is missing is anything to
read: nothing consumes `_metrics_handle` in `ggap-node/src/main.rs` yet and
there is no node-level gauge to extend.

So this is really "add the first node-level gauge, and make the retired state
its first value". Worth doing with whatever else wants one rather than alone,
which is why tk-4822 left it out.

## Acceptance

- [ ] A scrape distinguishes a quiesced node from a serving one.
- [ ] `docs/ggap-node.md`'s "A retired node" section names the metric rather
      than pointing at the log line.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.

## Dropped (2026-10-11)

Done inside tk-4822 instead. Its review found the visibility acceptance box
ticked while this task recorded that no metric existed — and that the fix for
an earlier finding had made it worse, since stopping `RaftMetricsTask` on
retirement freezes the `raft_*` gauges and leaves a retired node scraping
identically to an idle healthy one.

`metrics` was already a dependency of `ggap-node` and the facade is a single
call, so deferring was the wrong trade: `ggap_node_retired` is set at startup
(0 or 1, so the series exists on a healthy node) and on retirement.
`docs/ggap-node.md` names it and warns against reading the `raft_*` gauges
for this.

The broader framing here — "add the first node-level gauge" — was overstated.
Nothing had to be built first.
