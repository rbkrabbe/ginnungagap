+++
id = "tk-bf4e"
title = "serve_client/serve_cluster take no shutdown hook, so nothing can close a server in-process"
kind = "task"
status = "open"
size = "m"
priority = 2
blocked_by = []
tags = []
created = "2026-10-09T17:42:13+00:00"
spec_approved = false
review = "none"
touched = []
discovered_from = "tk-92e0"
+++
## Context

`serve_client_with_listener` and `serve_cluster_with_listener`
(`ggap-server/src/lib.rs:228` and its client counterpart) return
`anyhow::Result<()>` and run until the future is dropped. Neither takes a
`CancellationToken`, and neither uses tonic's
`serve_with_incoming_shutdown`, so there is no way to ask a server to stop.

In production that is survivable: `main.rs` cancels its own token to stop the
gossip task and then the process exits, taking the listeners with it. In a
test it is not. Aborting the server future closes the listener but leaves
tonic's per-connection tasks running, and each holds a clone of the router,
which holds the Raft group, which holds `Arc<FjallStore>`. While any peer has
a live connection the data dir stays locked.

Measured while writing tk-92e0: a node of a running three-node cluster, raft
shut down and every spawned handle aborted and awaited, still could not reopen
its own data dir 15 s later (`FjallError: Locked`). tk-92e0 works around it by
handing the `Arc<FjallStore>` to the replacement node instead of reopening,
which costs that test its only unfaithful step.

Worth having for its own sake too: a node that is asked to retire currently
has no way to stop serving before the process ends, which is the same gap
tk-8c62 runs into from the storage side.

## Why tk-92e0 waits on this

tk-92e0 Q1 is answered: its test is to be upgraded to a true
close-and-reopen once this lands. Its `restart_at_new_address` helper hands
the replacement node a live `Arc<FjallStore>`; with a shutdown hook it should
close the store and reopen the data dir instead, which is the step that makes
the test the operator action rather than a model of it.

## Blast radius

`crates/ggap-server/src/lib.rs` — both `serve_*_with_listener` functions and
the two convenience wrappers; `crates/ggap-node/src/main.rs` passes its
existing `shutdown` token. Callers that construct them:
`three_node_cluster.rs`, `rpc_metrics.rs`, `trace_propagation.rs`,
`benches/kv_write.rs`.

## Acceptance

- [ ] Both serve functions accept a cancellation token and stop serving when
      it fires, connections included.
- [ ] A test can shut a node down and reopen its data dir without
      `FjallError: Locked`.
- [ ] `ggap-node` passes its existing shutdown token through.
- [ ] Full checklist green: fmt, clippy -D warnings, build, test.
