# ggap-node

The runnable binary. Owns startup sequencing, configuration loading, and the
top-level `tokio::try_join!` that keeps both servers alive.

## Configuration layering

Configuration is resolved with `figment` in this precedence order (later
sources win):

1. `config/default.toml` — embedded at compile time via `include_str!`; always
   present and provides safe defaults for every key.
2. File specified with `--config <path>` — operator overrides for a specific
   deployment.
3. Environment variables with the prefix `GINNUNGAGAP_`, using `__` as the
   section separator (`GINNUNGAGAP_STORAGE__DATA_DIR`, etc.).

CLI flags (`--node-id`, `--client-addr`, `--client-listen-addr`, `--cluster-addr`,
`--cluster-listen-addr`, `--peer`, `--data-dir`)
are parsed separately with `clap` and take effect regardless of the file
configuration. They do not flow through `figment`.

## Startup sequence

1. Parse CLI and load/merge configuration.
2. Initialise the tracing subscriber (format determined by
   `observability.log_format`: `"json"` or pretty).
3. Start the Prometheus metrics recorder/exporter on `observability.metrics_addr`
   (when set).
4. Parse socket addresses and ensure the data directory exists.
5. Open `FjallStore`; load the `ShardMap` (initialising the default shard on
   first boot).
   Advance the boot counter in the `node` keyspace: the incarnation this node
   publishes its own addresses at (see below). This step can fail the start —
   see "Changing a node's address".
6. Create the shared state machine + Watch broadcast channel and the split-event
   channel, then build the `ShardRouter`.
6b. Restore the persisted directory. **If it holds this node's own tombstone,
   the node comes up quiesced** — see "A retired node" below.
7. For every shard in the `ShardMap`, start an `OpenRaftNode` (`GgapRaft`) over
   the fjall log store and state machine. On first boot each shard initialises
   its membership from persisted `bootstrap_members` (split-created shards), as a
   single-voter seed (`--seed`), or stays uninitialised to be joined later. A
   quiesced node starts none.
8. `tokio::try_join!` the gRPC servers (client + cluster). Either server failing
   causes the process to exit; `shutdown` is broadcast to background tasks.

## A retired node

A node retired through `AdminService.RemoveNode` **does not exit**. It quiesces:
KV, Admin and Raft answer `UNAVAILABLE`, no Raft group runs, and gossip keeps
serving so the tombstone reaches peers that were unreachable when it was
written. The process stays up and idle until an operator removes the host.

Exiting would accomplish nothing. Under a supervisor an exit is a restart —
a StatefulSet pod may only have `restartPolicy: Always` — and the restart would
find the tombstone in its persisted directory and quiesce anyway. Exiting only
adds a pod flap, or a crash loop if the restart refuses instead. Scaling the
StatefulSet down does not help either: it removes the *highest* ordinal, so a
retired middle node cannot be dropped without taking healthy nodes with it.

Both routes into the state are the same state: `RemoveNode` on a running node,
and a restart that reads its own tombstone. That is deliberate — a pod
rescheduled for any reason (drain, OOM, node failure) lands in the same place
rather than looping.

**A quiesced node looks healthy to a probe.** It holds its PVC and its
ordinal, and it answers its metrics endpoint — that is on neither gRPC
listener, so retirement does not affect it.

Scrape `ggap_node_retired`: `1` on a quiesced node, `0` on a serving one. Do
not read the `raft_*` gauges for this. Retirement stops the task that
publishes them, so they freeze at their last values and a retired node looks
exactly like an idle healthy one.

To put the host back in service, start it under a fresh `--node-id`: a retired
id is never reused.

## Changing a node's address

`--cluster-addr` and `--client-addr` are published by the node itself into the
directory, stamped with an incarnation taken from the boot counter in the data
dir. Restarting with a new address is therefore all an operator has to do: the
restart publishes above the address the peers hold, and they converge on the new
one.

**Not if the data dir was wiped in the same move.** A node starting from an empty
data dir publishes at incarnation 1 again, which loses to the higher incarnation
its peers already hold for that id, and the new address never takes. Give the
node a fresh `--node-id` when a wipe and an address change happen together —
and remove the old id from each shard's membership, since nothing else will.

### `cannot establish this node's incarnation`

The node refused to start because it could not settle on a rank to publish at.
Two causes reach this message, and they are recovered differently. Both are
deliberately an outage rather than a silent failure, because a node that starts
anyway looks healthy while being unable to correct its own address.

**1. The boot counter is present and cannot be read.** The node cannot tell
what rank it last published; starting at 1 would publish below whatever its
peers hold, and it would never win authorship of its own address back.

Nothing recovers this. The rank is not read back from the persisted directory —
that record lags, so it could name a rank this node has already published at.
Recover by treating it as a wipe: clear the data dir and start the host under a
fresh `--node-id`, removing the old id from each shard's membership. **Wiping
alone is not enough**: the count restarts at 1, which loses to the ranks the
peers hold, so the id has to change with it. The id is burned because the node
cannot prove what it published before.

**2. The rank could not be written back.** This boot would be ranked correctly;
the next one is the problem, because it would read the stale counter and
reissue the same number, and an equal rank keeps the entry already held — so an
address changed between those two boots would never install, with no symptom
but a move that did not happen.

*Do not wipe anything.* Nothing was published at the refused rank, and the
next start establishes one normally. Which rank it takes depends on how far the
write got, and neither case needs your attention: if it failed before
recording, the counter still holds the previous value and the next boot takes
the rank that was refused; if it recorded and then failed to flush, the counter
holds the new value and the next boot takes one *above* it, skipping a rank.
Skipping is harmless — ranks only have to increase.

The data directory is almost certainly full or read-only, which the Raft log
will not survive either. Restore write access and restart; the node keeps its
id, its data and its place in every membership.

## Known limitations

- No cluster-wide placement/rebalancing driver (`ggap-pd`) — shard placement is
  driven by explicit `AdminService` calls and splits.
- TLS is not configured on either server.
- The retained `PrometheusHandle` is not yet consumed by an in-process
  metrics-read transport.
