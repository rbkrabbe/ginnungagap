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
7. For every shard in the `ShardMap`, start an `OpenRaftNode` (`GgapRaft`) over
   the fjall log store and state machine. On first boot each shard initialises
   its membership from persisted `bootstrap_members` (split-created shards), as a
   single-voter seed (`--seed`), or stays uninitialised to be joined later.
8. `tokio::try_join!` the gRPC servers (client + cluster). Either server failing
   causes the process to exit; `shutdown` is broadcast to background tasks.

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

The node refused to start because neither the boot counter nor the persisted
directory could be read, so it cannot tell what rank it last published. Starting
anyway would publish at 1, below whatever the peers hold, and the node would
spend its life unable to correct its own address while looking healthy — so this
is deliberately an outage rather than a silent one.

Both records live in the `node` keyspace, so losing both points at the data dir
rather than at one key. Recover by treating it as a wipe: clear the data dir and
start the node under a fresh `--node-id`, removing the old id from each shard's
membership.

## Known limitations

- No cluster-wide placement/rebalancing driver (`ggap-pd`) — shard placement is
  driven by explicit `AdminService` calls and splits.
- TLS is not configured on either server.
- The retained `PrometheusHandle` is not yet consumed by an in-process
  metrics-read transport.
