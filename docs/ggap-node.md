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
6. Create the shared state machine + Watch broadcast channel and the split-event
   channel, then build the `ShardRouter`.
7. For every shard in the `ShardMap`, start an `OpenRaftNode` (`GgapRaft`) over
   the fjall log store and state machine. On first boot each shard initialises
   its membership from persisted `bootstrap_members` (split-created shards), as a
   single-voter seed (`--seed`), or stays uninitialised to be joined later.
8. `tokio::try_join!` the gRPC servers (client + cluster). Either server failing
   causes the process to exit; `shutdown` is broadcast to background tasks.

## Known limitations

- No cluster-wide placement/rebalancing driver (`ggap-pd`) — shard placement is
  driven by explicit `AdminService` calls and splits.
- TLS is not configured on either server.
- The retained `PrometheusHandle` is not yet consumed by an in-process
  metrics-read transport.
