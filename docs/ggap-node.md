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

CLI flags (`--node-id`, `--client-addr`, `--cluster-addr`, `--peer`, `--data-dir`)
are parsed separately with `clap` and take effect regardless of the file
configuration. They do not flow through `figment`.

## Startup sequence

1. Parse CLI.
2. Load and merge configuration.
3. Initialise the tracing subscriber (format determined by
   `observability.log_format`: `"json"` or pretty).
4. Parse socket addresses.
5. Construct the `RaftNode` (currently `StubRaftNode`).
6. `tokio::try_join!` the two gRPC servers. Either server failing causes the
   process to exit.

## What is not yet wired (Phase 4+)

- `StorageConfig` fields (`data_dir`, `max_key_bytes`, etc.) are parsed but not
  used — `FjallStore` is not opened yet.
- `RaftConfig`, `ConsistencyConfig`, `ServerConfig` are parsed but not passed
  anywhere.
- Peers (`--peer`) are parsed but not registered with openraft.
- Metrics (`observability.metrics_addr`) are not exported.
- TLS is not configured on either server.
