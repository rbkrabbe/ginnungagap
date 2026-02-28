# ggap-proto

Generated gRPC bindings and a compiled file descriptor set. Contains no logic.

## Source layout

```
proto/ginnungagap/v1/
├── types.proto    — KeyValue, ResponseHeader, ReadConsistency, WriteQuorum
├── kv.proto       — KvService (client-facing)
└── cluster.proto  — RaftService + AdminService (internal cluster traffic)
```

`build.rs` locates `protoc` via `protoc-bin-vendored` (no system protoc
required) and emits `descriptor.bin` alongside the generated Rust sources.
The descriptor is embedded as `ggap_proto::FILE_DESCRIPTOR_SET` and registered
with the tonic server-reflection service so tools like `grpcurl` and Postman
can introspect the schema at runtime.

## Wire contract

**`KvService`** is exposed on the client-facing port (default `:17000`).

| RPC | Notes |
|-----|-------|
| `Get` | `at_version = 0` returns current; non-zero requests an exact historical version (MVCC). |
| `Put` | `expect_version = 0` is unconditional; non-zero enforces optimistic concurrency. `ttl_secs = 0` means no TTL. |
| `Delete` | Always succeeds; `found = false` when the key was already absent. |
| `Scan` | Keyed pagination via `page_token` (opaque bytes = UTF-8 continuation key). |
| `Watch` | Bidirectional stream — Phase 5. Currently returns `UNIMPLEMENTED`. |
| `CompareAndSwap` | Value-level swap; returns current value regardless of success. |

**`RaftService`** and **`AdminService`** are exposed on the cluster-facing port
(default `:17001`) and are not reachable from outside the cluster.

`RaftMessage` carries opaque bytes so the openraft wire format does not bleed
into the proto schema. Phase 4 is responsible for encoding/decoding.

## Versioning invariant

Proto types must not leak into crates other than `ggap-proto` and `ggap-server`.
`ggap-types` defines the internal domain types; `ggap-server/src/convert.rs`
handles the boundary mapping. This keeps the rest of the codebase free of
protobuf concerns and avoids a transitive dependency on `prost`.
