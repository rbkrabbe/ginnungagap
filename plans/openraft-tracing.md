# Plan: link the user trace through openraft's task boundary

## Context

`crates/ggap-server/src/tracing_layer.rs:1-18` documents a known gap in the OpenTelemetry pipeline: when `KvService::put` calls `Raft::client_write`, openraft schedules its `AppendEntries` fan-out on internal tokio tasks that carry no tracing context. As a result, the leader's outbound `rpc.client.append_entries` span starts a **new trace** instead of chaining under the inbound `rpc.server` span, and per-entry apply work on the leader and on followers also runs detached. The follower's inbound server span chains under the leader's *new* outbound span (so the consensus leg is internally connected), but the connection back to the original user request is broken.

This plan fixes it by storing trace metadata in-memory on the leader, keyed by Raft log index. `KvCommand` itself stays unchanged — no trace data is serialized into the log or replicated to followers. When the leader applies or sends AppendEntries, it looks up the metadata from the store and re-anchors the span. Result: a single trace (with baggage) from inbound user RPC → leader propose → outbound AppendEntries → follower server span → follower apply, plus leader apply.

Baggage is propagated through this same path; baggage propagation in unrelated paths (e.g. the inbound `OtelServerLayer`, generic outbound RPCs) is intentionally out of scope and remains missing for now.

## Approach

**Store trace metadata in-memory on the leader, keyed by `(shard_id, log_index)`.** `KvCommand` remains untouched — no new fields, no serialization overhead, no log pollution or bincode compat issues.

**Key insight:** Followers don't need trace metadata for their own applies. Their inbound `rpc.server` span is already chained to the leader's outbound `rpc.client.append_entries` span (which we re-anchor to the user trace), so the trace connection is preserved end-to-end. Only the leader needs to remember "this entry came from request X with trace Y" so it can re-anchor its own apply and network spans.

The trace_context helpers use a **locally-constructed composite propagator** (`TraceContextPropagator` + `BaggagePropagator`) for inject/extract, so capture and re-extraction of all three headers work regardless of what the global `TextMapPropagator` is set to. This matches the "in this path only" scope: we don't touch the global propagator or the inbound `OtelServerLayer`.

### Trace context flow after the fix

1. **Capture on leader** in `KvServiceImpl::do_put` / `do_delete` / `do_compare_and_swap`: read `traceparent`/`tracestate`/`baggage` from the current span's OTel context (the `rpc.server` span set by `OtelServerLayer`). After `client_write()` returns with `LogId { index, ... }`, immediately store the captured context in `RequestMetadataStore`, keyed by `(shard_id, log_index)`.
2. **Leader outbound span** in `GgapNetwork::append_entries`: scan `rpc.entries` for the first `EntryPayload::Normal` entry. For that entry's log index, look up the metadata in `RequestMetadataStore` (if present — it's only populated on the leader). If found, extract a parent `opentelemetry::Context` and call `span.set_parent(parent_cx)` on the `rpc.client.append_entries` span before `.instrument(span).await`. The existing `MetadataInjector` then injects this re-rooted context into outbound gRPC metadata, so the follower's `rpc.server` span chains to the user trace.
3. **Apply-time re-anchor on leader** in `GgapStateMachine::apply`: for each `EntryPayload::Normal(cmd)`, look up the metadata for this entry's log index. Build an `apply.entry` span, set its parent to the extracted context (if found), and `.instrument` the `fsm.apply(...)` call. This is per-entry because a single batch may carry multiple distinct traces. Baggage from the original request is now in scope for any code that reads `Span::current().context()` during apply.
4. **Followers apply unmodified**: Followers apply normally without metadata lookup (they didn't originate the request). Their spans are already chained to the user trace via the leader's outbound `rpc.client.append_entries` span + inbound `rpc.server` span.

## File changes

### `crates/ggap-consensus/src/trace_context.rs` (new file)
Three helpers + a shared composite propagator + private `Injector`/`Extractor`:

- A module-private `fn composite_propagator() -> TextMapCompositePropagator` (or a `OnceLock`-cached instance) returning `TextMapCompositePropagator::new(vec![Box::new(TraceContextPropagator::new()), Box::new(BaggagePropagator::new())])`. Used for both helpers so capture/extract handle all three keys (`traceparent`, `tracestate`, `baggage`) regardless of the global propagator setup.

- `pub fn capture_current_trace_context() -> (Option<String>, Option<String>, Option<String>)` — uses `Span::current().context()` + the local composite propagator's `inject_context(...)` into a `HashMap`-backed `Injector`. Returns `(traceparent, tracestate, baggage)`.

- `pub(crate) fn extract_parent_context(traceparent: Option<&str>, tracestate: Option<&str>, baggage: Option<&str>) -> Option<opentelemetry::Context>` — short-circuits on all-`None`; otherwise wraps the strings in a `StaticExtractor` that returns `Some(&str)` for the three known keys and lets the local composite propagator extract a `Context`.

- `struct StaticExtractor` (local, private).

Crate dep additions in `crates/ggap-consensus/Cargo.toml`: `opentelemetry-sdk` (for `BaggagePropagator`) if not already present. Verify against current `Cargo.toml` during implementation.

Re-export both helpers from `crates/ggap-consensus/src/lib.rs`.

### `crates/ggap-server/src/metadata_store.rs` (new file)
A thread-safe, in-memory cache of request metadata keyed by `(ShardId, log_index)`:

```rust
pub struct RequestMetadataStore {
    // Map<(ShardId, u64), (traceparent, tracestate, baggage)>
    // Use a simple RwLock<HashMap>; entries are short-lived (lookup after apply completes)
    // and the map can be cleared periodically or after apply
}

impl RequestMetadataStore {
    pub fn new() -> Self { /* ... */ }
    pub fn store(&self, shard_id: ShardId, log_index: u64, 
                 traceparent: Option<String>, tracestate: Option<String>, baggage: Option<String>) { /* ... */ }
    pub fn take(&self, shard_id: ShardId, log_index: u64) -> Option<(Option<String>, Option<String>, Option<String>)> { /* ... */ }
}
```

The `take` method removes the entry after retrieving it, preventing unbounded growth. On the leader, callers store after `client_write()` returns and take at apply time.

Export from `crates/ggap-server/src/lib.rs` so `ggap-node` can instantiate and pass it to both the KvService and RaftNode.

### `crates/ggap-server/src/kv_service.rs:77-224`
After `client_write()` returns in `do_put` / `do_delete` / `do_compare_and_swap`:
```rust
let (traceparent, tracestate, baggage) = ggap_consensus::capture_current_trace_context();
self.metadata_store.store(self.shard_id, client_write_response.log_id().index,
                          traceparent, tracestate, baggage);
```

No changes to `KvCommand` construction.

### `crates/ggap-consensus/src/network.rs:108-149` (`append_entries` only)
Before building the `rpc.client.append_entries` span, scan entries for the first `EntryPayload::Normal`:
```rust
let parent_cx = rpc.entries.iter().find_map(|e| match &e.payload {
    EntryPayload::Normal(_) => {
        self.metadata_store.lookup(self.shard_id, e.log_id.index)
            .and_then(|(tp, ts, bg)| extract_parent_context(tp.as_deref(), ts.as_deref(), bg.as_deref()))
    }
    _ => None,
});
let span = tracing::info_span!(
    "rpc.client.append_entries", otel.kind = "client",
    "rpc.system" = "grpc",
    "rpc.method" = "ginnungagap.v1.RaftService/AppendEntries",
    shard_id = self.shard_id,
);
if let Some(cx) = parent_cx { span.set_parent(cx); }
```

Note: `lookup` (not `take`) because we might need the metadata again for apply. Separate store, add a `lookup` method.

### `crates/ggap-consensus/src/state_machine.rs:89-103` (`EntryPayload::Normal` arm in `apply`)
Wrap the `fsm.apply(...).await` in an `apply.entry` span, looking up the metadata for this entry's log index. Add `use tracing::Instrument;` and `use tracing_opentelemetry::OpenTelemetrySpanExt;`. Sketch:
```rust
EntryPayload::Normal(cmd) => {
    let converted_log_id = convert::or_log_id_to_log_id(log_id);
    let span = tracing::info_span!(
        "apply.entry", otel.kind = "internal",
        shard_id, raft_index = converted_log_id.index,
    );
    if let Some((tp, ts, bg)) = self.metadata_store.take(self.shard_id, converted_log_id.index) {
        if let Some(cx) = extract_parent_context(tp.as_deref(), ts.as_deref(), bg.as_deref()) {
            span.set_parent(cx);
        }
    }
    let resp = match self.fsm
        .apply(shard_id, converted_log_id, Some(cmd.clone()), None)
        .instrument(span)
        .await
    { /* existing match */ };
    responses.push(resp);
}
```

The `take` call here removes the metadata after use, so it's not kept indefinitely. Note that `GgapStateMachine` already has a reference to the metadata store (it's part of the RaftNode builder).

### No changes to `KvCommand`
`KvCommand` and all its match sites remain untouched. No new fields, no serialization overhead.

### Comment update at `crates/ggap-server/src/tracing_layer.rs:1-18`
Remove the "Limitation: openraft worker boundary" paragraph; add a short note that `KvService` handlers capture trace context and store it by log index, then the leader's outbound `rpc.client.append_entries` span and `apply.entry` spans re-extract it to maintain the trace connection (see `ggap_consensus::trace_context` and `ggap_server::metadata_store`).

## Reused existing pieces

- `MetadataInjector` and the `global::get_text_map_propagator(...).inject_context(...)` pattern at `crates/ggap-consensus/src/network.rs:23-33` (reused as the model for the new `HashMap`-backed `Injector`).
- `OtelServerLayer` extraction at `crates/ggap-server/src/tracing_layer.rs:72-91` (already extracts inbound `traceparent`/`tracestate`; nothing to change in the layer itself for this phase).
- `tracing_opentelemetry::OpenTelemetrySpanExt::{context, set_parent}` (already imported in `network.rs:4`; just add to `state_machine.rs` and `kv_service.rs`).

## Verification

1. **Build & lint** (per CLAUDE.md pre-push checklist):
   - `cargo fmt --all`
   - `cargo clippy --all-targets --all-features -- -D warnings`
   - `cargo build --all-targets`
   - `cargo test --all`

2. **Tighten existing test** at `crates/ggap-server/tests/trace_propagation.rs::outbound_raft_rpc_carries_injected_trace_context` (lines 252-301): replace the loose "non-zero trace_id" assertion (lines 293-300) with an assertion that *every* `rpc.client.*` span emitted on or after the Put has the **injected** `trace_id`. This becomes a precise regression guard for the fix.

3. **New multi-node test** in `crates/ggap-server/tests/trace_propagation.rs` (or extend `three_node_cluster.rs` with the OTel test pipeline). The test pipeline in `install_test_pipeline` (lines 52-71) sets only `TraceContextPropagator`; for this test, install a composite (`TraceContextPropagator` + `BaggagePropagator`) so the inbound layer extracts baggage too — this lets the assertions below verify end-to-end baggage flow even though the production inbound layer remains TraceContext-only. Steps:
   - Start a 3-node cluster.
   - Issue a `Put` against the leader with both an injected `traceparent` header *and* an injected `baggage` header (e.g. `user_id=alice`).
   - Wait for spans, then assert all of:
     - The leader's `rpc.server` span shares the injected `trace_id` *and* its parent is the injected span.
     - At least one `rpc.client.append_entries` span shares the injected `trace_id`.
     - At least one `apply.entry` span on the leader shares the injected `trace_id`, and the baggage entry `user_id=alice` is observable from `Span::current().context()` inside an instrumented assertion hook during apply.
     - At least one `apply.entry` span on a follower (any node id ≠ leader) shares the injected `trace_id` and the same baggage entry.

4. **Manual end-to-end smoke** via Kind + OTel Collector + Grafana (`scripts/local-deploy/`): `kubectl exec` a `grpcurl` Put with a synthetic `traceparent` header and confirm in Grafana Tempo (or the Collector debug exporter) that the trace shows: `rpc.server` (KvService/Put) → `rpc.client.append_entries` → `rpc.server` (RaftService/AppendEntries) → `apply.entry` on each replica.

## Architectural notes

- **Metadata store ownership**: `RequestMetadataStore` is instantiated in `ggap-node/src/main.rs` and passed (via `Arc`) to both `KvServiceImpl` (in `serve_client`) and `GgapNetwork` (in the RaftNode builder). This requires small signature changes to both but is mechanically straightforward.
- **Metadata lifetime**: Entries are stored after `client_write()` returns and retrieved (via `take`) at apply time. Transient growth is bounded by replication latency (typically milliseconds). No explicit cleanup is needed; entries are removed on use.
- **No log pollution**: Trace metadata never enters `KvCommand` or the Raft log, avoiding all serialization concerns and backwards-compatibility issues.
