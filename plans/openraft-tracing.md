# Plan: link the user trace through openraft's task boundary

## Context

`crates/ggap-server/src/tracing_layer.rs:1-18` documents a known gap in the OpenTelemetry pipeline: when `KvService::put` calls `Raft::client_write`, openraft schedules its `AppendEntries` fan-out on internal tokio tasks that carry no tracing context. As a result, the leader's outbound `rpc.client.append_entries` span starts a **new trace** instead of chaining under the inbound `rpc.server` span, and per-entry apply work on the leader and on followers also runs detached. The follower's inbound server span chains under the leader's *new* outbound span (so the consensus leg is internally connected), but the connection back to the original user request is broken.

The comment notes the fix requires plumbing trace context **into `KvCommand`**. This plan does exactly that: stamp W3C `traceparent`/`tracestate`/`baggage` onto each `KvCommand` at the gRPC handler, re-extract them on the leader's outbound `append_entries` span, and re-extract them on every replica's `apply` path. Result: a single trace (with baggage) from inbound user RPC → leader propose → outbound AppendEntries → follower server span → follower apply, plus leader apply.

Baggage is propagated through this same path; baggage propagation in unrelated paths (e.g. the inbound `OtelServerLayer`, generic outbound RPCs) is intentionally out of scope and remains missing for now.

## Approach

**Add `traceparent: Option<String>`, `tracestate: Option<String>`, and `baggage: Option<String>` to each user-facing `KvCommand` variant (`Put`, `Delete`, `Cas`).** Leave `Split` unchanged (it's an internal split-coordinator entry, not user-traceable). Do **not** wrap `KvCommand` — that would change openraft's `D` type (in `GgapTypeConfig`) and ripple through every storage trait, every match arm, and every `From<KvCommand>` site across `ggap-consensus`/`ggap-storage`. Per-variant fields are mechanically additive: most existing match sites already use named-field destructuring so the change is local.

The new fields are plain `Option<String>`s — `ggap-types` keeps zero gRPC and zero opentelemetry deps (CLAUDE.md hard constraint).

The trace_context helpers (below) use a **locally-constructed composite propagator** (`TraceContextPropagator` + `BaggagePropagator`) for inject/extract, so capture and re-extraction of all three headers work regardless of what the global `TextMapPropagator` is set to. This matches the "in this path only" scope: we don't touch the global propagator or the inbound `OtelServerLayer`.

### Trace context flow after the fix

1. **Capture** in `KvServiceImpl::do_put` / `do_delete` / `do_compare_and_swap`: read `traceparent`/`tracestate`/`baggage` from the current span's OTel context (the `rpc.server` span set by `OtelServerLayer`) and stamp them onto the constructed `KvCommand`.
2. **Replicate**: openraft serializes the command into the Raft log; followers receive identical bytes (deterministic).
3. **Leader outbound span** in `GgapNetwork::append_entries`: scan `rpc.entries` for the first `EntryPayload::Normal(cmd)` with a non-`None` `traceparent`. Use it (plus `tracestate` and `baggage`) to extract a parent `opentelemetry::Context` and call `span.set_parent(parent_cx)` on the `rpc.client.append_entries` span before `.instrument(span).await`. The existing `MetadataInjector` then injects this re-rooted context into outbound gRPC metadata, so the follower's `rpc.server` span and downstream `apply.entry` span chain to the user trace (and carry baggage when the global propagator includes `BaggagePropagator`).
4. **Apply-time re-anchor** in `GgapStateMachine::apply` (leader and every follower): for each `EntryPayload::Normal(cmd)`, build an `apply.entry` span, set its parent to the extracted context, and `.instrument` the `fsm.apply(...)` call. This is per-entry because a single batch may carry multiple distinct traces. Baggage from the original request is now in scope for any code that reads `Span::current().context()` during apply.

## File changes

### `crates/ggap-types/src/lib.rs:74-108`
Add three fields to each of `Put`, `Delete`, `Cas`:
```rust
traceparent: Option<String>,
tracestate: Option<String>,
baggage: Option<String>,
```
Leave `Split` unchanged. Add small accessor methods (also in this file, so zero new deps):
```rust
impl KvCommand {
    pub fn traceparent(&self) -> Option<&str> {
        match self {
            KvCommand::Put { traceparent, .. }
            | KvCommand::Delete { traceparent, .. }
            | KvCommand::Cas { traceparent, .. } => traceparent.as_deref(),
            KvCommand::Split { .. } => None,
        }
    }
    pub fn tracestate(&self) -> Option<&str> { /* same shape */ }
    pub fn baggage(&self) -> Option<&str> { /* same shape */ }
}
```

### `crates/ggap-consensus/src/trace_context.rs` (new file)
Two helpers + a shared composite propagator + private `Injector`/`Extractor`:
- A module-private `fn composite_propagator() -> TextMapCompositePropagator` (or a `OnceLock`-cached instance) returning `TextMapCompositePropagator::new(vec![Box::new(TraceContextPropagator::new()), Box::new(BaggagePropagator::new())])`. Used for both helpers so capture/extract handle all three keys (`traceparent`, `tracestate`, `baggage`) regardless of the global propagator setup.
- `pub fn capture_current_trace_context() -> (Option<String>, Option<String>, Option<String>)` — uses `Span::current().context()` + the local composite propagator's `inject_context(...)` into a `HashMap`-backed `Injector`. Returns `(traceparent, tracestate, baggage)`.
- `pub(crate) fn extract_parent_context(traceparent: Option<&str>, tracestate: Option<&str>, baggage: Option<&str>) -> Option<opentelemetry::Context>` — short-circuits on all-`None`; otherwise wraps the strings in a `StaticExtractor` that returns `Some(&str)` for the three known keys and lets the local composite propagator extract a `Context`.
- `struct StaticExtractor` (local, private).

Crate dep additions in `crates/ggap-consensus/Cargo.toml`: `opentelemetry-sdk` (for `BaggagePropagator`) if not already present (`TraceContextPropagator` lives in `opentelemetry_sdk::propagation` too). Verify against the current `Cargo.toml` during implementation.

Re-export from `crates/ggap-consensus/src/lib.rs` so `ggap-server` can call `capture_current_trace_context()`.

### `crates/ggap-server/src/kv_service.rs:77-224`
Before each `KvCommand::{Put,Delete,Cas} { ... }` literal in `do_put` / `do_delete` / `do_compare_and_swap`:
```rust
let (traceparent, tracestate, baggage) = ggap_consensus::capture_current_trace_context();
let cmd = KvCommand::Put { /* existing fields */, traceparent, tracestate, baggage };
```

### `crates/ggap-consensus/src/network.rs:108-149` (`append_entries` only)
Before building the `rpc.client.append_entries` span, scan entries for the first user-traceable command:
```rust
let parent_cx = rpc.entries.iter().find_map(|e| match &e.payload {
    EntryPayload::Normal(cmd) => {
        extract_parent_context(cmd.traceparent(), cmd.tracestate(), cmd.baggage())
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
Leave `vote` and `install_snapshot` alone — votes have no client trace; snapshots are infrastructure.

### `crates/ggap-consensus/src/state_machine.rs:89-103` (`EntryPayload::Normal` arm in `apply`)
Wrap the `fsm.apply(...).await` in an `apply.entry` span whose parent is the entry's extracted context. Add `use tracing::Instrument;` and `use tracing_opentelemetry::OpenTelemetrySpanExt;`. Sketch:
```rust
EntryPayload::Normal(cmd) => {
    let converted_log_id = convert::or_log_id_to_log_id(log_id);
    let span = tracing::info_span!(
        "apply.entry", otel.kind = "internal",
        shard_id, raft_index = converted_log_id.index,
    );
    if let Some(cx) = extract_parent_context(cmd.traceparent(), cmd.tracestate(), cmd.baggage()) {
        span.set_parent(cx);
    }
    let resp = match self.fsm
        .apply(shard_id, converted_log_id, Some(cmd.clone()), None)
        .instrument(span)
        .await
    { /* existing match */ };
    responses.push(resp);
}
```
The tracing dispatch travels into `tokio::task::spawn_blocking` inside `FjallStateMachine::apply` automatically.

### Mechanical match-site updates (add `..` or `traceparent: _, tracestate: _, baggage: _`)
- `crates/ggap-consensus/src/lib.rs` (`StubRaftNode::propose` arms)
- `crates/ggap-consensus/src/split.rs` (any `KvCommand` matches)
- `crates/ggap-storage/src/mem.rs` (apply match arms)
- `crates/ggap-storage/src/fjall.rs:415-585` (apply match arms)
- `crates/ggap-storage/src/ttl.rs` — TTL-driven `KvCommand::Delete` construction stamps `traceparent: None, tracestate: None, baggage: None` (TTL GC isn't tied to a user request)

### Tests to update with `traceparent: None, tracestate: None, baggage: None` (or `..`)
- `crates/ggap-consensus/tests/single_node.rs`
- `crates/ggap-consensus/tests/sim_cluster.rs`
- `crates/ggap-storage/tests/split_crash_bugs.rs`
- `crates/ggap-server/tests/three_node_cluster.rs`
- `crates/ggap-server/tests/split_single_node.rs`

### Comment update at `crates/ggap-server/src/tracing_layer.rs:1-18`
Remove the "Limitation: openraft worker boundary" paragraph; add a one-sentence note that `KvService` handlers stamp `traceparent`/`tracestate`/`baggage` onto each `KvCommand`, and that the leader's outbound `rpc.client.append_entries` span and every `apply.entry` span re-extract that context as their parent (see `ggap_consensus::trace_context`).

## Reused existing pieces

- `MetadataInjector` and the `global::get_text_map_propagator(...).inject_context(...)` pattern at `crates/ggap-consensus/src/network.rs:23-33` (reused as the model for the new `HashMap`-backed `Injector`).
- `OtelServerLayer` extraction at `crates/ggap-server/src/tracing_layer.rs:72-91` (already extracts inbound `traceparent`/`tracestate`; nothing to change in the layer itself for this phase).
- `tracing_opentelemetry::OpenTelemetrySpanExt::{context, set_parent}` (already imported in `network.rs:4`; just add to `state_machine.rs` and `kv_service.rs`).
- `bincode` derived from existing `serde::{Serialize, Deserialize}` on `KvCommand` — new fields ride on existing serialization.

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

## Known risk to flag in the PR description

`KvCommand` is bincode-serialized into the Raft log. Bincode is positional, so adding fields to existing variants is **not backwards-compatible** with logs/snapshots written by older binaries. Per CLAUDE.md the project is in Phase 6 hardening and Phase 7 (multi-raft) is explicitly deferred — there is no documented log-compat policy and existing tests use tempdirs. Wipe-on-upgrade is acceptable for this phase. Call this out in the PR description so reviewers can sign off explicitly.
