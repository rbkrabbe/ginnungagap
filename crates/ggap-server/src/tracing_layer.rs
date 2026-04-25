//! Tower layer that extracts W3C `traceparent`/`tracestate` from inbound gRPC
//! request headers and drives each request inside a `rpc.server` tracing span.
//!
//! The layer is applied to both the client-facing (KV) and cluster (Raft/Admin)
//! tonic servers, so all inbound RPCs — including Raft peer-to-peer calls —
//! participate in trace propagation.
//!
//! # Trace continuity across the openraft task boundary
//!
//! When a `KvService::put` triggers `Raft::client_write`, openraft schedules its
//! `AppendEntries` fan-out on internal tokio tasks that carry no tracing context.
//! The trace gap is bridged without touching `KvCommand` (no log pollution):
//!
//! 1. `OpenRaftNode::propose()` captures `traceparent`/`tracestate`/`baggage`
//!    from the active `rpc.server` span immediately after `client_write` returns,
//!    and stores them in `RequestMetadataStore` keyed by `(shard_id, log_index)`.
//! 2. `GgapNetwork::append_entries` looks up that context by the first Normal
//!    entry's log index and calls `span.set_parent(cx)` before instrumenting the
//!    outbound RPC — so the follower's inbound `rpc.server` span chains under the
//!    original user trace.
//! 3. `GgapStateMachine::apply` similarly re-anchors an `apply.entry` span for
//!    each Normal entry on the leader, making baggage visible during apply.
//!
//! See `ggap_consensus::trace_context` and `ggap_consensus::metadata_store`.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use opentelemetry::global;
use opentelemetry_http::HeaderExtractor;
use tower::{Layer, Service};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

// ---------------------------------------------------------------------------
// Layer
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
pub struct OtelServerLayer;

impl<S> Layer<S> for OtelServerLayer {
    type Service = OtelServer<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OtelServer { inner }
    }
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct OtelServer<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for OtelServer<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // Extract W3C parent context from the request headers.
        let parent_cx =
            global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(req.headers())));

        // Build a server-side span keyed on the gRPC method path.
        let path = req.uri().path().to_owned();
        let span = tracing::info_span!(
            "rpc.server",
            otel.name = %path,
            otel.kind = "server",
            "rpc.system" = "grpc",
            "rpc.method" = %path,
        );
        span.set_parent(parent_cx);

        let fut = self.inner.call(req);
        Box::pin(fut.instrument(span))
    }
}
