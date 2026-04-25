//! Tower layer that extracts W3C `traceparent`/`tracestate` from inbound gRPC
//! request headers and drives each request inside a `rpc.server` tracing span.
//!
//! The layer is applied to both the client-facing (KV) and cluster (Raft/Admin)
//! tonic servers, so all inbound RPCs — including Raft peer-to-peer calls —
//! participate in trace propagation.
//!
//! # Limitation: openraft worker boundary
//!
//! When a `KvService::put` triggers `Raft::client_write`, openraft schedules the
//! actual `AppendEntries` RPCs on an internal tokio task spawned without a
//! tracing span.  The outbound `rpc.client.*` spans emitted by `GgapNetwork`
//! therefore start a **new trace** on the leader rather than appearing as
//! children of the inbound `rpc.server` span.  However, the follower's
//! `RaftService/AppendEntries` server span *does* chain under the leader's
//! outbound client span, so the server→consensus→server leg is fully connected.
//! Linking the original client trace through openraft's task boundary would
//! require plumbing context into `KvCommand`; that is deferred to a future phase.

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
