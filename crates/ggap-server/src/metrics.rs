//! OpenTelemetry-aligned Prometheus metrics for inbound gRPC calls.
//!
//! Metric: [`rpc.server.call.duration`][spec] (histogram, unit: seconds).
//! Prometheus exposition name: `rpc_server_call_duration_seconds` —
//! `.`→`_` sanitization plus the `_seconds` unit suffix.
//!
//! Attributes (written with their canonical OTel dotted names; the Prometheus
//! exporter sanitizes `.` to `_` on export):
//!
//! - `rpc.system.name = "grpc"`
//! - `rpc.method` — **fully qualified** `<package>.<service>/<Method>`, e.g.
//!   `"ginnungagap.v1.KvService/Get"`. The separate `rpc.service` attribute
//!   is intentionally folded into `rpc.method` here so queries only need one
//!   label to identify a call site.
//! - `rpc.response.status_code` — numeric gRPC status code (0–16) stringified.
//!
//! Request count is derived from the histogram's `_count` suffix; no separate
//! counter is emitted.
//!
//! [spec]: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/

use std::sync::Arc;
use std::time::Duration;

use tonic::Status;

/// OTel canonical metric ID: `rpc.server.call.duration` (seconds).
pub const RPC_SERVER_CALL_DURATION: &str = "rpc_server_call_duration_seconds";

/// Record a latency observation for one inbound gRPC call.
///
/// `method` is the fully qualified gRPC method, e.g.
/// `"ginnungagap.v1.KvService/Get"`.
pub fn record<T>(method: &'static str, result: &Result<T, Status>, elapsed: Duration) {
    let code: i32 = result
        .as_ref()
        .err()
        .map(|s| s.code())
        .unwrap_or(tonic::Code::Ok)
        .into();
    let status_code: Arc<str> = code.to_string().into();
    metrics::histogram!(
        RPC_SERVER_CALL_DURATION,
        "rpc.system.name" => "grpc",
        "rpc.method" => method,
        "rpc.response.status_code" => status_code,
    )
    .record(elapsed.as_secs_f64());
}
