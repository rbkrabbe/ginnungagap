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
//! - `rpc.response.status_code` — gRPC status name, e.g. `"OK"`,
//!   `"NOT_FOUND"`, `"UNAVAILABLE"`.
//!
//! Request count is derived from the histogram's `_count` suffix; no separate
//! counter is emitted.
//!
//! [spec]: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/

use std::time::Duration;

use tonic::Status;

/// OTel canonical metric ID: `rpc.server.call.duration` (seconds).
pub const RPC_SERVER_CALL_DURATION: &str = "rpc_server_call_duration_seconds";

/// Record a latency observation for one inbound gRPC call.
///
/// `method` is the fully qualified gRPC method, e.g.
/// `"ginnungagap.v1.KvService/Get"`.
pub fn record<T>(method: &'static str, result: &Result<T, Status>, elapsed: Duration) {
    let code = result
        .as_ref()
        .err()
        .map(|s| s.code())
        .unwrap_or(tonic::Code::Ok);
    metrics::histogram!(
        RPC_SERVER_CALL_DURATION,
        "rpc.system.name" => "grpc",
        "rpc.method" => method,
        "rpc.response.status_code" => code_name(code),
    )
    .record(elapsed.as_secs_f64());
}

fn code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "CANCELLED",
        tonic::Code::Unknown => "UNKNOWN",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Aborted => "ABORTED",
        tonic::Code::OutOfRange => "OUT_OF_RANGE",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::Internal => "INTERNAL",
        tonic::Code::Unavailable => "UNAVAILABLE",
        tonic::Code::DataLoss => "DATA_LOSS",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
    }
}
