use std::time::Duration;

use tonic::Status;

/// Counter: total number of `KvService` unary RPCs completed, labeled by method and status.
pub const RPC_TOTAL: &str = "ggap_kv_requests_total";

/// Histogram: `KvService` unary RPC latency in seconds, labeled by method and status.
pub const RPC_DURATION: &str = "ggap_kv_request_duration_seconds";

/// Map a handler result into the snake_case name of its `tonic::Code`.
///
/// 1:1 with the gRPC status code space (16 codes × 5 methods = 80 tuples),
/// which is comfortably within Prometheus cardinality budgets and avoids
/// judgment calls about how to bucket related codes.
pub fn status_label(err: Option<&Status>) -> &'static str {
    let Some(err) = err else { return "ok" };
    use tonic::Code::*;
    match err.code() {
        Ok => "ok",
        Cancelled => "cancelled",
        Unknown => "unknown",
        InvalidArgument => "invalid_argument",
        DeadlineExceeded => "deadline_exceeded",
        NotFound => "not_found",
        AlreadyExists => "already_exists",
        PermissionDenied => "permission_denied",
        ResourceExhausted => "resource_exhausted",
        FailedPrecondition => "failed_precondition",
        Aborted => "aborted",
        OutOfRange => "out_of_range",
        Unimplemented => "unimplemented",
        Internal => "internal",
        Unavailable => "unavailable",
        DataLoss => "data_loss",
        Unauthenticated => "unauthenticated",
    }
}

/// Record a counter increment and a latency observation for one RPC.
pub fn record(method: &'static str, status: &'static str, elapsed: Duration) {
    metrics::counter!(RPC_TOTAL, "method" => method, "status" => status).increment(1);
    metrics::histogram!(RPC_DURATION, "method" => method, "status" => status)
        .record(elapsed.as_secs_f64());
}
