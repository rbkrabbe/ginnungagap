use std::time::Duration;

use tonic::Status;

/// Counter: total number of `KvService` unary RPCs completed, labeled by method and status.
pub const RPC_TOTAL: &str = "ggap_kv_requests_total";

/// Histogram: `KvService` unary RPC latency in seconds, labeled by method and status.
pub const RPC_DURATION: &str = "ggap_kv_request_duration_seconds";

/// Map a handler result into a stable, low-cardinality status label.
///
/// Labels are derived mechanically from the returned `tonic::Code` so the
/// mapping stays in sync with `convert::ggap_to_status` without duplicating
/// its logic. When new `Code`s appear they fall through to `"internal"`.
pub fn status_label(err: Option<&Status>) -> &'static str {
    let Some(err) = err else { return "ok" };
    use tonic::Code::*;
    match err.code() {
        Ok => "ok",
        NotFound => "not_found",
        InvalidArgument | OutOfRange => "invalid",
        Aborted => "conflict",
        Unavailable | DeadlineExceeded => "unavailable",
        FailedPrecondition => "failed_precondition",
        _ => "internal",
    }
}

/// Record a counter increment and a latency observation for one RPC.
pub fn record(method: &'static str, status: &'static str, elapsed: Duration) {
    metrics::counter!(RPC_TOTAL, "method" => method, "status" => status).increment(1);
    metrics::histogram!(RPC_DURATION, "method" => method, "status" => status)
        .record(elapsed.as_secs_f64());
}
