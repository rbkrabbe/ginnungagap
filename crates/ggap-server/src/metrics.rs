use std::sync::Arc;
use std::time::Duration;

use tonic::Status;

/// Counter: total number of `KvService` unary RPCs completed, labeled by method and status.
pub const RPC_TOTAL: &str = "ggap_kv_requests_total";

/// Histogram: `KvService` unary RPC latency in seconds, labeled by method and status.
pub const RPC_DURATION: &str = "ggap_kv_request_duration_seconds";

/// Record a counter increment and a latency observation for one RPC.
///
/// The `status` label is the native `tonic::Code` variant name (e.g. `"Ok"`,
/// `"NotFound"`, `"InvalidArgument"`) taken directly from `{:?}`; no
/// intermediate bucketing or case-conversion.
pub fn record<T>(method: &'static str, result: &Result<T, Status>, elapsed: Duration) {
    let code = result
        .as_ref()
        .err()
        .map(|s| s.code())
        .unwrap_or(tonic::Code::Ok);
    let status: Arc<str> = format!("{code:?}").into();
    metrics::counter!(RPC_TOTAL, "method" => method, "status" => status.clone()).increment(1);
    metrics::histogram!(RPC_DURATION, "method" => method, "status" => status)
        .record(elapsed.as_secs_f64());
}
