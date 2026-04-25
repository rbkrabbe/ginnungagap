use std::collections::HashMap;

use opentelemetry::{
    propagation::{Extractor, Injector, TextMapCompositePropagator, TextMapPropagator},
    Context,
};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use tracing_opentelemetry::OpenTelemetrySpanExt;

fn composite_propagator() -> TextMapCompositePropagator {
    TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ])
}

struct HashMapExtractor<'a>(&'a HashMap<String, String>);

impl<'a> Extractor for HashMapExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

struct HashMapInjector<'a>(&'a mut HashMap<String, String>);

impl<'a> Injector for HashMapInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

/// Serialize the active tracing span's OTel context into W3C headers.
///
/// Uses a locally-constructed composite propagator (TraceContext + Baggage)
/// so the capture is independent of the global propagator setup.
pub fn capture_current_trace_context() -> (Option<String>, Option<String>, Option<String>) {
    let cx = tracing::Span::current().context();
    let mut map = HashMap::new();
    composite_propagator().inject_context(&cx, &mut HashMapInjector(&mut map));
    let traceparent = map.remove("traceparent");
    let tracestate = map.remove("tracestate");
    let baggage = map.remove("baggage");
    (traceparent, tracestate, baggage)
}

/// Reconstruct an OTel `Context` from W3C trace headers.
///
/// Returns `None` when all three arguments are `None` (no-op path for
/// non-leader applies and cases where capture was skipped).
pub fn extract_parent_context(
    traceparent: Option<&str>,
    tracestate: Option<&str>,
    baggage: Option<&str>,
) -> Option<Context> {
    if traceparent.is_none() && tracestate.is_none() && baggage.is_none() {
        return None;
    }
    let mut map = HashMap::new();
    if let Some(v) = traceparent {
        map.insert("traceparent".to_owned(), v.to_owned());
    }
    if let Some(v) = tracestate {
        map.insert("tracestate".to_owned(), v.to_owned());
    }
    if let Some(v) = baggage {
        map.insert("baggage".to_owned(), v.to_owned());
    }
    Some(composite_propagator().extract(&HashMapExtractor(&map)))
}
