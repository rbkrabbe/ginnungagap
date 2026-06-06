use std::net::SocketAddr;

use anyhow::Context;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    runtime,
    trace::{Sampler, TracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::ObservabilityConfig;

// ---------------------------------------------------------------------------
// TracingGuard — holds the OTel provider so we can flush on shutdown
// ---------------------------------------------------------------------------

pub enum TracingGuard {
    None,
    Otel(TracerProvider),
}

impl TracingGuard {
    pub fn shutdown(self) {
        if let TracingGuard::Otel(provider) = self {
            if let Err(e) = provider.shutdown() {
                eprintln!("OTLP tracer shutdown error: {e}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// init_tracing
// ---------------------------------------------------------------------------

/// Install the global tracing subscriber and, when `tracing_otlp_endpoint` is
/// non-empty, also set up the OTLP gRPC exporter and attach a
/// `tracing_opentelemetry` layer.
///
/// Returns a [`TracingGuard`] that **must** be held until the process is ready
/// to exit and then dropped/shutdown to flush in-flight spans.
pub fn init_tracing(config: &ObservabilityConfig, node_id: u64) -> anyhow::Result<TracingGuard> {
    let json = config.log_format == "json";
    let filter = EnvFilter::new(&config.log_level);

    if config.tracing_otlp_endpoint.is_empty() {
        // No OTLP endpoint — plain fmt subscriber only (today's behavior).
        if json {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().pretty())
                .init();
        }
        return Ok(TracingGuard::None);
    }

    // Build OTLP gRPC span exporter.
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.tracing_otlp_endpoint)
        .build()
        .context("failed to build OTLP span exporter")?;

    let resource = Resource::new_with_defaults([
        KeyValue::new(SERVICE_NAME, "ginnungagap"),
        KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        // service.instance.id uses the string constant directly because
        // opentelemetry-semantic-conventions 0.27 gates it behind
        // semconv_experimental.
        KeyValue::new("service.instance.id", node_id.to_string()),
    ]);

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio)
        .with_resource(resource)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    if json {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("ginnungagap")))
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().pretty())
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("ginnungagap")))
            .init();
    }

    Ok(TracingGuard::Otel(provider))
}

// ---------------------------------------------------------------------------
// init_metrics_recorder
// ---------------------------------------------------------------------------

const HIST_BUCKETS: &[f64] = &[
    // 5 steps/decade: 0.1ms – 100ms
    0.0001, 0.000158, 0.000251, 0.000398, 0.000631, //
    0.001, 0.00158, 0.00251, 0.00398, 0.00631, //
    0.01, 0.0158, 0.0251, 0.0398, 0.0631, 0.1, //
    // 3 steps/decade: 100ms – 5s
    0.215, 0.464, 1.0, 2.15, 4.64,
];

fn configured_builder() -> anyhow::Result<PrometheusBuilder> {
    PrometheusBuilder::new()
        .set_buckets(HIST_BUCKETS)
        .context("failed to configure histogram buckets")
}

/// Install a process-global Prometheus recorder and, when `addr` is `Some`,
/// spawn the scrape endpoint.
///
/// The `PrometheusHandle` is **always** returned so callers can render metrics
/// even when the HTTP listener is disabled (e.g. to expose them via a different
/// transport). Callers should hold the handle for the lifetime of the process.
///
/// When `addr` is `Some` the optional `JoinHandle` should be awaited after the
/// main servers shut down so in-flight samples are flushed before exit.
pub fn init_metrics_recorder(
    addr: Option<SocketAddr>,
    shutdown: CancellationToken,
) -> anyhow::Result<(PrometheusHandle, Option<JoinHandle<()>>)> {
    if let Some(addr) = addr {
        let (recorder, exporter) = configured_builder()?
            .with_http_listener(addr)
            .build()
            .context("failed to build Prometheus exporter")?;

        let handle = recorder.handle();
        metrics::set_global_recorder(recorder)
            .map_err(|e| anyhow::anyhow!("metrics recorder already installed: {e}"))?;

        tracing::info!(%addr, "metrics endpoint listening");

        let task = tokio::spawn(async move {
            tokio::select! {
                _ = exporter => {}
                _ = shutdown.cancelled() => {
                    tracing::info!("metrics exporter shutting down");
                }
            }
        });

        Ok((handle, Some(task)))
    } else {
        tracing::info!("metrics endpoint disabled (empty metrics_addr)");
        let recorder = configured_builder()?.build_recorder();
        let handle = recorder.handle();
        metrics::set_global_recorder(recorder)
            .map_err(|e| anyhow::anyhow!("metrics recorder already installed: {e}"))?;
        Ok((handle, None))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorder_renders_recorded_metrics() {
        // Build a recorder locally without touching the process-global recorder
        // so this test is safe to run alongside others.
        let recorder = configured_builder()
            .expect("bucket config is valid")
            .build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("ggap_test_counter_total").increment(1);
            metrics::histogram!("ggap_test_duration_seconds").record(0.005);
        });

        let rendered = handle.render();
        assert!(
            rendered.contains("ggap_test_counter_total"),
            "counter missing from render output:\n{rendered}"
        );
        assert!(
            rendered.contains("ggap_test_duration_seconds"),
            "histogram missing from render output:\n{rendered}"
        );
    }
}
