use std::net::SocketAddr;

use anyhow::Context;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Install a process-global Prometheus recorder and spawn the scrape endpoint.
///
/// Returns `Ok(None)` when `addr` is `None` (metrics disabled). Callers should
/// await the returned handle after the main servers shut down so in-flight
/// samples are flushed before exit.
pub fn init_metrics_recorder(
    addr: Option<SocketAddr>,
    shutdown: CancellationToken,
) -> anyhow::Result<Option<JoinHandle<()>>> {
    let Some(addr) = addr else {
        tracing::info!("metrics endpoint disabled (empty metrics_addr)");
        return Ok(None);
    };

    let (recorder, exporter) = PrometheusBuilder::new()
        .with_http_listener(addr)
        .build()
        .context("failed to build Prometheus exporter")?;

    metrics::set_global_recorder(recorder)
        .map_err(|e| anyhow::anyhow!("metrics recorder already installed: {e}"))?;

    tracing::info!(%addr, "metrics endpoint listening");

    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = exporter => {}
            _ = shutdown.cancelled() => {
                tracing::info!("metrics exporter shutting down");
            }
        }
    });

    Ok(Some(handle))
}
