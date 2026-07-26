//! OpenTelemetry integration for metrics and traces plus an in-process
//! log query facade.
//!
//! Metrics/traces push over OTLP for `Backend::Remote`, skipped for
//! `Backend::Memory`. Logs go through `tracing_subscriber::fmt::json()`
//! to stdout; a K8s log shipper collects it in production. The memory
//! backend also feeds a copy of that JSON stream into a per-flow ring
//! buffer exposed via [`query::LogsStore`].

pub mod query;

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use std::sync::Arc;
use std::time::Duration;

/// Default per-flow log ring buffer capacity.
pub const MEMORY_LOG_CAPACITY: usize = 1024;

/// Default per-flow metric ring buffer capacity.
pub const MEMORY_METRIC_CAPACITY: usize = 1024;

/// Errors returned by telemetry initialization.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// OTLP metrics exporter build failed.
    #[error("Failed to initialize OpenTelemetry metrics: {source}")]
    MetricsInit {
        #[source]
        source: opentelemetry_sdk::metrics::MetricError,
    },
    /// OTLP trace exporter build failed.
    #[error("Failed to initialize OpenTelemetry tracer: {source}")]
    TracerInit {
        #[source]
        source: opentelemetry::trace::TraceError,
    },
}

/// One span in a log record's span chain, root-to-leaf.
///
/// Keeps span identity (`name`) with the fields declared on that span
/// (e.g. `task.run` carries `task`, `task_id`, `task_type`) so consumers
/// can classify by span topology without losing which span contributed
/// which field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSpan {
    /// Span name (e.g. `flow.run`, `task.run`, `task.handle`).
    pub name: String,
    /// Fields declared on this span, flattened to strings.
    pub fields: Vec<(String, String)>,
}

/// A single log record captured by a telemetry backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLog {
    /// Log record body (the tracing event's `message` field).
    pub body: String,
    /// Log level as emitted by tracing (lowercased: "info" | "warn" | "error").
    pub level: String,
    /// RFC3339 timestamp when present in the source line.
    pub timestamp: Option<String>,
    /// The tracing `target` (usually the module path).
    pub target: String,
    /// Span chain from root to leaf. Empty for events emitted outside any span.
    pub spans: Vec<StoredSpan>,
    /// Fields declared on the event itself (not inherited from spans).
    pub fields: Vec<(String, String)>,
}

/// Backend selection for telemetry signals.
#[derive(Debug, Clone)]
pub enum Backend {
    /// Push metrics and traces over OTLP/gRPC to a remote collector.
    /// Logs still go to stdout for a K8s log shipper to collect.
    Remote {
        /// gRPC endpoint of the collector.
        endpoint: String,
    },
    /// Keep everything in-process for demo and single-node dev.
    Memory {
        /// Log records retained per flow before oldest entries drop.
        logs_per_flow: usize,
        /// Metric samples retained per flow before oldest entries drop.
        metrics_per_flow: usize,
    },
}

/// OpenTelemetry configuration for metrics, tracing, and log export.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Backend selection.
    pub backend: Backend,
    /// Service name reported as the `service.name` resource attribute.
    pub service_name: String,
    /// Service version reported as the `service.version` resource attribute.
    pub service_version: String,
    /// How often the metrics reader pushes accumulated samples, in seconds.
    pub metrics_export_interval_secs: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            backend: Backend::Memory {
                logs_per_flow: MEMORY_LOG_CAPACITY,
                metrics_per_flow: MEMORY_METRIC_CAPACITY,
            },
            service_name: "flowgen".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            metrics_export_interval_secs: 60,
        }
    }
}

/// Guard + backend-specific query and writer handles.
pub struct Telemetry {
    /// Owns the OTel providers; drop to shut them down.
    pub guard: TelemetryGuard,
    /// Backend log query handle for the admin UI.
    pub logs_store: Option<Arc<dyn query::LogsStore>>,
    /// Writer the tracing `fmt` layer feeds a copy of every JSON log line.
    pub logs_writer: Option<query::MemoryLogsStoreWriter>,
}

/// Builds the telemetry providers for the selected backend and
/// returns a [`Telemetry`] handle owning them.
pub fn init_telemetry(config: TelemetryConfig) -> Result<Telemetry, Error> {
    let resource = Resource::new(vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", config.service_version.clone()),
    ]);

    match config.backend {
        Backend::Remote { ref endpoint } => build_remote(endpoint, &config, resource),
        Backend::Memory {
            logs_per_flow,
            metrics_per_flow,
        } => Ok(build_memory(logs_per_flow, metrics_per_flow)),
    }
}

fn build_remote(
    endpoint: &str,
    config: &TelemetryConfig,
    resource: Resource,
) -> Result<Telemetry, Error> {
    let metrics_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|source| Error::MetricsInit { source })?;

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(
            opentelemetry_sdk::metrics::PeriodicReader::builder(
                metrics_exporter,
                opentelemetry_sdk::runtime::Tokio,
            )
            .with_interval(Duration::from_secs(config.metrics_export_interval_secs))
            .build(),
        )
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let tracer = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|source| Error::TracerInit { source })?;

    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(tracer, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    Ok(Telemetry {
        guard: TelemetryGuard {
            meter_provider: Some(meter_provider),
            tracer_provider: Some(tracer_provider),
        },
        logs_store: None,
        logs_writer: None,
    })
}

fn build_memory(logs_per_flow: usize, _metrics_per_flow: usize) -> Telemetry {
    let (writer, query_handle) = query::pair(logs_per_flow);
    Telemetry {
        guard: TelemetryGuard {
            meter_provider: None,
            tracer_provider: None,
        },
        logs_store: Some(Arc::new(query_handle)),
        logs_writer: Some(writer),
    }
}

/// Owns the OTel meter and tracer providers; shuts them down on drop.
pub struct TelemetryGuard {
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<opentelemetry_sdk::trace::TracerProvider>,
}

impl TelemetryGuard {
    pub fn shutdown(self) -> Result<(), Error> {
        Ok(())
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(mp) = self.meter_provider.as_ref() {
            if let Err(e) = mp.shutdown() {
                tracing::warn!(error = %e, "Failed to shutdown meter provider");
            }
        }

        if let Some(tp) = self.tracer_provider.as_ref() {
            if let Err(e) = tp.shutdown() {
                tracing::warn!(error = %e, "Failed to shutdown tracer provider");
            }
        }

        opentelemetry::global::shutdown_tracer_provider();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_config_default_backend_is_memory() {
        let config = TelemetryConfig::default();
        assert!(matches!(
            config.backend,
            Backend::Memory {
                logs_per_flow: MEMORY_LOG_CAPACITY,
                metrics_per_flow: MEMORY_METRIC_CAPACITY,
            }
        ));
        assert_eq!(config.service_name, "flowgen");
        assert_eq!(config.metrics_export_interval_secs, 60);
    }

    #[test]
    fn test_telemetry_config_remote() {
        let config = TelemetryConfig {
            backend: Backend::Remote {
                endpoint: "http://otel-collector:4317".to_string(),
            },
            service_name: "flowgen-prod".to_string(),
            service_version: "1.0.0".to_string(),
            metrics_export_interval_secs: 30,
        };
        assert!(matches!(
            &config.backend,
            Backend::Remote { endpoint } if endpoint == "http://otel-collector:4317"
        ));
        assert_eq!(config.service_name, "flowgen-prod");
        assert_eq!(config.metrics_export_interval_secs, 30);
    }
}
