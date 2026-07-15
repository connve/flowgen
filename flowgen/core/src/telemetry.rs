//! OpenTelemetry integration for metrics, distributed tracing, and logs.
//!
//! Provides two backends selectable at init time:
//!
//! - `Backend::Otlp` — pushes metrics/traces/logs to an OTLP endpoint
//!   (e.g. an OpenTelemetry Collector fronting VictoriaLogs / VictoriaMetrics /
//!   Loki / Tempo).
//! - `Backend::Memory` — retains everything in-process, queryable via
//!   the returned `MemoryLogSink`. Used when telemetry is not configured
//!   so the app runs standalone without any collector.
//!
//! Regardless of backend, callers get a `TelemetryGuard` that owns the
//! providers. `TelemetryGuard::logger_provider()` is always available and
//! is what `flowgen::main::init_tracing` passes into the
//! `OpenTelemetryTracingBridge` layer.

use async_trait::async_trait;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::export::logs::{LogBatch, LogExporter};
use opentelemetry_sdk::logs::{LogRecord, LogResult, LoggerProvider};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Errors that can occur during telemetry initialization.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to build the OTLP metrics exporter.
    #[error("Failed to initialize OpenTelemetry metrics: {source}")]
    MetricsInit {
        #[source]
        source: opentelemetry_sdk::metrics::MetricError,
    },
    /// Failed to build the OTLP trace exporter.
    #[error("Failed to initialize OpenTelemetry tracer: {source}")]
    TracerInit {
        #[source]
        source: opentelemetry::trace::TraceError,
    },
    /// Failed to build the OTLP log exporter.
    #[error("Failed to initialize OpenTelemetry logger: {source}")]
    LoggerInit {
        #[source]
        source: opentelemetry_sdk::logs::LogError,
    },
}

/// Backend selection for OpenTelemetry signals.
///
/// `Memory` is the default when telemetry is not configured — the app
/// still emits into a `LoggerProvider`, but the records stay in-process
/// and can be inspected via the returned `MemoryLogSink`. This mirrors
/// the in-memory cache fallback used elsewhere in flowgen.
#[derive(Debug, Clone)]
pub enum Backend {
    /// Push all signals to an OTLP-compatible collector at `endpoint`.
    Otlp { endpoint: String },
    /// Keep all signals in-memory (no network I/O).
    Memory,
}

/// OpenTelemetry configuration for metrics, tracing, and log export.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Backend selection.
    pub backend: Backend,
    /// Service name for resource identification.
    pub service_name: String,
    /// Service version for resource identification.
    pub service_version: String,
    /// Metrics export interval in seconds (defaults to 60s). Ignored
    /// by the `Memory` backend, which does not batch metric exports.
    pub metrics_export_interval_secs: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            backend: Backend::Memory,
            service_name: "flowgen".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            metrics_export_interval_secs: 60,
        }
    }
}

/// Initializes OpenTelemetry providers for the selected backend.
///
/// Always returns a `TelemetryGuard`; the guard's `logger_provider()`
/// is what the tracing subscriber's `OpenTelemetryTracingBridge` layer
/// consumes.
pub fn init_telemetry(config: TelemetryConfig) -> Result<TelemetryGuard, Error> {
    let resource = Resource::new(vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", config.service_version.clone()),
    ]);

    match config.backend {
        Backend::Otlp { ref endpoint } => build_otlp(endpoint, &config, resource),
        Backend::Memory => Ok(build_memory(resource)),
    }
}

fn build_otlp(
    endpoint: &str,
    config: &TelemetryConfig,
    resource: Resource,
) -> Result<TelemetryGuard, Error> {
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
        .with_resource(resource.clone())
        .build();
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|source| Error::LoggerInit { source })?;

    let logger_provider = LoggerProvider::builder()
        .with_batch_exporter(log_exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();

    Ok(TelemetryGuard {
        meter_provider: Some(meter_provider),
        tracer_provider: Some(tracer_provider),
        logger_provider,
        memory_logs: None,
    })
}

fn build_memory(resource: Resource) -> TelemetryGuard {
    let sink = MemoryLogSink::new();
    let logger_provider = LoggerProvider::builder()
        .with_log_processor(
            opentelemetry_sdk::logs::BatchLogProcessor::builder(
                MemoryLogExporter::new(Arc::clone(&sink.inner)),
                opentelemetry_sdk::runtime::Tokio,
            )
            .build(),
        )
        .with_resource(resource)
        .build();

    TelemetryGuard {
        meter_provider: None,
        tracer_provider: None,
        logger_provider,
        memory_logs: Some(sink),
    }
}

/// Guard that owns the OpenTelemetry providers for the process lifetime.
///
/// Dropping the guard flushes and shuts down each provider. The
/// `Memory` backend also owns the in-memory sink so that consumers
/// (e.g. the web backend) can query retained records.
pub struct TelemetryGuard {
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<opentelemetry_sdk::trace::TracerProvider>,
    logger_provider: LoggerProvider,
    memory_logs: Option<MemoryLogSink>,
}

impl TelemetryGuard {
    /// Returns the logger provider so callers can install the
    /// `opentelemetry-appender-tracing` bridge into their `tracing`
    /// subscriber stack.
    pub fn logger_provider(&self) -> &LoggerProvider {
        &self.logger_provider
    }

    /// Returns the in-memory log sink when the guard was built with
    /// `Backend::Memory`. `None` for OTLP backends, whose logs are
    /// pushed to the configured collector.
    pub fn memory_logs(&self) -> Option<MemoryLogSink> {
        self.memory_logs.clone()
    }

    /// Explicitly shuts down telemetry, flushing all pending data.
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

        if let Err(e) = self.logger_provider.shutdown() {
            tracing::warn!(error = %e, "Failed to shutdown logger provider");
        }

        opentelemetry::global::shutdown_tracer_provider();
    }
}

/// Handle to the in-memory log buffer used by the `Memory` backend.
///
/// Cloning a sink yields another handle to the same underlying buffer,
/// so the web backend can hold one and the guard another without
/// coordinating their lifetimes.
#[derive(Debug, Clone)]
pub struct MemoryLogSink {
    inner: Arc<Mutex<Vec<StoredLog>>>,
}

impl MemoryLogSink {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a snapshot of every log record retained by the sink.
    pub fn snapshot(&self) -> Vec<StoredLog> {
        match self.inner.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Removes every retained record.
    pub fn clear(&self) {
        match self.inner.lock() {
            Ok(mut guard) => guard.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    }
}

impl Default for MemoryLogSink {
    fn default() -> Self {
        Self::new()
    }
}

/// A single log record captured by the in-memory backend.
///
/// The fields mirror the small subset of the OTel `LogRecord` shape
/// that the web backend consumes today; attributes are flattened to
/// string values because that is what the SSE and history endpoints
/// serialize back to the UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLog {
    /// Log record body converted to a string.
    pub body: String,
    /// Attributes attached to the log record.
    pub attributes: Vec<(String, String)>,
}

#[derive(Debug)]
struct MemoryLogExporter {
    inner: Arc<Mutex<Vec<StoredLog>>>,
}

impl MemoryLogExporter {
    fn new(inner: Arc<Mutex<Vec<StoredLog>>>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl LogExporter for MemoryLogExporter {
    async fn export(&mut self, batch: LogBatch<'_>) -> LogResult<()> {
        let mut records = Vec::new();
        for (record, _scope) in batch.iter() {
            records.push(stored_log_from(record));
        }
        match self.inner.lock() {
            Ok(mut guard) => guard.extend(records),
            Err(poisoned) => poisoned.into_inner().extend(records),
        }
        Ok(())
    }
}

fn stored_log_from(record: &LogRecord) -> StoredLog {
    let body = match record.body.as_ref() {
        Some(any) => any_value_to_string(any),
        None => String::new(),
    };
    let mut attributes = Vec::new();
    for (key, value) in record.attributes_iter() {
        attributes.push((key.to_string(), any_value_to_string(value)));
    }
    StoredLog { body, attributes }
}

fn any_value_to_string(value: &opentelemetry::logs::AnyValue) -> String {
    use opentelemetry::logs::AnyValue;
    match value {
        AnyValue::String(s) => s.to_string(),
        AnyValue::Int(i) => i.to_string(),
        AnyValue::Double(d) => d.to_string(),
        AnyValue::Boolean(b) => b.to_string(),
        AnyValue::Bytes(b) => format!("{b:?}"),
        AnyValue::ListAny(l) => format!("{l:?}"),
        AnyValue::Map(m) => format!("{m:?}"),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_config_default_backend_is_memory() {
        let config = TelemetryConfig::default();
        assert!(matches!(config.backend, Backend::Memory));
        assert_eq!(config.service_name, "flowgen");
        assert_eq!(config.metrics_export_interval_secs, 60);
    }

    #[test]
    fn test_telemetry_config_otlp() {
        let config = TelemetryConfig {
            backend: Backend::Otlp {
                endpoint: "http://otel-collector:4317".to_string(),
            },
            service_name: "flowgen-prod".to_string(),
            service_version: "1.0.0".to_string(),
            metrics_export_interval_secs: 30,
        };
        assert!(matches!(
            &config.backend,
            Backend::Otlp { endpoint } if endpoint == "http://otel-collector:4317"
        ));
        assert_eq!(config.service_name, "flowgen-prod");
        assert_eq!(config.metrics_export_interval_secs, 30);
    }
}
