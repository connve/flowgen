//! OpenTelemetry integration for metrics and distributed tracing.
//!
//! Provides OTLP exporter configuration and integration with the existing tracing infrastructure.
//! Metrics are automatically collected from tracing spans and exported to an OTLP endpoint.

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use std::time::Duration;

/// Errors that can occur during telemetry initialization.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to initialize OpenTelemetry metrics: {source}")]
    MetricsInit {
        #[source]
        source: opentelemetry_sdk::metrics::MetricError,
    },
    #[error("Failed to initialize OpenTelemetry tracer: {source}")]
    TracerInit {
        #[source]
        source: opentelemetry::trace::TraceError,
    },
}

/// OpenTelemetry configuration for metrics and tracing export.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// OTLP endpoint for exporting metrics and traces (e.g., "http://localhost:4317").
    pub otlp_endpoint: String,
    /// Service name for resource identification.
    pub service_name: String,
    /// Service version for resource identification.
    pub service_version: String,
    /// Metrics export interval in seconds (defaults to 60s).
    pub metrics_export_interval_secs: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: "http://localhost:4317".to_string(),
            service_name: "flowgen-worker".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            metrics_export_interval_secs: 60,
        }
    }
}

/// Initializes OpenTelemetry with OTLP exporter for metrics and tracing.
///
/// This sets up:
/// - A MeterProvider for metrics collection
/// - A TracerProvider for distributed tracing
/// - Integration with the tracing-subscriber layer
///
/// Metrics are automatically collected from tracing spans with the `otel.` prefix.
pub fn init_telemetry(config: TelemetryConfig) -> Result<TelemetryGuard, Error> {
    // Create resource with service identification.
    let resource = Resource::new(vec![
        KeyValue::new("service.name", config.service_name.clone()),
        KeyValue::new("service.version", config.service_version.clone()),
    ]);

    // Initialize metrics exporter with OTLP.
    let metrics_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .build()
        .map_err(|source| Error::MetricsInit { source })?;

    // Create meter provider with periodic export.
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

    // Set global meter provider.
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // Initialize tracer provider with OTLP.
    let tracer = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .build()
        .map_err(|source| Error::TracerInit { source })?;

    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(tracer, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();

    // Set global tracer provider.
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    Ok(TelemetryGuard {
        meter_provider,
        tracer_provider,
    })
}

/// Guard that ensures proper shutdown of OpenTelemetry providers.
///
/// When dropped, this flushes any pending metrics and traces.
pub struct TelemetryGuard {
    meter_provider: SdkMeterProvider,
    tracer_provider: opentelemetry_sdk::trace::TracerProvider,
}

impl TelemetryGuard {
    /// Explicitly shuts down telemetry, flushing all pending data.
    pub fn shutdown(self) -> Result<(), Error> {
        // Shutdown happens automatically on drop.
        Ok(())
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // Flush metrics before shutdown.
        if let Err(e) = self.meter_provider.shutdown() {
            tracing::warn!(error = %e, "Failed to shutdown meter provider");
        }

        // Flush traces before shutdown.
        if let Err(e) = self.tracer_provider.shutdown() {
            tracing::warn!(error = %e, "Failed to shutdown tracer provider");
        }

        // Shutdown global providers.
        opentelemetry::global::shutdown_tracer_provider();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_config_default() {
        let config = TelemetryConfig::default();
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.service_name, "flowgen-worker");
        assert_eq!(config.metrics_export_interval_secs, 60);
    }

    #[test]
    fn test_telemetry_config_custom() {
        let config = TelemetryConfig {
            otlp_endpoint: "http://otel-collector:4317".to_string(),
            service_name: "flowgen-prod".to_string(),
            service_version: "1.0.0".to_string(),
            metrics_export_interval_secs: 30,
        };
        assert_eq!(config.otlp_endpoint, "http://otel-collector:4317");
        assert_eq!(config.service_name, "flowgen-prod");
        assert_eq!(config.metrics_export_interval_secs, 30);
    }
}
