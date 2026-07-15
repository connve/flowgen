//! End-to-end integration test for the OTLP push pipeline.
//!
//! We exercise the same wiring `flowgen::main` uses:
//!
//! 1. Build a `TelemetryGuard` with the in-memory backend (no network).
//! 2. Install `OpenTelemetryTracingBridge` on a fresh subscriber so every
//!    `tracing::event!` becomes an OTel `LogRecord`.
//! 3. Emit `tracing::info!` and `tracing::error!` events with structured
//!    attributes that mirror what `flow::activity::FlowRegistry` will emit.
//! 4. Flush the provider and assert the sink captured both bodies and the
//!    attributes flowgen relies on downstream (flow, task, event_id).

use flowgen_core::telemetry::{init_telemetry, Backend, TelemetryConfig};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use tracing::subscriber::with_default;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

#[tokio::test(flavor = "multi_thread")]
async fn tracing_events_flow_through_otlp_bridge_into_memory_sink() {
    let guard = init_telemetry(TelemetryConfig {
        backend: Backend::Memory,
        service_name: "flowgen-test".to_string(),
        service_version: "0.0.0".to_string(),
        metrics_export_interval_secs: 60,
    })
    .expect("init_telemetry must succeed for the memory backend");

    let sink = guard
        .memory_logs()
        .expect("memory backend always exposes a sink");

    let bridge = OpenTelemetryTracingBridge::new(guard.logger_provider());
    let subscriber = Registry::default().with(bridge);

    with_default(subscriber, || {
        tracing::info!(
            target: "flowgen.activity",
            flow = "orders",
            task = "handle",
            event_id = "evt-1",
            "task.handle completed"
        );
        tracing::error!(
            target: "flowgen.activity",
            flow = "orders",
            task = "handle",
            event_id = "evt-2",
            "task.handle failed"
        );
    });

    for _ in guard.logger_provider().force_flush() {}

    let mut records = sink.snapshot();
    records.sort_by(|a, b| a.body.cmp(&b.body));

    assert_eq!(
        records.len(),
        2,
        "expected two log records, got {records:?}"
    );

    let failed = records
        .iter()
        .find(|r| r.body == "task.handle failed")
        .expect("failed record must be present");
    let completed = records
        .iter()
        .find(|r| r.body == "task.handle completed")
        .expect("completed record must be present");

    for record in [failed, completed] {
        let attrs: std::collections::HashMap<_, _> = record.attributes.iter().cloned().collect();
        assert_eq!(attrs.get("flow").map(String::as_str), Some("orders"));
        assert_eq!(attrs.get("task").map(String::as_str), Some("handle"));
        assert!(
            attrs.contains_key("event_id"),
            "record missing event_id attribute: {record:?}"
        );
    }
    assert_eq!(
        failed
            .attributes
            .iter()
            .find(|(k, _)| k == "event_id")
            .map(|(_, v)| v.as_str()),
        Some("evt-2")
    );
    assert_eq!(
        completed
            .attributes
            .iter()
            .find(|(k, _)| k == "event_id")
            .map(|(_, v)| v.as_str()),
        Some("evt-1")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_sink_clear_drops_retained_records() {
    let guard = init_telemetry(TelemetryConfig {
        backend: Backend::Memory,
        service_name: "flowgen-test".to_string(),
        service_version: "0.0.0".to_string(),
        metrics_export_interval_secs: 60,
    })
    .expect("init_telemetry must succeed for the memory backend");

    let sink = guard
        .memory_logs()
        .expect("memory backend always exposes a sink");
    let bridge = OpenTelemetryTracingBridge::new(guard.logger_provider());
    let subscriber = Registry::default().with(bridge);

    with_default(subscriber, || {
        tracing::info!(target: "flowgen.activity", "first");
    });
    for _ in guard.logger_provider().force_flush() {}
    assert_eq!(sink.snapshot().len(), 1);

    sink.clear();
    assert!(sink.snapshot().is_empty());

    with_default(subscriber_for_next(&guard), || {
        tracing::info!(target: "flowgen.activity", "second");
    });
    for _ in guard.logger_provider().force_flush() {}
    let after = sink.snapshot();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].body, "second");
}

fn subscriber_for_next(
    guard: &flowgen_core::telemetry::TelemetryGuard,
) -> impl tracing::Subscriber + Send + Sync {
    Registry::default().with(OpenTelemetryTracingBridge::new(guard.logger_provider()))
}
