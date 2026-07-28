//! End-to-end integration test for the memory logs pipeline.
//!
//! Runs the same wiring `flowgen::main` uses for the memory backend:
//! `tracing_subscriber::fmt::json()` with a `MemoryLogsStoreWriter` sink,
//! then asserts that emitted events land in `MemoryLogsStore` with
//! flow/task/level attributes taken from the parent span hierarchy.

use flowgen_core::telemetry::query::LogFilter;
use flowgen_core::telemetry::{init_telemetry, Backend, TelemetryConfig};
use futures_util::StreamExt;
use tracing::subscriber::with_default;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

fn memory_config() -> TelemetryConfig {
    TelemetryConfig {
        backend: Backend::Memory {
            logs_per_flow: 1024,
            metrics_per_flow: 1024,
        },
        service_name: "flowgen-test".to_string(),
        service_version: "0.0.0".to_string(),
        metrics_export_interval_secs: 60,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tracing_events_flow_through_json_writer_into_query_backend() {
    let telemetry = init_telemetry(memory_config()).expect("init_telemetry must succeed");
    let query = telemetry
        .logs_store
        .as_ref()
        .cloned()
        .expect("memory backend always exposes a logs query");
    let writer = telemetry
        .logs_writer
        .clone()
        .expect("memory backend always exposes a logs writer");

    let layer = tracing_subscriber::fmt::layer().json().with_writer(writer);
    let subscriber = Registry::default().with(layer);

    with_default(subscriber, || {
        let flow_span = tracing::info_span!("flow.run", flow = "orders");
        let _flow_guard = flow_span.enter();
        let task_span = tracing::info_span!("task.run", task = "handle", task_type = "script");
        let _task_guard = task_span.enter();
        tracing::info!(event_id = "evt-1", "task.handle completed");
        tracing::error!(event_id = "evt-2", "task.handle failed");
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let records = query
        .query(
            LogFilter {
                flow: Some("orders".to_string()),
                ..Default::default()
            },
            100,
        )
        .await
        .expect("query must succeed");

    assert_eq!(
        records.len(),
        2,
        "expected two log records, got {records:?}"
    );
    let bodies: Vec<&str> = records.iter().map(|r| r.body.as_str()).collect();
    assert!(bodies.contains(&"task.handle completed"));
    assert!(bodies.contains(&"task.handle failed"));

    for record in &records {
        let span_field = |key: &str| -> Option<String> {
            record
                .spans
                .iter()
                .rev()
                .flat_map(|s| s.fields.iter())
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(span_field("flow").as_deref(), Some("orders"));
        assert_eq!(span_field("task").as_deref(), Some("handle"));
        assert!(
            record.fields.iter().any(|(k, _)| k == "event_id"),
            "event_id missing from {record:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn filter_narrows_query_results_by_flow() {
    let telemetry = init_telemetry(memory_config()).expect("init_telemetry must succeed");
    let query = telemetry.logs_store.as_ref().cloned().unwrap();
    let writer = telemetry.logs_writer.clone().unwrap();
    let layer = tracing_subscriber::fmt::layer().json().with_writer(writer);
    let subscriber = Registry::default().with(layer);

    with_default(subscriber, || {
        let orders_span = tracing::info_span!("flow.run", flow = "orders");
        orders_span.in_scope(|| tracing::info!("a"));
        orders_span.in_scope(|| tracing::info!("b"));
        let payments_span = tracing::info_span!("flow.run", flow = "payments");
        payments_span.in_scope(|| tracing::info!("c"));
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let orders_only = query
        .query(
            LogFilter {
                flow: Some("orders".to_string()),
                ..Default::default()
            },
            100,
        )
        .await
        .unwrap();
    assert_eq!(orders_only.len(), 2);

    let payments_only = query
        .query(
            LogFilter {
                flow: Some("payments".to_string()),
                ..Default::default()
            },
            100,
        )
        .await
        .unwrap();
    assert_eq!(payments_only.len(), 1);
    assert_eq!(payments_only[0].body, "c");
}

#[tokio::test(flavor = "multi_thread")]
async fn tail_delivers_only_records_matching_the_filter() {
    let telemetry = init_telemetry(memory_config()).expect("init_telemetry must succeed");
    let query = telemetry.logs_store.as_ref().cloned().unwrap();
    let writer = telemetry.logs_writer.clone().unwrap();
    let layer = tracing_subscriber::fmt::layer().json().with_writer(writer);
    let subscriber = Registry::default().with(layer);

    let mut tail = query
        .tail(LogFilter {
            flow: Some("orders".to_string()),
            ..Default::default()
        })
        .await
        .expect("tail must succeed");

    with_default(subscriber, || {
        let orders = tracing::info_span!("flow.run", flow = "orders");
        orders.in_scope(|| tracing::info!("matches"));
        let payments = tracing::info_span!("flow.run", flow = "payments");
        payments.in_scope(|| tracing::info!("skipped"));
        orders.in_scope(|| tracing::info!("matches too"));
    });

    let first = tokio::time::timeout(std::time::Duration::from_secs(2), tail.next())
        .await
        .expect("first record must arrive")
        .expect("stream must not close");
    assert_eq!(first.body, "matches");

    let second = tokio::time::timeout(std::time::Duration::from_secs(2), tail.next())
        .await
        .expect("second record must arrive")
        .expect("stream must not close");
    assert_eq!(second.body, "matches too");
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_backend_exposes_no_logs_store_or_writer() {
    let telemetry = init_telemetry(TelemetryConfig {
        backend: Backend::Remote {
            endpoint: "http://127.0.0.1:14317".to_string(),
        },
        service_name: "flowgen-test".to_string(),
        service_version: "0.0.0".to_string(),
        metrics_export_interval_secs: 60,
    })
    .expect("init_telemetry must succeed for the remote backend");

    assert!(telemetry.logs_store.is_none());
    assert!(telemetry.logs_writer.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn non_string_fields_are_stringified_via_json_writer() {
    let telemetry = init_telemetry(memory_config()).expect("init_telemetry must succeed");
    let query = telemetry.logs_store.as_ref().cloned().unwrap();
    let writer = telemetry.logs_writer.clone().unwrap();
    let layer = tracing_subscriber::fmt::layer().json().with_writer(writer);
    let subscriber = Registry::default().with(layer);

    with_default(subscriber, || {
        let flow = tracing::info_span!(
            "task.run",
            flow = "orders",
            task_id = 7_i64,
            task_type = "script"
        );
        flow.in_scope(|| {
            tracing::info!(duration_ms = 123_u64, cache_hit = true, "done");
        });
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let records = query
        .query(
            LogFilter {
                flow: Some("orders".to_string()),
                ..Default::default()
            },
            10,
        )
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    let span_field = |key: &str| -> Option<String> {
        record
            .spans
            .iter()
            .rev()
            .flat_map(|s| s.fields.iter())
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    };
    let event_field = |key: &str| -> Option<String> {
        record
            .fields
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(span_field("task_id").as_deref(), Some("7"));
    assert_eq!(span_field("task_type").as_deref(), Some("script"));
    assert_eq!(event_field("duration_ms").as_deref(), Some("123"));
    assert_eq!(event_field("cache_hit").as_deref(), Some("true"));
}

#[tokio::test(flavor = "multi_thread")]
async fn per_flow_ring_buffer_evicts_oldest_when_full() {
    let telemetry = init_telemetry(TelemetryConfig {
        backend: Backend::Memory {
            logs_per_flow: 3,
            metrics_per_flow: 3,
        },
        service_name: "flowgen-test".to_string(),
        service_version: "0.0.0".to_string(),
        metrics_export_interval_secs: 60,
    })
    .expect("init_telemetry must succeed");
    let query = telemetry.logs_store.as_ref().cloned().unwrap();
    let writer = telemetry.logs_writer.clone().unwrap();
    let layer = tracing_subscriber::fmt::layer().json().with_writer(writer);
    let subscriber = Registry::default().with(layer);

    with_default(subscriber, || {
        let orders = tracing::info_span!("flow.run", flow = "orders");
        orders.in_scope(|| {
            for i in 0..5 {
                tracing::info!(idx = i as u64, "n");
            }
        });
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let records = query
        .query(
            LogFilter {
                flow: Some("orders".to_string()),
                ..Default::default()
            },
            100,
        )
        .await
        .unwrap();
    assert_eq!(records.len(), 3);
    let idx: Vec<&str> = records
        .iter()
        .filter_map(|r| {
            r.fields
                .iter()
                .find(|(k, _)| k == "idx")
                .map(|(_, v)| v.as_str())
        })
        .collect();
    assert_eq!(idx, vec!["2", "3", "4"]);
}
