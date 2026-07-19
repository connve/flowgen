//! Integration tests for the `nats_kv_store` processor against a real
//! NATS server in a Docker container.
//!
//! Exercises the four supported operations — `Put`, `Get`, `List`,
//! `Delete` — with the same event-flow shape a YAML task uses in
//! production: an upstream event drives the operation, the processor
//! emits a result event downstream.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use flowgen_core::event::{EventBuilder, EventData};
use flowgen_nats::jetstream::kv_store::{Config as KvConfig, Operation, ProcessorBuilder};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::mpsc;

async fn start_nats() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("nats", "2.11.8-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .await
        .expect("start nats container");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("map nats port");
    (container, format!("nats://127.0.0.1:{port}"))
}

fn test_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("build TaskManager"),
    );
    let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
        as Arc<dyn flowgen_core::cache::Cache>;
    Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("test_flow".to_string())
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .expect("build TaskContext"),
    )
}

async fn spawn_processor(
    config: KvConfig,
) -> (
    mpsc::Sender<flowgen_core::event::Event>,
    mpsc::Receiver<flowgen_core::event::Event>,
) {
    let (in_tx, in_rx) = mpsc::channel(4);
    let (out_tx, out_rx) = mpsc::channel(4);

    let processor = ProcessorBuilder::new()
        .config(Arc::new(config))
        .receiver(in_rx)
        .sender(out_tx)
        .task_id(0)
        .task_type("nats_kv_store")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build processor");

    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = processor.run().await;
    });

    (in_tx, out_rx)
}

fn drive_event(subject: &str, data: serde_json::Value) -> flowgen_core::event::Event {
    EventBuilder::new()
        .subject(subject.to_string())
        .data(EventData::Json(data))
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event")
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn put_operation_writes_value_and_emits_put_result() {
    let (_nats, url) = start_nats().await;
    let (tx, mut rx) = spawn_processor(KvConfig {
        name: "kv_put".to_string(),
        url,
        bucket: "kv_put_bucket".to_string(),
        operation: Operation::Put,
        key: Some("greeting".to_string()),
        ..Default::default()
    })
    .await;

    tx.send(drive_event(
        "trigger",
        serde_json::json!({"content": "hello world"}),
    ))
    .await
    .expect("send event");

    let result_event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("put emits result")
        .expect("channel open");
    let result = result_event.data_as_json().expect("json");
    assert_eq!(result.get("key").and_then(|k| k.as_str()), Some("greeting"));
    assert!(
        result.get("revision").and_then(|r| r.as_u64()).is_some(),
        "put result must carry a revision"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn get_operation_retrieves_previously_put_value() {
    let (_nats, url) = start_nats().await;
    let (put_tx, mut put_rx) = spawn_processor(KvConfig {
        name: "kv_put".to_string(),
        url: url.clone(),
        bucket: "kv_get_bucket".to_string(),
        operation: Operation::Put,
        key: Some("greeting".to_string()),
        ..Default::default()
    })
    .await;
    put_tx
        .send(drive_event(
            "trigger",
            serde_json::json!({"content": "stored"}),
        ))
        .await
        .expect("send put");
    let _ = tokio::time::timeout(Duration::from_secs(5), put_rx.recv())
        .await
        .expect("put ack");

    let (get_tx, mut get_rx) = spawn_processor(KvConfig {
        name: "kv_get".to_string(),
        url,
        bucket: "kv_get_bucket".to_string(),
        operation: Operation::Get,
        key: Some("greeting".to_string()),
        ..Default::default()
    })
    .await;
    get_tx
        .send(drive_event("trigger", serde_json::json!({})))
        .await
        .expect("send get");

    let result = tokio::time::timeout(Duration::from_secs(5), get_rx.recv())
        .await
        .expect("get returns")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(result.get("key").and_then(|k| k.as_str()), Some("greeting"));
    assert_eq!(result.get("found").and_then(|f| f.as_bool()), Some(true));
    assert_eq!(
        result.get("content").and_then(|c| c.as_str()),
        Some("stored")
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn get_operation_reports_missing_key_as_not_found() {
    let (_nats, url) = start_nats().await;
    let (tx, mut rx) = spawn_processor(KvConfig {
        name: "kv_get_missing".to_string(),
        url,
        bucket: "kv_missing_bucket".to_string(),
        operation: Operation::Get,
        key: Some("no_such_key".to_string()),
        ..Default::default()
    })
    .await;
    tx.send(drive_event("trigger", serde_json::json!({})))
        .await
        .expect("send event");

    let result = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("get returns")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(result.get("found").and_then(|f| f.as_bool()), Some(false));
    assert!(result.get("content").is_none_or(|c| c.is_null()));
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn list_operation_returns_all_keys_under_prefix() {
    let (_nats, url) = start_nats().await;

    // Seed three keys through the put processor so the whole
    // pipeline is exercised through the task, not just the raw
    // cache API.
    let (put_tx, mut put_rx) = spawn_processor(KvConfig {
        name: "kv_seed".to_string(),
        url: url.clone(),
        bucket: "kv_list_bucket".to_string(),
        operation: Operation::Put,
        key: Some("orders.{{ event.data.id }}".to_string()),
        ..Default::default()
    })
    .await;
    for id in ["a", "b", "c"] {
        put_tx
            .send(drive_event(
                "trigger",
                serde_json::json!({"id": id, "content": id}),
            ))
            .await
            .expect("send put");
        let _ = tokio::time::timeout(Duration::from_secs(5), put_rx.recv()).await;
    }

    let (list_tx, mut list_rx) = spawn_processor(KvConfig {
        name: "kv_list".to_string(),
        url,
        bucket: "kv_list_bucket".to_string(),
        operation: Operation::List,
        key_prefix: Some("orders.".to_string()),
        ..Default::default()
    })
    .await;
    list_tx
        .send(drive_event("trigger", serde_json::json!({})))
        .await
        .expect("send list");

    let result = tokio::time::timeout(Duration::from_secs(5), list_rx.recv())
        .await
        .expect("list returns")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    let mut keys: Vec<String> = result
        .get("keys")
        .and_then(|k| k.as_array())
        .expect("keys array")
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["orders.a", "orders.b", "orders.c"]);
    assert_eq!(result.get("count").and_then(|c| c.as_u64()), Some(3));
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn delete_operation_removes_key_from_bucket() {
    let (_nats, url) = start_nats().await;

    let (put_tx, mut put_rx) = spawn_processor(KvConfig {
        name: "kv_put".to_string(),
        url: url.clone(),
        bucket: "kv_delete_bucket".to_string(),
        operation: Operation::Put,
        key: Some("doomed".to_string()),
        ..Default::default()
    })
    .await;
    put_tx
        .send(drive_event(
            "trigger",
            serde_json::json!({"content": "bye"}),
        ))
        .await
        .expect("send put");
    let _ = tokio::time::timeout(Duration::from_secs(5), put_rx.recv()).await;

    let (del_tx, mut del_rx) = spawn_processor(KvConfig {
        name: "kv_delete".to_string(),
        url: url.clone(),
        bucket: "kv_delete_bucket".to_string(),
        operation: Operation::Delete,
        key: Some("doomed".to_string()),
        ..Default::default()
    })
    .await;
    del_tx
        .send(drive_event("trigger", serde_json::json!({})))
        .await
        .expect("send delete");
    let delete_result = tokio::time::timeout(Duration::from_secs(5), del_rx.recv())
        .await
        .expect("delete returns")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(
        delete_result.get("key").and_then(|k| k.as_str()),
        Some("doomed")
    );

    // Confirm via a follow-up get that the key is really gone.
    let (get_tx, mut get_rx) = spawn_processor(KvConfig {
        name: "kv_get".to_string(),
        url,
        bucket: "kv_delete_bucket".to_string(),
        operation: Operation::Get,
        key: Some("doomed".to_string()),
        ..Default::default()
    })
    .await;
    get_tx
        .send(drive_event("trigger", serde_json::json!({})))
        .await
        .expect("send get");
    let after = tokio::time::timeout(Duration::from_secs(5), get_rx.recv())
        .await
        .expect("get returns")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(after.get("found").and_then(|f| f.as_bool()), Some(false));
}
