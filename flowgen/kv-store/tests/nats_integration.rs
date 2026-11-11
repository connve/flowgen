//! Integration tests for the `kv_store` processor against a real NATS server
//! in a Docker container.
//!
//! Exercises the four operations with the same event-flow shape a YAML task
//! uses in production: an upstream event drives the operation, the processor
//! emits a result event downstream. Also covers per-key `ttl`, which only a
//! real server enforces.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use flowgen_core::event::{EventBuilder, EventData};
use flowgen_kv_store::store::{KvStore, Operation, ProcessorBuilder};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::mpsc;

/// Bound on one step, including the processor's first connect; generous so
/// containers started in parallel do not time it out.
const STEP_TIMEOUT: Duration = Duration::from_secs(30);

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
    config: KvStore,
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
        .task_type("kv_store")
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

/// Sends `data` and returns the result the processor emits.
async fn call(
    tx: &mpsc::Sender<flowgen_core::event::Event>,
    rx: &mut mpsc::Receiver<flowgen_core::event::Event>,
    data: serde_json::Value,
) -> serde_json::Value {
    tx.send(drive_event("trigger", data))
        .await
        .expect("send event");
    tokio::time::timeout(STEP_TIMEOUT, rx.recv())
        .await
        .expect("the processor answers")
        .expect("channel open")
        .data_as_json()
        .expect("json")
}

fn config(url: &str, bucket: &str, operation: Operation) -> KvStore {
    KvStore {
        name: "kv".to_string(),
        url: url.to_string(),
        bucket: bucket.to_string(),
        operation,
        ..Default::default()
    }
}

async fn get(url: &str, bucket: &str, key: &str) -> serde_json::Value {
    let (tx, mut rx) = spawn_processor(KvStore {
        key: Some(key.to_string()),
        ..config(url, bucket, Operation::Get)
    })
    .await;
    call(&tx, &mut rx, serde_json::json!({})).await
}

async fn list_keys(url: &str, bucket: &str, prefix: &str) -> Vec<String> {
    let (tx, mut rx) = spawn_processor(KvStore {
        key_prefix: Some(prefix.to_string()),
        ..config(url, bucket, Operation::List)
    })
    .await;
    let result = call(&tx, &mut rx, serde_json::json!({})).await;
    let mut keys: Vec<String> = result["keys"]
        .as_array()
        .expect("keys array")
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    keys.sort();
    keys
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_put_value_is_read_back() {
    let (_nats, url) = start_nats().await;
    let (tx, mut rx) = spawn_processor(KvStore {
        key: Some("greeting".to_string()),
        ..config(&url, "kv_get_bucket", Operation::Put)
    })
    .await;

    let put = call(&tx, &mut rx, serde_json::json!({"content": "stored"})).await;

    assert_eq!(put, serde_json::json!({"key": "greeting"}));
    assert_eq!(
        get(&url, "kv_get_bucket", "greeting").await,
        serde_json::json!({"key": "greeting", "content": "stored", "found": true})
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_missing_key_is_not_found() {
    let (_nats, url) = start_nats().await;

    let result = get(&url, "kv_missing_bucket", "no_such_key").await;

    assert_eq!(result["found"], false);
    assert!(result["content"].is_null());
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_short_ttl_expires_on_time_in_a_bucket_the_task_creates() {
    let (_nats, url) = start_nats().await;
    let (tx, mut rx) = spawn_processor(KvStore {
        key: Some("short".to_string()),
        ttl: Some(Duration::from_secs(2)),
        ..config(&url, "kv_own_ttl_bucket", Operation::Put)
    })
    .await;
    call(&tx, &mut rx, serde_json::json!({"content": "gone soon"})).await;

    let mut found = true;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while found && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        found = get(&url, "kv_own_ttl_bucket", "short").await["found"]
            .as_bool()
            .expect("found flag");
    }

    assert!(!found, "A 2s TTL must expire within 6s");
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn list_returns_the_keys_under_the_prefix() {
    let (_nats, url) = start_nats().await;
    let (tx, mut rx) = spawn_processor(KvStore {
        key: Some("orders.{{ event.data.id }}".to_string()),
        ..config(&url, "kv_list_bucket", Operation::Put)
    })
    .await;
    for id in ["a", "b", "c"] {
        call(&tx, &mut rx, serde_json::json!({"id": id, "content": id})).await;
    }
    call(&tx, &mut rx, serde_json::json!({"id": "x", "content": "x"})).await;

    let keys = list_keys(&url, "kv_list_bucket", "orders.").await;

    assert_eq!(keys, vec!["orders.a", "orders.b", "orders.c", "orders.x"]);
    assert!(list_keys(&url, "kv_list_bucket", "other.").await.is_empty());
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_deleted_key_is_gone() {
    let (_nats, url) = start_nats().await;
    let (put_tx, mut put_rx) = spawn_processor(KvStore {
        key: Some("doomed".to_string()),
        ..config(&url, "kv_delete_bucket", Operation::Put)
    })
    .await;
    call(&put_tx, &mut put_rx, serde_json::json!({"content": "bye"})).await;
    let (del_tx, mut del_rx) = spawn_processor(KvStore {
        key: Some("doomed".to_string()),
        ..config(&url, "kv_delete_bucket", Operation::Delete)
    })
    .await;

    let deleted = call(&del_tx, &mut del_rx, serde_json::json!({})).await;

    assert_eq!(deleted, serde_json::json!({"key": "doomed"}));
    assert_eq!(
        get(&url, "kv_delete_bucket", "doomed").await["found"],
        false
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn put_entries_writes_changes_and_prunes_the_rest_under_the_prefix() {
    let (_nats, url) = start_nats().await;
    let (tx, mut rx) = spawn_processor(KvStore {
        key_prefix: Some("flows.".to_string()),
        prune: true,
        ..config(&url, "kv_prune_bucket", Operation::Put)
    })
    .await;
    let entries = |entries: serde_json::Value| serde_json::json!({ "entries": entries });

    let first = call(
        &tx,
        &mut rx,
        entries(serde_json::json!([
            {"key": "a", "value": "1"},
            {"key": "b", "value": "2"},
            {"key": "demo/c", "value": "3"}
        ])),
    )
    .await;
    assert_eq!(
        first["put"],
        serde_json::json!(["flows.a", "flows.b", "flows.demo/c"])
    );

    let second = call(
        &tx,
        &mut rx,
        entries(serde_json::json!([
            {"key": "a", "value": "1"},
            {"key": "b", "value": "changed"}
        ])),
    )
    .await;
    assert_eq!(second["put"], serde_json::json!(["flows.b"]));
    assert_eq!(second["deleted"], serde_json::json!(["flows.demo/c"]));
    assert_eq!(second["unchanged"], 1);

    tx.send(drive_event("trigger", entries(serde_json::json!([]))))
        .await
        .expect("send empty put");
    let empty = tokio::time::timeout(STEP_TIMEOUT, rx.recv())
        .await
        .expect("an empty prune is not retried")
        .expect("channel open");
    assert!(empty.error.is_some(), "An empty prune must fail");
    assert_eq!(
        list_keys(&url, "kv_prune_bucket", "flows.").await,
        vec!["flows.a", "flows.b"]
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_put_after_the_bucket_is_deleted_recreates_it() {
    let (_nats, url) = start_nats().await;
    let (tx, mut rx) = spawn_processor(KvStore {
        key: Some("greeting".to_string()),
        ..config(&url, "kv_recreate_bucket", Operation::Put)
    })
    .await;
    call(&tx, &mut rx, serde_json::json!({"content": "a"})).await;

    let client = async_nats::connect(&url).await.expect("nats connect");
    async_nats::jetstream::new(client)
        .delete_key_value("kv_recreate_bucket")
        .await
        .expect("delete bucket");

    call(&tx, &mut rx, serde_json::json!({"content": "b"})).await;
    assert_eq!(
        list_keys(&url, "kv_recreate_bucket", "").await,
        vec!["greeting".to_string()]
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_ttl_on_a_bucket_without_per_key_ttl_fails_at_start() {
    use flowgen_core::task::runner::Runner;
    let (_nats, url) = start_nats().await;
    let client = async_nats::connect(&url).await.expect("nats connect");
    async_nats::jetstream::new(client)
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "kv_no_ttl".to_string(),
            ..Default::default()
        })
        .await
        .expect("create bucket without per-key TTL");
    let (_tx, rx) = mpsc::channel(1);
    let processor = ProcessorBuilder::new()
        .config(Arc::new(KvStore {
            key: Some("k".to_string()),
            ttl: Some(Duration::from_secs(60)),
            ..config(&url, "kv_no_ttl", Operation::Put)
        }))
        .receiver(rx)
        .task_id(0)
        .task_type("kv_store")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build processor");

    let started = tokio::time::timeout(STEP_TIMEOUT, processor.init())
        .await
        .expect("init finishes");

    assert!(matches!(
        started,
        Err(flowgen_kv_store::store::Error::TtlNotAllowed { bucket }) if bucket == "kv_no_ttl"
    ));
}
