//! Integration tests for the `mongodb_collection` and `mongodb_change_stream`
//! processors against a real MongoDB server in a Docker container.
//!
//! Exercises `read`, `write`, `upsert`, and change-stream watching with the same
//! event-flow shape a YAML task uses in production: an upstream event
//! drives the operation, the processor emits a result event downstream.
//! Change streams require a replica set, so the container is started as a
//! single-node replica set (`rs.initiate()` right after boot).
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use flowgen_core::event::{Event, EventBuilder, EventData};
use flowgen_mongodb::change_stream::ChangeStreamReaderBuilder;
use flowgen_mongodb::collection::ProcessorBuilder;
use flowgen_mongodb::config::{ChangeStream as ChangeStreamConfig, Collection, Operation};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::client::ClientError;
use testcontainers::core::error::TestcontainersError;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::mpsc;

/// Serialises container startup across the tests in this file, so only the
/// first one pays for the image pull and the rest start against a warm cache.
static START_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Starts the container, retrying a failed image pull.
///
/// A CI runner fetches the image over the network on every run, and that
/// transfer occasionally dies mid-stream — surfacing as
/// `PullImage("bytes remaining on stream")`. testcontainers treats it as
/// terminal and does not retry, so one flaky download fails the test.
async fn start_mongo_container() -> ContainerAsync<GenericImage> {
    let mut last_error = None;
    for attempt in 1..=3 {
        if attempt > 1 {
            tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
        }
        let result = GenericImage::new("mongo", "7.0")
            .with_exposed_port(27017.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
            .with_cmd(["--replSet", "rs0", "--bind_ip_all"])
            .start()
            .await;
        match result {
            Ok(container) => return container,
            // A container that starts and then fails its `WaitFor` would fail
            // the same way three times over, so only pulls are worth retrying.
            Err(e) if !is_pull_failure(&e) => panic!("start mongo container: {e:?}"),
            Err(e) => last_error = Some(e),
        }
    }
    panic!("start mongo container: {last_error:?}");
}

fn is_pull_failure(error: &TestcontainersError) -> bool {
    matches!(
        error,
        TestcontainersError::Client(ClientError::PullImage { .. })
    )
}

async fn start_mongo() -> (ContainerAsync<GenericImage>, std::path::PathBuf) {
    let guard = START_LOCK.lock().await;
    let container = start_mongo_container().await;
    drop(guard);
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("map mongo port");
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");

    // Change streams require a replica set; a single-node set still needs
    // an explicit rs.initiate() before it will accept writes.
    let client = mongodb::Client::with_uri_str(&uri)
        .await
        .expect("connect for replica set init");
    // `mongod` validates the replica set config against the address it
    // sees itself listening on inside the container (its own port 27017),
    // not the host-mapped port the test client connects through.
    let init = mongodb::bson::doc! {
        "replSetInitiate": {
            "_id": "rs0",
            "members": [{ "_id": 0, "host": "127.0.0.1:27017" }],
        }
    };
    client
        .database("admin")
        .run_command(init)
        .await
        .expect("initiate replica set");

    // Primary election after initiate is not instant.
    wait_for_primary(&client).await;

    let credentials_path = write_credentials(port);
    (container, credentials_path)
}

async fn wait_for_primary(client: &mongodb::Client) {
    for _ in 0..30 {
        let status = client
            .database("admin")
            .run_command(mongodb::bson::doc! { "isMaster": 1 })
            .await;
        if let Ok(doc) = status {
            if doc.get_bool("ismaster").unwrap_or(false) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("replica set did not elect a primary in time");
}

fn write_credentials(port: u16) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "flowgen_mongo_test_creds_{}.json",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(
        &path,
        serde_json::json!({
            "host": "127.0.0.1",
            "port": port,
            "options": { "directConnection": "true" }
        })
        .to_string(),
    )
    .expect("write credentials file");
    path
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

async fn spawn_collection_processor(
    config: Collection,
) -> (mpsc::Sender<Event>, mpsc::Receiver<Event>) {
    let (in_tx, in_rx) = mpsc::channel(4);
    let (out_tx, out_rx) = mpsc::channel(4);

    let processor = ProcessorBuilder::new()
        .config(Arc::new(config))
        .receiver(in_rx)
        .sender(out_tx)
        .task_id(0)
        .task_type("mongodb_collection")
        .task_context(test_task_context())
        .build()
        .expect("build processor");

    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = processor.run().await;
    });

    (in_tx, out_rx)
}

fn json_filter(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    match value {
        serde_json::Value::Object(map) => map,
        other => panic!("filter must be an object, got {other:?}"),
    }
}

fn drive_event(data: serde_json::Value) -> Event {
    EventBuilder::new()
        .subject("trigger".to_string())
        .data(EventData::Json(data))
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event")
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn write_then_read_round_trips_through_real_mongo() {
    let (_mongo, credentials_path) = start_mongo().await;

    let (write_tx, mut write_rx) = spawn_collection_processor(Collection {
        name: "write_customer".to_string(),
        operation: Operation::Write,
        credentials_path: Some(credentials_path.clone()),
        db_name: "sales".to_string(),
        collection_name: "customers".to_string(),
        filter: Default::default(),
        depends_on: None,
        retry: None,
    })
    .await;
    write_tx
        .send(drive_event(
            serde_json::json!({"name": "Ada", "status": "active"}),
        ))
        .await
        .expect("send write event");
    let write_result = tokio::time::timeout(Duration::from_secs(10), write_rx.recv())
        .await
        .expect("write emits result")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert!(
        write_result.get("insertedId").is_some(),
        "insert result must carry insertedId, got {write_result:?}"
    );

    let (read_tx, mut read_rx) = spawn_collection_processor(Collection {
        name: "read_customers".to_string(),
        operation: Operation::Read,
        credentials_path: Some(credentials_path),
        db_name: "sales".to_string(),
        collection_name: "customers".to_string(),
        filter: json_filter(serde_json::json!({ "status": "active" })),
        depends_on: None,
        retry: None,
    })
    .await;
    read_tx
        .send(drive_event(serde_json::json!({})))
        .await
        .expect("send read event");

    let read_result = tokio::time::timeout(Duration::from_secs(10), read_rx.recv())
        .await
        .expect("read emits result")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(
        read_result.get("name").and_then(|v| v.as_str()),
        Some("Ada")
    );
    assert_eq!(
        read_result.get("status").and_then(|v| v.as_str()),
        Some("active")
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn read_filter_applies_query_operators_and_value_types() {
    let (_mongo, credentials_path) = start_mongo().await;

    let (write_tx, mut write_rx) = spawn_collection_processor(Collection {
        name: "write_people".to_string(),
        operation: Operation::Write,
        credentials_path: Some(credentials_path.clone()),
        db_name: "sales".to_string(),
        collection_name: "people".to_string(),
        filter: Default::default(),
        depends_on: None,
        retry: None,
    })
    .await;
    for (name, age) in [("Ada", 36), ("Grace", 45), ("Alan", 24)] {
        write_tx
            .send(drive_event(serde_json::json!({ "name": name, "age": age })))
            .await
            .expect("send write event");
        tokio::time::timeout(Duration::from_secs(10), write_rx.recv())
            .await
            .expect("write emits result")
            .expect("channel open");
    }

    let (read_tx, mut read_rx) = spawn_collection_processor(Collection {
        name: "read_people".to_string(),
        operation: Operation::Read,
        credentials_path: Some(credentials_path),
        db_name: "sales".to_string(),
        collection_name: "people".to_string(),
        filter: json_filter(serde_json::json!({ "age": { "$gt": 30 } })),
        depends_on: None,
        retry: None,
    })
    .await;
    read_tx
        .send(drive_event(serde_json::json!({})))
        .await
        .expect("send read event");

    let mut names = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(10), read_rx.recv())
            .await
            .expect("read emits result")
            .expect("channel open");
        let name = event
            .data_as_json()
            .expect("json")
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        names.push(name);
    }
    names.sort();
    assert_eq!(
        names,
        vec![Some("Ada".to_string()), Some("Grace".to_string())],
        "`$gt` must compare numerically and exclude the 24-year-old"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn read_with_no_matches_emits_no_events() {
    let (_mongo, credentials_path) = start_mongo().await;

    let (read_tx, mut read_rx) = spawn_collection_processor(Collection {
        name: "read_empty".to_string(),
        operation: Operation::Read,
        credentials_path: Some(credentials_path),
        db_name: "sales".to_string(),
        collection_name: "customers".to_string(),
        filter: json_filter(serde_json::json!({ "status": "does_not_exist" })),
        depends_on: None,
        retry: None,
    })
    .await;
    read_tx
        .send(drive_event(serde_json::json!({})))
        .await
        .expect("send read event");

    let result = tokio::time::timeout(Duration::from_secs(3), read_rx.recv()).await;
    assert!(
        result.is_err(),
        "no matching documents must emit no events, got {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn read_emits_every_matching_document() {
    let (_mongo, credentials_path) = start_mongo().await;

    let (write_tx, mut write_rx) = spawn_collection_processor(Collection {
        name: "write_customer".to_string(),
        operation: Operation::Write,
        credentials_path: Some(credentials_path.clone()),
        db_name: "sales".to_string(),
        collection_name: "batch".to_string(),
        filter: Default::default(),
        depends_on: None,
        retry: None,
    })
    .await;
    for name in ["Ada", "Grace", "Katherine"] {
        write_tx
            .send(drive_event(serde_json::json!({"name": name, "batch": "x"})))
            .await
            .expect("send write event");
        let _ = tokio::time::timeout(Duration::from_secs(10), write_rx.recv())
            .await
            .expect("write completes");
    }

    let (read_tx, mut read_rx) = spawn_collection_processor(Collection {
        name: "read_batch".to_string(),
        operation: Operation::Read,
        credentials_path: Some(credentials_path),
        db_name: "sales".to_string(),
        collection_name: "batch".to_string(),
        filter: json_filter(serde_json::json!({ "batch": "x" })),
        depends_on: None,
        retry: None,
    })
    .await;
    read_tx
        .send(drive_event(serde_json::json!({})))
        .await
        .expect("send read event");

    let mut names = Vec::new();
    for _ in 0..3 {
        let event = tokio::time::timeout(Duration::from_secs(10), read_rx.recv())
            .await
            .expect("read emits result")
            .expect("channel open");
        let name = event
            .data_as_json()
            .expect("json")
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        names.push(name);
    }
    names.sort();
    assert_eq!(
        names,
        vec![
            Some("Ada".to_string()),
            Some("Grace".to_string()),
            Some("Katherine".to_string())
        ]
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn change_stream_emits_event_on_insert() {
    let (_mongo, credentials_path) = start_mongo().await;

    let (change_tx, mut change_rx) = mpsc::channel::<Event>(4);
    let change_reader = ChangeStreamReaderBuilder::new()
        .config(Arc::new(ChangeStreamConfig {
            name: "watch_sales".to_string(),
            credentials_path: Some(credentials_path.clone()),
            db_name: "sales".to_string(),
            depends_on: None,
            retry: None,
        }))
        .sender(change_tx)
        .task_id(0)
        .task_type("mongodb_change_stream")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build change stream reader");

    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = change_reader.run().await;
    });

    // The change stream needs a moment to establish its cursor before the
    // write below; there is no synchronous "ready" signal to await on.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (write_tx, mut write_rx) = spawn_collection_processor(Collection {
        name: "write_customer".to_string(),
        operation: Operation::Write,
        credentials_path: Some(credentials_path),
        db_name: "sales".to_string(),
        collection_name: "customers".to_string(),
        filter: Default::default(),
        depends_on: None,
        retry: None,
    })
    .await;
    write_tx
        .send(drive_event(serde_json::json!({"name": "Grace"})))
        .await
        .expect("send write event");
    let _ = tokio::time::timeout(Duration::from_secs(10), write_rx.recv())
        .await
        .expect("write completes");

    let change_event = tokio::time::timeout(Duration::from_secs(10), change_rx.recv())
        .await
        .expect("change stream emits event")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(
        change_event.get("name").and_then(|v| v.as_str()),
        Some("Grace")
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn change_stream_emits_updates_and_deletes() {
    let (mongo, credentials_path) = start_mongo().await;
    let port = mongo
        .get_host_port_ipv4(27017)
        .await
        .expect("map mongo port");

    let (change_tx, mut change_rx) = mpsc::channel::<Event>(8);
    let change_reader = ChangeStreamReaderBuilder::new()
        .config(Arc::new(ChangeStreamConfig {
            name: "watch".to_string(),
            credentials_path: Some(credentials_path),
            db_name: "d".to_string(),
            depends_on: None,
            retry: None,
        }))
        .sender(change_tx)
        .task_id(0)
        .task_type("mongodb_change_stream")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build change stream reader");
    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = change_reader.run().await;
    });
    tokio::time::sleep(Duration::from_secs(2)).await;

    let collection =
        mongodb::Client::with_uri_str(format!("mongodb://127.0.0.1:{port}/?directConnection=true"))
            .await
            .expect("connect")
            .database("d")
            .collection::<mongodb::bson::Document>("c");
    let mut received = Vec::new();
    let mut ids = std::collections::HashSet::new();
    for step in 0..4 {
        match step {
            0 => collection
                .insert_one(mongodb::bson::doc! { "_id": 1, "n": "a" })
                .await
                .map(|_| ()),
            1 => collection
                .update_one(
                    mongodb::bson::doc! { "_id": 1 },
                    mongodb::bson::doc! { "$set": { "n": "b" } },
                )
                .await
                .map(|_| ()),
            2 => collection
                .delete_one(mongodb::bson::doc! { "_id": 1 })
                .await
                .map(|_| ()),
            _ => collection
                .insert_one(mongodb::bson::doc! { "_id": 2, "n": "c" })
                .await
                .map(|_| ()),
        }
        .expect("write");
        let event = tokio::time::timeout(Duration::from_secs(10), change_rx.recv())
            .await
            .expect("change stream emits event")
            .expect("channel open");
        let meta = event.meta.clone().expect("meta");
        ids.insert(event.id.clone().expect("event id"));
        received.push((
            event.subject.clone(),
            meta["operation_type"].clone(),
            event.data_as_json().expect("json"),
        ));
    }

    assert_eq!(ids.len(), 4, "every change needs its own event id");
    assert_eq!(
        received,
        vec![
            (
                "c".to_string(),
                "insert".into(),
                serde_json::json!({"_id": 1, "n": "a"})
            ),
            (
                "c".to_string(),
                "update".into(),
                serde_json::json!({"_id": 1, "n": "b"})
            ),
            (
                "c".to_string(),
                "delete".into(),
                serde_json::json!({"_id": 1})
            ),
            (
                "c".to_string(),
                "insert".into(),
                serde_json::json!({"_id": 2, "n": "c"})
            ),
        ]
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn upsert_updates_matching_or_inserts_new_document() {
    let (_mongo, credentials_path) = start_mongo().await;

    let (write_tx, mut write_rx) = spawn_collection_processor(Collection {
        name: "write_customer".to_string(),
        operation: Operation::Write,
        credentials_path: Some(credentials_path.clone()),
        db_name: "sales".to_string(),
        collection_name: "customers".to_string(),
        filter: Default::default(),
        depends_on: None,
        retry: None,
    })
    .await;
    write_tx
        .send(drive_event(serde_json::json!({
            "name": "Ada",
            "email": "ada@example.com"
        })))
        .await
        .expect("send write event");
    let written = tokio::time::timeout(Duration::from_secs(10), write_rx.recv())
        .await
        .expect("write emits result")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    let original_id = written.get("insertedId").cloned();

    let (upsert_tx, mut upsert_rx) = spawn_collection_processor(Collection {
        name: "upsert_customers".to_string(),
        operation: Operation::Upsert,
        credentials_path: Some(credentials_path.clone()),
        db_name: "sales".to_string(),
        collection_name: "customers".to_string(),
        filter: json_filter(serde_json::json!({ "email": "ada@example.com" })),
        depends_on: None,
        retry: None,
    })
    .await;

    upsert_tx
        .send(drive_event(serde_json::json!({
            "_id": { "$oid": "696a1d842f9c12344cd86eab" },
            "id": "2-updated",
            "name": "Ada Lovelace",
            "status": "active"
        })))
        .await
        .expect("send upsert event");
    let updated = tokio::time::timeout(Duration::from_secs(10), upsert_rx.recv())
        .await
        .expect("upsert emits result")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(
        updated.get("name").and_then(|v| v.as_str()),
        Some("Ada Lovelace"),
        "matched upsert must emit the updated document, got {updated:?}"
    );
    assert_eq!(
        updated.get("status").and_then(|v| v.as_str()),
        Some("active")
    );
    assert_eq!(
        updated.get("_id"),
        original_id.as_ref(),
        "matched upsert must not modify _id; updated={updated:?} original={original_id:?}"
    );

    let (upsert_insert_tx, mut upsert_insert_rx) = spawn_collection_processor(Collection {
        name: "upsert_new_customers".to_string(),
        operation: Operation::Upsert,
        credentials_path: Some(credentials_path.clone()),
        db_name: "sales".to_string(),
        collection_name: "customers".to_string(),
        filter: json_filter(serde_json::json!({ "email": "grace@example.com" })),
        depends_on: None,
        retry: None,
    })
    .await;
    upsert_insert_tx
        .send(drive_event(serde_json::json!({
            "_id": { "$oid": "66803022a16e6a0000edb3f9" },
            "id": "2-updated",
            "name": "Grace",
            "email": "grace@example.com",
            "status": "inactive"
        })))
        .await
        .expect("send upsert event");
    let inserted = tokio::time::timeout(Duration::from_secs(10), upsert_insert_rx.recv())
        .await
        .expect("upsert emits result")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(
        inserted.get("email").and_then(|v| v.as_str()),
        Some("grace@example.com"),
        "non-matching upsert must insert and emit the new document, got {inserted:?}"
    );
    assert_eq!(
        inserted.get("status").and_then(|v| v.as_str()),
        Some("inactive")
    );
    assert_eq!(
        inserted.get("_id"),
        Some(&serde_json::json!({ "$oid": "66803022a16e6a0000edb3f9" })),
        "inserted document must get the payload _id"
    );

    upsert_insert_tx
        .send(drive_event(serde_json::json!({
            "$inc": { "visits": 1 }
        })))
        .await
        .expect("send upsert event");
    let visit_doc = tokio::time::timeout(Duration::from_secs(10), upsert_insert_rx.recv())
        .await
        .expect("upsert emits result")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(
        visit_doc.get("visits").and_then(|v| v.as_i64()),
        Some(1),
        "update-operator payload must be applied verbatim, got {visit_doc:?}"
    );

    let (read_tx, mut read_rx) = spawn_collection_processor(Collection {
        name: "read_customers".to_string(),
        operation: Operation::Read,
        credentials_path: Some(credentials_path),
        db_name: "sales".to_string(),
        collection_name: "customers".to_string(),
        filter: Default::default(),
        depends_on: None,
        retry: None,
    })
    .await;
    read_tx
        .send(drive_event(serde_json::json!({})))
        .await
        .expect("send read event");

    let mut names = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(10), read_rx.recv())
            .await
            .expect("read emits result")
            .expect("channel open");
        let name = event
            .data_as_json()
            .expect("json")
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        names.push(name);
    }
    names.sort();
    assert_eq!(
        names,
        vec![Some("Ada Lovelace".to_string()), Some("Grace".to_string())],
        "the matched upsert must update in place, not insert a duplicate"
    );
}
