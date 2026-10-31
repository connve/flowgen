//! Integration tests for the `postgres_query` and `postgres_cdc` tasks against
//! a PostgreSQL server with logical replication in a Docker container.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt32Array,
};
use arrow::record_batch::RecordBatch;
use flowgen_core::event::{Event, EventBuilder, EventData};
use flowgen_postgres::cdc::{self, Cdc};
use flowgen_postgres::query::{ProcessorBuilder, Query};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const PASSWORD: &str = "flowgen";

struct Server {
    _container: ContainerAsync<GenericImage>,
    _credentials: tempfile::NamedTempFile,
    credentials_path: PathBuf,
    client: tokio_postgres::Client,
}

async fn start_postgres() -> Server {
    start_postgres_with(&[]).await
}

/// Starts the server with `args` added to its command line.
async fn start_postgres_with(args: &[&str]) -> Server {
    let command = ["postgres", "-c", "wal_level=logical"]
        .into_iter()
        .chain(args.iter().copied());
    let container = GenericImage::new("postgres", "17-alpine")
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_PASSWORD", PASSWORD)
        .with_cmd(command)
        .start()
        .await
        .expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("map postgres port");
    let client = connect(port).await;

    let credentials = tempfile::NamedTempFile::new().expect("create credentials file");
    std::fs::write(
        credentials.path(),
        format!(
            r#"{{"host": "127.0.0.1", "port": {port}, "database": "postgres", "username": "postgres", "password": "{PASSWORD}", "ssl_mode": "disable"}}"#
        ),
    )
    .expect("write credentials file");
    Server {
        _container: container,
        credentials_path: credentials.path().to_path_buf(),
        _credentials: credentials,
        client,
    }
}

/// Connects once the server accepts connections; the image restarts the
/// server after its first initialisation.
async fn connect(port: u16) -> tokio_postgres::Client {
    let config = format!("host=127.0.0.1 port={port} user=postgres password={PASSWORD}");
    for _ in 0..60 {
        if let Ok((client, connection)) =
            tokio_postgres::connect(&config, tokio_postgres::NoTls).await
        {
            tokio::spawn(connection);
            if client.simple_query("SELECT 1").await.is_ok() {
                return client;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("postgres did not accept connections in time");
}

fn query(server: &Server, statement: &str, parameters: &[&str], batch_size: usize) -> Query {
    serde_json::from_value(serde_json::json!({
        "name": "read",
        "credentials_path": server.credentials_path,
        "query": statement,
        "parameters": parameters,
        "batch_size": batch_size,
    }))
    .expect("query config")
}

fn task_context() -> Arc<flowgen_core::task::context::TaskContext> {
    stoppable_task_context(CancellationToken::new())
}

fn stoppable_task_context(
    cancellation_token: CancellationToken,
) -> Arc<flowgen_core::task::context::TaskContext> {
    shared_task_context(&new_cache(), cancellation_token)
}

fn new_cache() -> Arc<dyn flowgen_core::cache::Cache> {
    Arc::new(flowgen_core::cache::memory::MemoryCache::new())
}

/// A context whose cache outlives the task, as across a restart.
fn shared_task_context(
    cache: &Arc<dyn flowgen_core::cache::Cache>,
    cancellation_token: CancellationToken,
) -> Arc<flowgen_core::task::context::TaskContext> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("build TaskManager"),
    );
    Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("test_flow".to_string())
            .task_manager(task_manager)
            .cache(Arc::clone(cache))
            .cancellation_token(cancellation_token)
            .build()
            .expect("build TaskContext"),
    )
}

/// Runs the task for one triggering event and collects what it emits.
async fn run(
    config: Query,
    completion_tx: Option<flowgen_core::event::SharedCompletionTx>,
) -> Vec<Event> {
    let (in_tx, in_rx) = mpsc::channel(4);
    let (out_tx, mut out_rx) = mpsc::channel(16);
    let processor = ProcessorBuilder::new()
        .config(Arc::new(config))
        .receiver(in_rx)
        .sender(out_tx)
        .task_id(1)
        .task_type("postgres_query")
        .task_context(task_context())
        .build()
        .expect("build processor");
    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = processor.run().await;
    });

    let mut trigger = EventBuilder::new()
        .subject("trigger".to_string())
        .data(EventData::Json(serde_json::json!({})))
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event");
    trigger.completion_tx = completion_tx;
    in_tx.send(trigger).await.expect("send trigger");
    drop(in_tx);

    let mut events = Vec::new();
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_secs(30), out_rx.recv()).await {
        events.push(event);
    }
    events
}

fn batch(event: &Event) -> &RecordBatch {
    match &event.data {
        EventData::ArrowRecordBatch(batch) => batch,
        other => panic!("expected a record batch, got {other:?}"),
    }
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column {name}"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column {name} type"))
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn every_supported_column_type_reaches_arrow() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            r#"
            CREATE TYPE mood AS ENUM ('calm', 'busy');
            CREATE TABLE sample (
                flag boolean, letter "char", small int2, regular int4, big int8, object oid,
                single float4, double float8, exact numeric, body text, label varchar(20),
                code char(3), doc json, docb jsonb, id uuid, day date, at time,
                local_at timestamp, utc_at timestamptz, blob bytea, state mood
            );
            INSERT INTO sample VALUES (
                true, 'x', 7, 42, 9000000000, 16384, 1.5, 2.25, 12345.678, 'hello', 'label',
                'ab', '{"a": 1}', '{"b": 2}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                '2026-10-27', '12:30:00.5', '2026-10-27 12:30:00', '2026-10-27 12:30:00+02',
                '\x0102', 'busy'
            );
            INSERT INTO sample DEFAULT VALUES;
            "#,
        )
        .await
        .expect("seed sample table");

    let events = run(
        query(
            &server,
            "SELECT * FROM sample ORDER BY regular NULLS LAST",
            &[],
            100,
        ),
        None,
    )
    .await;
    assert_eq!(events.len(), 1);
    let batch = batch(&events[0]);
    assert_eq!(batch.num_rows(), 2);

    assert!(column::<BooleanArray>(batch, "flag").value(0));
    assert_eq!(column::<StringArray>(batch, "letter").value(0), "x");
    assert_eq!(column::<Int16Array>(batch, "small").value(0), 7);
    assert_eq!(column::<Int32Array>(batch, "regular").value(0), 42);
    assert_eq!(column::<Int64Array>(batch, "big").value(0), 9_000_000_000);
    assert_eq!(column::<UInt32Array>(batch, "object").value(0), 16384);
    assert_eq!(column::<Float32Array>(batch, "single").value(0), 1.5);
    assert_eq!(column::<Float64Array>(batch, "double").value(0), 2.25);
    assert_eq!(column::<StringArray>(batch, "exact").value(0), "12345.678");
    assert_eq!(column::<StringArray>(batch, "body").value(0), "hello");
    assert_eq!(column::<StringArray>(batch, "label").value(0), "label");
    assert_eq!(column::<StringArray>(batch, "code").value(0), "ab ");
    assert_eq!(column::<StringArray>(batch, "doc").value(0), r#"{"a": 1}"#);
    assert_eq!(column::<StringArray>(batch, "docb").value(0), r#"{"b": 2}"#);
    assert_eq!(
        column::<StringArray>(batch, "id").value(0),
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    );
    assert_eq!(column::<Date32Array>(batch, "day").value(0), 20_753);
    assert_eq!(
        column::<Time64MicrosecondArray>(batch, "at").value(0),
        45_000_500_000
    );
    assert_eq!(
        column::<TimestampMicrosecondArray>(batch, "local_at").value(0),
        1_793_104_200_000_000
    );
    assert_eq!(
        column::<TimestampMicrosecondArray>(batch, "utc_at").value(0),
        1_793_097_000_000_000
    );
    assert_eq!(column::<BinaryArray>(batch, "blob").value(0), [1, 2]);
    assert_eq!(column::<StringArray>(batch, "state").value(0), "busy");

    for field in batch.schema().fields() {
        assert!(
            batch.column_by_name(field.name()).unwrap().is_null(1),
            "{} is null in the second row",
            field.name()
        );
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn text_parameters_bind_as_the_statement_types() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            "CREATE TABLE orders (id int8, placed date, total numeric);
             INSERT INTO orders VALUES (1, '2026-01-01', 10), (2, '2026-06-01', 20), (3, '2026-06-02', 30);",
        )
        .await
        .expect("seed orders");

    let events = run(
        query(
            &server,
            "SELECT id FROM orders WHERE placed >= $1 AND total < $2 ORDER BY id",
            &["2026-05-01", "30"],
            100,
        ),
        None,
    )
    .await;
    let ids = column::<Int64Array>(batch(&events[0]), "id");
    assert_eq!(ids.values().to_vec(), vec![2]);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn rows_are_split_into_batches_and_the_last_one_carries_completion() {
    let server = start_postgres().await;
    let (completion_tx, _completion_rx) = flowgen_core::event::new_completion_channel(1);

    let events = run(
        query(&server, "SELECT generate_series(1, 5) AS n", &[], 2),
        Some(completion_tx),
    )
    .await;
    let sizes: Vec<usize> = events.iter().map(|event| batch(event).num_rows()).collect();
    assert_eq!(sizes, vec![2, 2, 1]);
    let completions: Vec<bool> = events
        .iter()
        .map(|event| event.completion_tx.is_some())
        .collect();
    assert_eq!(completions, vec![false, false, true]);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn an_empty_result_keeps_the_columns() {
    let server = start_postgres().await;

    let events = run(
        query(
            &server,
            "SELECT 1::int4 AS n, 'a'::text AS s WHERE false",
            &[],
            100,
        ),
        None,
    )
    .await;
    assert_eq!(events.len(), 1);
    let batch = batch(&events[0]);
    assert_eq!(batch.num_rows(), 0);
    let names: Vec<&str> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, vec!["n", "s"]);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_statement_without_rows_emits_an_empty_batch() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute("CREATE TABLE audit (note text)")
        .await
        .expect("create audit");

    let events = run(
        query(&server, "INSERT INTO audit VALUES ($1)", &["written"], 100),
        None,
    )
    .await;
    assert_eq!(events.len(), 1);
    assert_eq!(batch(&events[0]).num_columns(), 0);
    let rows = server
        .client
        .query_one("SELECT note FROM audit", &[])
        .await
        .expect("read audit");
    assert_eq!(rows.get::<_, String>(0), "written");
}

const SLOT: &str = "flowgen_changes";

fn cdc_config(server: &Server, max_deliver: Option<u32>) -> Cdc {
    serde_json::from_value(json!({
        "name": "watch",
        "credentials_path": server.credentials_path,
        "publication": "changes",
        "slot": SLOT,
        "max_deliver": max_deliver,
        "retry": { "max_attempts": null, "initial_backoff": "10ms" },
    }))
    .expect("cdc config")
}

/// Starts the task and waits until it holds the slot, so later writes are captured.
async fn spawn_cdc(server: &Server, config: Cdc, stop: CancellationToken) -> mpsc::Receiver<Event> {
    let rx = start_cdc(config, stoppable_task_context(stop));
    wait_for_slot(server, true).await;
    rx
}

/// Starts the task without waiting for it to hold the slot.
fn start_cdc(
    config: Cdc,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
) -> mpsc::Receiver<Event> {
    let (tx, rx) = mpsc::channel(16);
    run_cdc(Arc::new(config), task_context, tx);
    rx
}

/// Runs the task, sending its changes to `tx`.
fn run_cdc(
    config: Arc<Cdc>,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    tx: mpsc::Sender<Event>,
) -> tokio::task::JoinHandle<()> {
    let processor = cdc::ProcessorBuilder::new()
        .config(config)
        .sender(tx)
        .task_id(0)
        .task_type("postgres_cdc")
        .task_context(task_context)
        .build()
        .expect("build processor");
    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = processor.run().await;
    })
}

async fn wait_for_slot(server: &Server, active: bool) {
    for _ in 0..100 {
        let rows = server
            .client
            .query(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&SLOT],
            )
            .await
            .expect("read slots");
        if rows
            .first()
            .is_some_and(|row| row.get::<_, bool>(0) == active)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("slot never became active = {active}");
}

async fn next_change(rx: &mut mpsc::Receiver<Event>) -> Event {
    tokio::time::timeout(Duration::from_secs(30), rx.recv())
        .await
        .expect("change arrives")
        .expect("channel open")
}

fn complete(event: &Event) {
    event
        .completion_tx
        .as_ref()
        .expect("completion channel")
        .signal_completion(None);
}

fn fail(event: &Event) {
    event
        .completion_tx
        .as_ref()
        .expect("completion channel")
        .signal_completion_with_error("Write failed".to_string());
}

fn data(event: &Event) -> &serde_json::Value {
    match &event.data {
        EventData::Json(data) => data,
        other => panic!("expected JSON, got {other:?}"),
    }
}

fn meta<'a>(event: &'a Event, key: &str) -> Option<&'a serde_json::Value> {
    event.meta.as_ref().and_then(|meta| meta.get(key))
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn row_changes_arrive_with_their_table_and_operation() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            "CREATE TABLE items (id int PRIMARY KEY, name text, price numeric, tags jsonb, active boolean);
             CREATE TABLE notes (id int PRIMARY KEY, body text);
             ALTER TABLE notes REPLICA IDENTITY FULL;
             CREATE PUBLICATION changes FOR ALL TABLES;",
        )
        .await
        .expect("create tables");
    let mut rx = spawn_cdc(&server, cdc_config(&server, None), CancellationToken::new()).await;

    server
        .client
        .batch_execute(
            r#"INSERT INTO items VALUES (1, 'pen', 2.50, '{"a": 1}', true);
               UPDATE items SET name = 'ink' WHERE id = 1;
               DELETE FROM items WHERE id = 1;
               INSERT INTO notes VALUES (1, 'a');
               UPDATE notes SET body = 'b' WHERE id = 1;
               TRUNCATE notes;"#,
        )
        .await
        .expect("write changes");

    let mut changes = Vec::new();
    for _ in 0..6 {
        let change = next_change(&mut rx).await;
        complete(&change);
        changes.push(change);
    }

    let summary: Vec<(&str, &str)> = changes
        .iter()
        .map(|change| {
            (
                change.subject.as_str(),
                meta(change, "operation_type")
                    .and_then(|v| v.as_str())
                    .expect("operation_type"),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("items", "insert"),
            ("items", "update"),
            ("items", "delete"),
            ("notes", "insert"),
            ("notes", "update"),
            ("notes", "truncate"),
        ]
    );
    assert_eq!(
        data(&changes[0]),
        &json!({"id": 1, "name": "pen", "price": "2.50", "tags": {"a": 1}, "active": true})
    );
    assert_eq!(meta(&changes[0], "schema"), Some(&json!("public")));
    assert_eq!(meta(&changes[1], "before"), None);
    assert_eq!(data(&changes[1])["name"], "ink");
    assert_eq!(data(&changes[2]), &json!({"id": 1}));
    assert_eq!(
        meta(&changes[4], "before"),
        Some(&json!({"id": 1, "body": "a"}))
    );
    assert_eq!(data(&changes[5]), &serde_json::Value::Null);

    let mut ids: Vec<_> = changes.iter().map(|change| change.id.clone()).collect();
    ids.dedup();
    assert_eq!(ids.len(), 6);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_restarted_cdc_resumes_after_the_last_completed_change() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            "CREATE TABLE people (id int PRIMARY KEY, name text);
             CREATE PUBLICATION changes FOR ALL TABLES;",
        )
        .await
        .expect("create table");
    let insert = |id: i32, name: &'static str| {
        let client = &server.client;
        async move {
            client
                .execute("INSERT INTO people VALUES ($1, $2)", &[&id, &name])
                .await
                .expect("insert person");
        }
    };

    let stop = CancellationToken::new();
    let mut rx = spawn_cdc(&server, cdc_config(&server, None), stop.clone()).await;
    insert(1, "Ann").await;
    complete(&next_change(&mut rx).await);
    insert(2, "Grace").await;
    let in_flight = next_change(&mut rx).await;
    assert_eq!(data(&in_flight)["name"], "Grace");
    stop.cancel();
    wait_for_slot(&server, false).await;
    insert(3, "Ada").await;

    let mut rx = spawn_cdc(&server, cdc_config(&server, None), CancellationToken::new()).await;
    let mut names = Vec::new();
    for _ in 0..2 {
        let change = next_change(&mut rx).await;
        complete(&change);
        names.push(data(&change)["name"].clone());
    }
    assert_eq!(names, [json!("Grace"), json!("Ada")]);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_failed_change_is_delivered_again_until_max_deliver() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            "CREATE TABLE people (id int PRIMARY KEY, name text);
             CREATE PUBLICATION changes FOR ALL TABLES;",
        )
        .await
        .expect("create table");
    let mut rx = spawn_cdc(
        &server,
        cdc_config(&server, Some(2)),
        CancellationToken::new(),
    )
    .await;
    server
        .client
        .batch_execute(
            "INSERT INTO people VALUES (1, 'Ann'); INSERT INTO people VALUES (2, 'Grace');",
        )
        .await
        .expect("insert people");

    let first = next_change(&mut rx).await;
    fail(&first);
    let again = next_change(&mut rx).await;
    fail(&again);
    let next = next_change(&mut rx).await;
    complete(&next);

    assert_eq!(first.id, again.id);
    assert_eq!(data(&again)["name"], "Ann");
    assert_eq!(data(&next)["name"], "Grace");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_transaction_slower_than_the_server_timeout_is_not_replayed() {
    let server = start_postgres_with(&["-c", "wal_sender_timeout=3s"]).await;
    server
        .client
        .batch_execute(
            "CREATE TABLE people (id int PRIMARY KEY, name text);
             CREATE PUBLICATION changes FOR ALL TABLES;",
        )
        .await
        .expect("create table");
    let mut config = cdc_config(&server, None);
    config.status_interval = Duration::from_secs(1);
    let mut rx = spawn_cdc(&server, config, CancellationToken::new()).await;
    server
        .client
        .execute(
            "INSERT INTO people SELECT n, 'person' FROM generate_series(1, 15) AS n",
            &[],
        )
        .await
        .expect("insert people");
    server
        .client
        .execute("INSERT INTO people VALUES (16, 'after')", &[])
        .await
        .expect("insert after");

    let mut ids = std::collections::HashSet::new();
    loop {
        let change = next_change(&mut rx).await;
        assert!(
            ids.insert(change.id.clone()),
            "change {:?} was delivered again",
            change.id
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        complete(&change);
        if data(&change)["name"] == "after" {
            break;
        }
    }
    assert_eq!(ids.len(), 16);
}

fn copy_config(server: &Server, batch_size: usize) -> Cdc {
    let mut config = cdc_config(server, None);
    config.initial_copy = true;
    config.copy_batch_size = std::num::NonZeroUsize::new(batch_size).expect("batch size");
    config
}

async fn create_people(server: &Server) {
    server
        .client
        .batch_execute(
            "CREATE TABLE people (id int PRIMARY KEY, name text);
             INSERT INTO people VALUES (1, 'Ann'), (2, 'Grace');
             CREATE PUBLICATION changes FOR ALL TABLES;",
        )
        .await
        .expect("create table");
}

/// Receives `count` events of the copy, completing them when `done`, and
/// the rows they hold sorted by name.
async fn receive_copy(
    rx: &mut mpsc::Receiver<Event>,
    count: usize,
    done: bool,
) -> (Vec<Event>, Vec<serde_json::Value>) {
    let mut events = Vec::new();
    let mut rows = Vec::new();
    for _ in 0..count {
        let event = next_change(rx).await;
        assert_eq!(meta(&event, "operation_type"), Some(&json!("copy")));
        if done {
            complete(&event);
        }
        rows.extend(
            data(&event)
                .as_array()
                .expect("copied rows")
                .iter()
                .cloned(),
        );
        events.push(event);
    }
    rows.sort_by_key(|row| row["name"].to_string());
    (events, rows)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn an_initial_copy_emits_existing_rows_in_batches_before_the_changes() {
    let server = start_postgres().await;
    create_people(&server).await;

    let mut rx = start_cdc(copy_config(&server, 1000), task_context());
    let (copied, rows) = receive_copy(&mut rx, 1, false).await;
    server
        .client
        .execute("INSERT INTO people VALUES (3, 'Ada')", &[])
        .await
        .expect("insert during the copy");
    copied.iter().for_each(complete);
    let change = next_change(&mut rx).await;
    complete(&change);

    assert_eq!(
        rows,
        [
            json!({"id": 1, "name": "Ann"}),
            json!({"id": 2, "name": "Grace"})
        ]
    );
    assert_eq!(copied[0].subject, "people");
    assert_eq!(meta(&change, "operation_type"), Some(&json!("insert")));
    assert_eq!(data(&change)["name"], "Ada");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn an_unfinished_initial_copy_starts_again() {
    let server = start_postgres().await;
    create_people(&server).await;
    let cache = new_cache();

    let stop = CancellationToken::new();
    let mut rx = start_cdc(
        copy_config(&server, 1),
        shared_task_context(&cache, stop.clone()),
    );
    let (_first, _) = receive_copy(&mut rx, 1, false).await;
    stop.cancel();
    wait_for_slot(&server, false).await;

    let mut rx = start_cdc(
        copy_config(&server, 1),
        shared_task_context(&cache, CancellationToken::new()),
    );
    let (copied, rows) = receive_copy(&mut rx, 2, true).await;

    assert_ne!(copied[0].id, copied[1].id);
    assert_eq!(
        rows,
        [
            json!({"id": 1, "name": "Ann"}),
            json!({"id": 2, "name": "Grace"})
        ]
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn an_initial_copy_started_again_keeps_the_changes_in_the_slot() {
    let server = start_postgres().await;
    create_people(&server).await;
    let cache = new_cache();

    let stop = CancellationToken::new();
    let mut rx = start_cdc(
        copy_config(&server, 1000),
        shared_task_context(&cache, stop.clone()),
    );
    let (interrupted, _) = receive_copy(&mut rx, 1, false).await;
    stop.cancel();
    wait_for_slot(&server, false).await;
    server
        .client
        .batch_execute("DELETE FROM people WHERE id = 1; INSERT INTO people VALUES (3, 'Ada');")
        .await
        .expect("write while stopped");

    let mut rx = start_cdc(
        copy_config(&server, 1000),
        shared_task_context(&cache, CancellationToken::new()),
    );
    let (copied, rows) = receive_copy(&mut rx, 1, true).await;
    assert_ne!(copied[0].id, interrupted[0].id);
    let mut changes = Vec::new();
    for _ in 0..2 {
        let change = next_change(&mut rx).await;
        complete(&change);
        changes.push((
            meta(&change, "operation_type").cloned(),
            data(&change).clone(),
        ));
    }

    assert_eq!(
        rows,
        [
            json!({"id": 3, "name": "Ada"}),
            json!({"id": 2, "name": "Grace"})
        ]
    );
    assert_eq!(
        changes,
        [
            (Some(json!("delete")), json!({"id": 1})),
            (Some(json!("insert")), json!({"id": 3, "name": "Ada"})),
        ]
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_slot_that_lost_its_wal_is_reported() {
    let server = start_postgres_with(&["-c", "max_slot_wal_keep_size=1MB"]).await;
    server
        .client
        .batch_execute(
            "CREATE TABLE people (id int PRIMARY KEY, name text);
             CREATE PUBLICATION changes FOR ALL TABLES;",
        )
        .await
        .expect("create table");
    server
        .client
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .expect("create slot");
    for id in 0..3 {
        server
            .client
            .execute("INSERT INTO people VALUES ($1, 'person')", &[&id])
            .await
            .expect("insert person");
        server
            .client
            .batch_execute("SELECT pg_switch_wal()")
            .await
            .expect("switch WAL");
    }
    server
        .client
        .batch_execute("CHECKPOINT")
        .await
        .expect("checkpoint");

    let processor = cdc::ProcessorBuilder::new()
        .config(Arc::new(cdc_config(&server, None)))
        .task_id(0)
        .task_type("postgres_cdc")
        .task_context(task_context())
        .build()
        .expect("build processor");
    let result = {
        use flowgen_core::task::runner::Runner;
        processor.init().await
    };
    assert!(matches!(result, Err(cdc::Error::SlotLost { .. })));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_finished_initial_copy_is_not_repeated() {
    let server = start_postgres().await;
    create_people(&server).await;
    let cache = new_cache();

    let stop = CancellationToken::new();
    let mut rx = start_cdc(
        copy_config(&server, 1000),
        shared_task_context(&cache, stop.clone()),
    );
    receive_copy(&mut rx, 1, true).await;
    wait_for_slot(&server, true).await;
    stop.cancel();
    wait_for_slot(&server, false).await;
    server
        .client
        .execute("INSERT INTO people VALUES (3, 'Ada')", &[])
        .await
        .expect("insert while stopped");

    let mut rx = start_cdc(
        copy_config(&server, 1000),
        shared_task_context(&cache, CancellationToken::new()),
    );
    let change = next_change(&mut rx).await;
    complete(&change);
    assert_eq!(meta(&change, "operation_type"), Some(&json!("insert")));
    assert_eq!(data(&change)["name"], "Ada");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn an_initial_copy_reads_only_what_the_publication_sends() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            "CREATE TABLE people (id int PRIMARY KEY, name text, secret text);
             CREATE TABLE notes (id int PRIMARY KEY, body text,
                                 size int GENERATED ALWAYS AS (length(body)) STORED);
             CREATE TABLE archived_notes () INHERITS (notes);
             INSERT INTO people VALUES (1, 'Ann', 'a'), (2, 'Grace', 'b');
             INSERT INTO notes (id, body) VALUES (1, 'a');
             INSERT INTO archived_notes (id, body) VALUES (2, 'b');
             CREATE PUBLICATION changes FOR TABLE people (id, name) WHERE (id > 1), notes;",
        )
        .await
        .expect("create tables");

    let mut rx = start_cdc(copy_config(&server, 1000), task_context());
    let mut copied = Vec::new();
    for _ in 0..3 {
        let event = next_change(&mut rx).await;
        complete(&event);
        copied.push((event.subject.clone(), data(&event).clone()));
    }
    copied.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        copied,
        [
            (
                "archived_notes".to_string(),
                json!([{"id": 2, "body": "b"}])
            ),
            ("notes".to_string(), json!([{"id": 1, "body": "a"}])),
            ("people".to_string(), json!([{"id": 2, "name": "Grace"}])),
        ]
    );
}

/// Writes one event per entry of `data` and collects what the task emits.
async fn write(
    server: &Server,
    table: &str,
    change_type: Option<&str>,
    data: Vec<EventData>,
) -> Vec<Event> {
    let config: flowgen_postgres::write::Write = serde_json::from_value(json!({
        "name": "save",
        "credentials_path": server.credentials_path,
        "table": table,
        "change_type": change_type,
        "max_in_flight": 1,
    }))
    .expect("write config");
    let (in_tx, in_rx) = mpsc::channel(4);
    let (out_tx, mut out_rx) = mpsc::channel(16);
    let processor = flowgen_postgres::write::ProcessorBuilder::new()
        .config(Arc::new(config))
        .receiver(in_rx)
        .sender(out_tx)
        .task_id(1)
        .task_type("postgres_write")
        .task_context(task_context())
        .build()
        .expect("build processor");
    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = processor.run().await;
    });

    let count = data.len();
    for data in data {
        let event = EventBuilder::new()
            .subject("rows".to_string())
            .data(data)
            .task_id(0)
            .task_type("test")
            .build()
            .expect("build event");
        in_tx.send(event).await.expect("send rows");
    }
    let mut events = Vec::new();
    for _ in 0..count {
        let event = tokio::time::timeout(Duration::from_secs(30), out_rx.recv())
            .await
            .expect("write result")
            .expect("channel open");
        events.push(event);
    }
    events
}

fn rows_written(event: &Event) -> &serde_json::Value {
    assert_eq!(event.error, None);
    &data(event)["rows_written"]
}

async fn people(server: &Server) -> Vec<(i32, Option<String>)> {
    server
        .client
        .query("SELECT id, name FROM people ORDER BY id", &[])
        .await
        .expect("read people")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn rows_are_appended_with_their_values_parsed_into_the_column_types() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            "CREATE TABLE items (id int, name text, price numeric, tags jsonb, active boolean,
                                 added timestamptz, note text DEFAULT 'none');",
        )
        .await
        .expect("create table");

    let written = write(
        &server,
        "public.items",
        None,
        vec![EventData::Json(json!([
            {"id": 1, "name": "pen, \"blue\"", "price": "2.50", "tags": {"a": [1]}, "active": true,
             "added": "2026-10-30T10:00:00Z"},
            {"id": 2, "name": "", "price": 3, "tags": null, "active": false, "added": null},
        ]))],
    )
    .await;

    let rows = server
        .client
        .query(
            "SELECT id, name, price::text, tags::text, active, added::text, note FROM items ORDER BY id",
            &[],
        )
        .await
        .expect("read items");
    let row = |index: usize| -> (
        i32,
        String,
        String,
        Option<String>,
        bool,
        Option<String>,
        String,
    ) {
        let row = &rows[index];
        (
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
            row.get(6),
        )
    };
    assert_eq!(rows_written(&written[0]), &json!(2));
    assert_eq!(data(&written[0])["table"], "public.items");
    assert_eq!(
        row(0),
        (
            1,
            "pen, \"blue\"".to_string(),
            "2.50".to_string(),
            Some(r#"{"a": [1]}"#.to_string()),
            true,
            Some("2026-10-30 10:00:00+00".to_string()),
            "none".to_string()
        )
    );
    assert_eq!(
        row(1),
        (
            2,
            String::new(),
            "3".to_string(),
            None,
            false,
            None,
            "none".to_string()
        )
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn an_upsert_keeps_the_last_row_of_each_key_and_a_delete_removes_by_key() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            "CREATE TABLE people (id int PRIMARY KEY, name text);
             INSERT INTO people VALUES (1, 'Ann'), (2, 'Grace');",
        )
        .await
        .expect("create table");

    let upserted = write(
        &server,
        "people",
        Some("upsert"),
        vec![EventData::Json(json!([
            {"id": 2, "name": "Grace H"},
            {"id": 3, "name": "Ada"},
            {"id": 2, "name": "Grace Hopper"},
        ]))],
    )
    .await;
    let after_upsert = people(&server).await;
    let deleted = write(
        &server,
        "people",
        Some("delete"),
        vec![EventData::Json(
            json!([{"id": 1, "name": "ignored"}, {"id": 9}]),
        )],
    )
    .await;

    assert_eq!(rows_written(&upserted[0]), &json!(2));
    assert_eq!(
        after_upsert,
        [
            (1, Some("Ann".to_string())),
            (2, Some("Grace Hopper".to_string())),
            (3, Some("Ada".to_string()))
        ]
    );
    assert_eq!(rows_written(&deleted[0]), &json!(1));
    assert_eq!(
        people(&server).await,
        [
            (2, Some("Grace Hopper".to_string())),
            (3, Some("Ada".to_string()))
        ]
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_record_batch_upsert_writes_its_null_columns_and_bytes() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            "CREATE TABLE people (id int PRIMARY KEY, name text, photo bytea);
             INSERT INTO people VALUES (1, 'Ann', NULL);",
        )
        .await
        .expect("create table");
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>,
        ),
        (
            "name",
            Arc::new(StringArray::from(vec![None::<&str>, None])) as Arc<dyn Array>,
        ),
        (
            "photo",
            Arc::new(BinaryArray::from(vec![Some(b"\x00\xff".as_slice()), None])) as Arc<dyn Array>,
        ),
    ])
    .expect("build batch");

    let written = write(
        &server,
        "people",
        Some("upsert"),
        vec![EventData::ArrowRecordBatch(batch)],
    )
    .await;
    let photo: Option<Vec<u8>> = server
        .client
        .query_one("SELECT photo FROM people WHERE id = 1", &[])
        .await
        .expect("read photo")
        .get(0);

    assert_eq!(rows_written(&written[0]), &json!(2));
    assert_eq!(people(&server).await, [(1, None), (2, None)]);
    assert_eq!(photo, Some(vec![0x00, 0xff]));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn an_upsert_needs_a_primary_key_and_its_values() {
    let server = start_postgres().await;
    server
        .client
        .batch_execute(
            "CREATE TABLE notes (id int, body text);
             CREATE TABLE people (id int PRIMARY KEY, name text);",
        )
        .await
        .expect("create tables");

    let without_key = write(
        &server,
        "notes",
        Some("upsert"),
        vec![EventData::Json(json!({"id": 1, "body": "a"}))],
    )
    .await;
    let without_value = write(
        &server,
        "people",
        Some("delete"),
        vec![EventData::Json(json!({"name": "Ann"}))],
    )
    .await;

    assert_eq!(
        without_key[0].error.as_deref(),
        Some("Table 'notes' has no primary key to match rows by")
    );
    assert_eq!(
        without_value[0].error.as_deref(),
        Some("The rows for 'people' have no value for its primary key column 'id'")
    );
}

/// A row the shared source tests write and read back.
#[derive(serde::Deserialize)]
struct Record {
    payload: String,
}

/// The task on a server that drops a replication connection silent for 3
/// seconds, for the shared source tests.
struct PostgresSource {
    server: Server,
    config: Arc<Cdc>,
}

impl PostgresSource {
    async fn start() -> Self {
        let server = start_postgres_with(&["-c", "wal_sender_timeout=3s"]).await;
        server
            .client
            .batch_execute(
                "CREATE TABLE records (id serial PRIMARY KEY, payload text);
                 CREATE PUBLICATION changes FOR ALL TABLES;",
            )
            .await
            .expect("create table");
        let mut config = cdc_config(&server, None);
        config.status_interval = Duration::from_secs(1);
        config.initial_copy = false;
        Self {
            config: Arc::new(config),
            server,
        }
    }
}

#[async_trait::async_trait]
impl flowgen_core::conformance::Source for PostgresSource {
    async fn produce(&self, payload: &str) {
        self.server
            .client
            .execute("INSERT INTO records (payload) VALUES ($1)", &[&payload])
            .await
            .expect("insert record");
    }

    async fn start(
        &self,
        context: Arc<flowgen_core::task::context::TaskContext>,
        tx: mpsc::Sender<Event>,
    ) -> tokio::task::JoinHandle<()> {
        run_cdc(Arc::clone(&self.config), context, tx)
    }

    fn payload(&self, event: &Event) -> String {
        let record: Record = serde_json::from_value(data(event).clone()).expect("record");
        record.payload
    }

    fn idle(&self) -> Duration {
        Duration::from_secs(8)
    }

    fn ack_deadline(&self) -> Duration {
        Duration::from_secs(3)
    }
}

flowgen_core::source_conformance!(PostgresSource::start().await);
