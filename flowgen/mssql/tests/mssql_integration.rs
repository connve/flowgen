//! Integration tests for the `mssql_query` task against SQL Server in a
//! Docker container: SQL Server 2022, or Azure SQL Edge on arm64, where SQL
//! Server does not run.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt8Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use flowgen_core::event::{Event, EventBuilder, EventData};
use flowgen_mssql::query::{ProcessorBuilder, Query};
use flowgen_mssql::Client;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::mpsc;

const PASSWORD: &str = "Flowgen_Pass1";

struct Server {
    _container: ContainerAsync<GenericImage>,
    _credentials: tempfile::NamedTempFile,
    credentials_path: PathBuf,
    client: Client,
}

impl Server {
    async fn execute(&self, statement: &str) {
        let mut connection = self.client.connection().await.expect("connection");
        connection
            .simple_query(statement)
            .await
            .expect("run statement")
            .into_results()
            .await
            .expect("read results");
    }
}

async fn start_mssql() -> Server {
    let (name, tag) = match cfg!(target_arch = "aarch64") {
        true => ("mcr.microsoft.com/azure-sql-edge", "latest"),
        false => ("mcr.microsoft.com/mssql/server", "2022-latest"),
    };
    let container = GenericImage::new(name, tag)
        .with_exposed_port(1433.tcp())
        .with_env_var("ACCEPT_EULA", "Y")
        .with_env_var("MSSQL_SA_PASSWORD", PASSWORD)
        .start()
        .await
        .expect("start sql server container");
    let port = container
        .get_host_port_ipv4(1433)
        .await
        .expect("map sql server port");

    let credentials = tempfile::NamedTempFile::new().expect("create credentials file");
    std::fs::write(
        credentials.path(),
        format!(
            r#"{{"host": "127.0.0.1", "port": {port}, "database": "master", "username": "sa", "password": "{PASSWORD}", "trust_server_certificate": true, "encrypt": false}}"#
        ),
    )
    .expect("write credentials file");
    let client = connect(credentials.path()).await;
    Server {
        _container: container,
        credentials_path: credentials.path().to_path_buf(),
        _credentials: credentials,
        client,
    }
}

/// Connects once the server accepts logins, which takes a while after start.
async fn connect(credentials_path: &std::path::Path) -> Client {
    let credentials =
        flowgen_mssql::client::read_credentials(credentials_path).expect("credentials");
    let mut last_error = None;
    for _ in 0..60 {
        match Client::new(
            &credentials,
            2,
            Duration::from_secs(5),
            Duration::from_secs(10),
        )
        .await
        {
            Ok(client) => return client,
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("sql server did not accept logins in time: {last_error:?}");
}

fn query(server: &Server, statement: &str, parameters: &[&str], batch_size: usize) -> Query {
    serde_json::from_value(json!({
        "name": "read",
        "credentials_path": server.credentials_path,
        "query": statement,
        "parameters": parameters,
        "batch_size": batch_size,
    }))
    .expect("query config")
}

fn task_context() -> Arc<flowgen_core::task::context::TaskContext> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("build TaskManager"),
    );
    Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("test_flow".to_string())
            .task_manager(task_manager)
            .cache(Arc::new(flowgen_core::cache::memory::MemoryCache::new()))
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
        .task_type("mssql_query")
        .task_context(task_context())
        .build()
        .expect("build processor");
    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = processor.run().await;
    });

    let mut trigger = EventBuilder::new()
        .subject("trigger".to_string())
        .data(EventData::Json(json!({})))
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
    let server = start_mssql().await;
    server
        .execute(
            "CREATE TABLE sample (
                flag bit, tiny tinyint NOT NULL, small smallint NOT NULL, regular int NOT NULL,
                big bigint NULL, single real NOT NULL, wide float NULL, exact decimal(18, 4),
                cash money, body nvarchar(50), label varchar(20), id uniqueidentifier,
                day date, at time, old_at datetime, local_at datetime2, utc_at datetimeoffset,
                blob varbinary(10), doc xml
            );
            INSERT INTO sample VALUES (
                1, 200, -7, 42, 9000000000, 1.5, 2.25, -12345.6789, 12.34, N'zażółć', 'label',
                'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', '2026-10-27', '12:30:00.5',
                '2026-10-27 12:30:00', '2026-10-27 12:30:00.123456', '2026-10-27 12:30:00 +02:00',
                0x0102, '<a>1</a>'
            );
            INSERT INTO sample (tiny, small, regular, single) VALUES (0, 0, 0, 0);",
        )
        .await;

    let events = run(
        query(
            &server,
            "SELECT * FROM sample ORDER BY regular DESC",
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
    assert_eq!(column::<UInt8Array>(batch, "tiny").value(0), 200);
    assert_eq!(column::<Int16Array>(batch, "small").value(0), -7);
    assert_eq!(column::<Int32Array>(batch, "regular").value(0), 42);
    assert_eq!(column::<Int64Array>(batch, "big").value(0), 9_000_000_000);
    assert!(column::<Int64Array>(batch, "big").is_null(1));
    assert_eq!(column::<Float32Array>(batch, "single").value(0), 1.5);
    assert_eq!(column::<Float64Array>(batch, "wide").value(0), 2.25);
    assert_eq!(
        column::<StringArray>(batch, "exact").value(0),
        "-12345.6789"
    );
    assert_eq!(column::<Float64Array>(batch, "cash").value(0), 12.34);
    assert_eq!(column::<StringArray>(batch, "body").value(0), "zażółć");
    assert_eq!(column::<StringArray>(batch, "label").value(0), "label");
    assert_eq!(
        column::<StringArray>(batch, "id").value(0).to_lowercase(),
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    );
    assert_eq!(column::<Date32Array>(batch, "day").value(0), 20_753);
    assert_eq!(
        column::<Time64MicrosecondArray>(batch, "at").value(0),
        45_000_500_000
    );
    assert_eq!(
        column::<TimestampMicrosecondArray>(batch, "old_at").value(0),
        1_793_104_200_000_000
    );
    assert_eq!(
        column::<TimestampMicrosecondArray>(batch, "local_at").value(0),
        1_793_104_200_123_456
    );
    assert_eq!(
        column::<TimestampMicrosecondArray>(batch, "utc_at").value(0),
        1_793_097_000_000_000
    );
    assert_eq!(
        batch
            .schema()
            .field_with_name("utc_at")
            .unwrap()
            .data_type(),
        &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(column::<BinaryArray>(batch, "blob").value(0), &[1, 2]);
    assert_eq!(column::<StringArray>(batch, "doc").value(0), "<a>1</a>");
    for name in [
        "flag", "exact", "body", "day", "local_at", "utc_at", "blob", "doc",
    ] {
        assert!(
            batch.column_by_name(name).unwrap().is_null(1),
            "{name} is null"
        );
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn rows_are_split_into_batches_and_the_last_one_carries_completion() {
    let server = start_mssql().await;
    let (completion_tx, completion_rx) = flowgen_core::event::new_completion_channel(1);

    let events = run(
        query(
            &server,
            "SELECT TOP 5 ROW_NUMBER() OVER (ORDER BY object_id) AS n FROM sys.objects",
            &[],
            2,
        ),
        Some(completion_tx),
    )
    .await;

    let sizes: Vec<usize> = events.iter().map(|event| batch(event).num_rows()).collect();
    assert_eq!(sizes, [2, 2, 1]);
    assert!(events[..2]
        .iter()
        .all(|event| event.completion_tx.is_none()));
    let last = events[2]
        .completion_tx
        .as_ref()
        .expect("last batch completion");
    last.signal_completion(None);
    assert!(completion_rx.await.expect("completion").is_ok());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn text_parameters_bind_and_an_empty_result_keeps_the_columns() {
    let server = start_mssql().await;
    server
        .execute("CREATE TABLE people (id int, name nvarchar(20)); INSERT INTO people VALUES (1, N'Ann'), (2, N'Grace');")
        .await;

    let found = run(
        query(
            &server,
            "SELECT name FROM people WHERE id = @P1",
            &["2"],
            100,
        ),
        None,
    )
    .await;
    let empty = run(
        query(
            &server,
            "SELECT id, name FROM people WHERE id = @P1",
            &["9"],
            100,
        ),
        None,
    )
    .await;

    assert_eq!(
        column::<StringArray>(batch(&found[0]), "name").value(0),
        "Grace"
    );
    let empty = batch(&empty[0]);
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.schema().fields().len(), 2);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_sql_error_fails_the_event_without_retries() {
    let server = start_mssql().await;

    let events = run(
        query(&server, "SELECT * FROM missing_table", &[], 100),
        None,
    )
    .await;

    assert_eq!(events.len(), 1);
    assert!(events[0]
        .error
        .as_deref()
        .is_some_and(|error| error.contains("missing_table")));
}
