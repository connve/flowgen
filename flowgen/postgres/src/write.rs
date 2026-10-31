//! The `postgres_write` task: writes rows into a PostgreSQL table with COPY.

use crate::client::{Client, Error as ClientError};
use crate::query::{default_connection_timeout, default_max_connections, default_query_timeout};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{record_batch_rows, Event, EventBuilder, EventData, EventExt};
use flowgen_core::task::context::TaskContext;
use flowgen_core::task::runner::Runner;
use flowgen_core::task::TaskConfig;
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_postgres::Transaction;
use tracing::error;

/// Configuration for the `postgres_write` task.
///
/// Writes the rows of each event into a table with `COPY`.
///
/// ```yaml
/// - postgres_write:
///     name: save_orders
///     credentials_path: /etc/postgres/credentials.json
///     table: public.orders
///     change_type: upsert
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Write {
    /// Unique task identifier.
    pub name: String,
    /// Path to the PostgreSQL credentials file (JSON).
    pub credentials_path: PathBuf,
    /// Table to write to, as `table` or `schema.table` (supports templating).
    pub table: String,
    /// How the rows change the table: appended when unset, or upserted or
    /// deleted by the table's primary key.
    #[serde(default)]
    pub change_type: Option<ChangeType>,
    /// Maximum connections in the pool.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Maximum time to open a connection.
    #[serde(default = "default_connection_timeout", with = "humantime_serde")]
    pub connection_timeout: Duration,
    /// Maximum time a statement runs on the server, and to wait for a
    /// connection from the pool.
    #[serde(default = "default_query_timeout", with = "humantime_serde")]
    pub query_timeout: Duration,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for Write {}

/// How the rows change the table.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChangeType {
    /// Inserts each row, or updates the row with its primary key.
    Upsert,
    /// Deletes the row with each row's primary key.
    Delete,
}

/// Errors that can occur while writing rows.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to render config: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Failed to read the event data: {source}")]
    EventData {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Event data must be a JSON object, an array of objects, or a record batch")]
    InvalidRows,
    #[error("Failed to build event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to serialize the write result: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Failed to send event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("Writing to '{table}' failed: {source}")]
    Write {
        table: String,
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("Table '{table}' has no primary key to match rows by")]
    NoPrimaryKey { table: String },
    #[error("The rows for '{table}' have no value for its primary key column '{column}'")]
    MissingKey { table: String, column: String },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(&'static str),
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

impl Error {
    /// Whether retrying might help: no connection, or a transient server
    /// error. The rows are written in one transaction, so a retry writes
    /// them once.
    fn is_retryable(&self) -> bool {
        match self {
            Self::Client(_) => true,
            Self::Write { source, .. } => crate::client::is_transient(source),
            _ => false,
        }
    }
}

/// What the task emits after writing an event's rows.
#[derive(Serialize)]
struct Written<'a> {
    table: &'a str,
    rows_written: u64,
}

/// Temporary table the rows are copied into before an upsert or delete.
const STAGING_TABLE: &str = "flowgen_write";

/// The rows of an event and the columns they write.
struct Batch {
    rows: Vec<Map<String, Value>>,
    columns: Vec<String>,
}

impl Batch {
    /// The rows of a JSON object, an array of objects, or a record batch,
    /// which writes all its columns and its binary values as `bytea`.
    fn from_event(data: &EventData) -> Result<Self, Error> {
        let rows = match data {
            EventData::ArrowRecordBatch(batch) => {
                record_batch_rows(batch, bytea).map_err(|source| Error::EventData { source })?
            }
            data => {
                json_rows(Value::try_from(data).map_err(|source| Error::EventData { source })?)?
            }
        };
        let columns = key_columns(&rows);
        Ok(Self { rows, columns })
    }
}

/// Bytes in the `bytea` hex format.
fn bytea(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(2 + 2 * bytes.len());
    text.push_str("\\x");
    for byte in bytes {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    text
}

/// The rows of JSON data: one object, or an array of objects.
fn json_rows(data: Value) -> Result<Vec<Map<String, Value>>, Error> {
    match data {
        Value::Object(row) => Ok(vec![row]),
        Value::Array(rows) => rows
            .into_iter()
            .map(|row| match row {
                Value::Object(row) => Ok(row),
                _ => Err(Error::InvalidRows),
            })
            .collect(),
        _ => Err(Error::InvalidRows),
    }
}

/// The keys the rows set, in the order they first appear.
fn key_columns(rows: &[Map<String, Value>]) -> Vec<String> {
    let mut seen = HashSet::new();
    rows.iter()
        .flat_map(|row| row.keys())
        .filter(|column| seen.insert(column.as_str()))
        .cloned()
        .collect()
}

/// A value as the text the server parses into its column; `None` for null.
fn text(value: &Value) -> Option<Cow<'_, str>> {
    match value {
        Value::Null => None,
        Value::Bool(true) => Some(Cow::Borrowed("true")),
        Value::Bool(false) => Some(Cow::Borrowed("false")),
        Value::Number(number) => Some(Cow::Owned(number.to_string())),
        Value::String(text) => Some(Cow::Borrowed(text)),
        value => Some(Cow::Owned(value.to_string())),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `schema.table` or `table` as a quoted identifier.
fn quote_table(table: &str) -> String {
    match table.split_once('.') {
        Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(table),
    }
}

fn column_list<S: AsRef<str>>(columns: &[S]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column.as_ref()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The rows as CSV for `COPY … (FORMAT csv)`: each value quoted, and a
/// missing value or null as an unquoted empty field, which COPY reads as null.
fn csv<'a>(rows: impl IntoIterator<Item = &'a Map<String, Value>>, columns: &[String]) -> BytesMut {
    let mut out = BytesMut::new();
    for row in rows {
        for (position, column) in columns.iter().enumerate() {
            if position > 0 {
                out.put_u8(b',');
            }
            if let Some(text) = row.get(column).and_then(text) {
                quoted(&mut out, &text);
            }
        }
        out.put_u8(b'\n');
    }
    out
}

fn quoted(out: &mut BytesMut, text: &str) {
    out.put_u8(b'"');
    for byte in text.bytes() {
        if byte == b'"' {
            out.put_u8(b'"');
        }
        out.put_u8(byte);
    }
    out.put_u8(b'"');
}

/// The last row of each key, in the order of those rows, since one
/// statement can't change a row twice. Keys compare as the text the server
/// reads, so `1` and `"1"` are one key.
fn last_per_key<'a>(rows: &'a [Map<String, Value>], key: &[String]) -> Vec<&'a Map<String, Value>> {
    let mut last = HashMap::new();
    for (position, row) in rows.iter().enumerate() {
        let values: Vec<Option<Cow<str>>> = key
            .iter()
            .map(|column| row.get(column).and_then(text))
            .collect();
        last.insert(values, position);
    }
    let mut positions: Vec<usize> = last.into_values().collect();
    positions.sort_unstable();
    positions
        .into_iter()
        .map(|position| &rows[position])
        .collect()
}

fn upsert_statement(table: &str, columns: &[String], key: &[String]) -> String {
    let list = column_list(columns);
    let updates: Vec<String> = columns
        .iter()
        .filter(|column| !key.iter().any(|key| key == *column))
        .map(|column| {
            let column = quote_ident(column);
            format!("{column} = EXCLUDED.{column}")
        })
        .collect();
    let action = match updates.is_empty() {
        true => "NOTHING".to_string(),
        false => format!("UPDATE SET {}", updates.join(", ")),
    };
    format!(
        "INSERT INTO {table} ({list}) SELECT {list} FROM {STAGING_TABLE} ON CONFLICT ({}) DO {action}",
        column_list(key)
    )
}

fn delete_statement(table: &str, key: &[String]) -> String {
    let matches: Vec<String> = key
        .iter()
        .map(|column| {
            let column = quote_ident(column);
            format!("target.{column} = staged.{column}")
        })
        .collect();
    format!(
        "DELETE FROM {table} AS target USING {STAGING_TABLE} AS staged WHERE {}",
        matches.join(" AND ")
    )
}

/// The primary key columns of `table`, in key order.
async fn primary_key(
    transaction: &Transaction<'_>,
    table: &str,
) -> Result<Vec<String>, tokio_postgres::Error> {
    transaction
        .query(
            "SELECT a.attname FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = $1::text::regclass AND i.indisprimary \
             ORDER BY array_position(i.indkey::int2[], a.attnum)",
            &[&table],
        )
        .await?
        .iter()
        .map(|row| row.try_get(0))
        .collect()
}

/// Streams `data` into `statement`, a `COPY … FROM STDIN`, and returns the
/// rows copied.
async fn copy(
    transaction: &Transaction<'_>,
    statement: &str,
    data: BytesMut,
) -> Result<u64, tokio_postgres::Error> {
    let sink = transaction.copy_in::<_, bytes::Bytes>(statement).await?;
    let mut sink = std::pin::pin!(sink);
    sink.send(data.freeze()).await?;
    sink.as_mut().finish().await
}

/// Handles individual write events.
pub struct EventHandler {
    client: Arc<Client>,
    config: Arc<Write>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    task_type: &'static str,
    task_context: Arc<TaskContext>,
}

#[async_trait]
impl flowgen_core::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    fn is_retryable(&self, error: &Error) -> bool {
        error.is_retryable()
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }
        let event = Arc::new(event);
        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let config = self
                .config
                .render_for_event(event.as_ref())
                .map_err(|source| Error::ConfigRender { source })?;
            let batch = Batch::from_event(&event.data)?;
            let rows_written = match batch.rows.is_empty() {
                true => 0,
                false => self.write(&config, &batch).await?,
            };
            let written = serde_json::to_value(Written {
                table: &config.table,
                rows_written,
            })
            .map_err(|source| Error::SerdeJson { source })?;
            let mut result = EventBuilder::new()
                .data(EventData::Json(written))
                .subject(format!("{}.{}", event.subject, config.name))
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;
            match (&self.tx, &event.completion_tx) {
                (None, Some(completion_tx)) => {
                    completion_tx.signal_completion(result.data_as_json().ok());
                }
                (Some(_), completion_tx) => result.completion_tx = completion_tx.clone(),
                (None, None) => {}
            }
            result
                .send_with_logging(self.tx.as_ref())
                .context("num_records", rows_written)
                .await
                .map_err(|source| Error::SendMessage { source })
        })
        .await
    }
}

impl EventHandler {
    /// Writes the rows in one transaction and returns how many rows changed.
    async fn write(&self, config: &Write, batch: &Batch) -> Result<u64, Error> {
        let failed = |source| Error::Write {
            table: config.table.clone(),
            source,
        };
        let table = quote_table(&config.table);
        let mut connection = self.client.connection().await?;
        let transaction = connection.transaction().await.map_err(failed)?;
        let written = match config.change_type {
            None => copy(
                &transaction,
                &format!(
                    "COPY {table} ({}) FROM STDIN (FORMAT csv)",
                    column_list(&batch.columns)
                ),
                csv(&batch.rows, &batch.columns),
            )
            .await
            .map_err(failed)?,
            Some(change_type) => {
                let key = primary_key(&transaction, &table).await.map_err(failed)?;
                if key.is_empty() {
                    return Err(Error::NoPrimaryKey {
                        table: config.table.clone(),
                    });
                }
                if let Some(column) = key.iter().find(|key| !batch.columns.contains(key)) {
                    return Err(Error::MissingKey {
                        table: config.table.clone(),
                        column: column.clone(),
                    });
                }
                let staged = match change_type {
                    ChangeType::Upsert => &batch.columns,
                    ChangeType::Delete => &key,
                };
                let list = column_list(staged);
                transaction
                    .batch_execute(&format!(
                        "CREATE TEMP TABLE {STAGING_TABLE} ON COMMIT DROP AS \
                         SELECT {list} FROM {table} WITH NO DATA"
                    ))
                    .await
                    .map_err(failed)?;
                copy(
                    &transaction,
                    &format!("COPY {STAGING_TABLE} ({list}) FROM STDIN (FORMAT csv)"),
                    csv(last_per_key(&batch.rows, &key), staged),
                )
                .await
                .map_err(failed)?;
                let statement = match change_type {
                    ChangeType::Upsert => upsert_statement(&table, staged, &key),
                    ChangeType::Delete => delete_statement(&table, &key),
                };
                transaction.execute(&statement, &[]).await.map_err(failed)?
            }
        };
        transaction.commit().await.map_err(failed)?;
        Ok(written)
    }
}

/// Write processor implementing the Runner trait.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Write>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<TaskContext>,
    task_type: &'static str,
}

#[async_trait]
impl Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self
            .config
            .render(&serde_json::Map::new())
            .map_err(|source| Error::ConfigRender { source })?;
        let client = self
            .task_context
            .client_registry
            .get_or_init(
                flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
                    .field("credentials_path", &config.credentials_path)
                    .field("max_connections", &config.max_connections)
                    .field("connection_timeout", &config.connection_timeout)
                    .field("query_timeout", &config.query_timeout)
                    .build(),
                || async {
                    let credentials = crate::client::read_credentials(&config.credentials_path)?;
                    Ok(Client::new(
                        &credentials,
                        config.max_connections,
                        config.connection_timeout,
                        config.query_timeout,
                    )
                    .await?)
                },
            )
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        Ok(EventHandler {
            client,
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize PostgreSQL write processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await?;

        flowgen_core::task::runner::process_events(
            self.rx,
            Arc::new(event_handler),
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
    }
}

/// Builder for creating Processor instances.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<Write>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<Write>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
        self
    }

    pub fn task_context(mut self, task_context: Arc<TaskContext>) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or(Error::MissingBuilderAttribute("config"))?,
            rx: self.rx.ok_or(Error::MissingBuilderAttribute("receiver"))?,
            tx: self.tx,
            task_id: self
                .task_id
                .ok_or(Error::MissingBuilderAttribute("task_id"))?,
            task_context: self
                .task_context
                .ok_or(Error::MissingBuilderAttribute("task_context"))?,
            task_type: self
                .task_type
                .ok_or(Error::MissingBuilderAttribute("task_type"))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(value: Value) -> Map<String, Value> {
        match value {
            Value::Object(row) => row,
            other => panic!("expected an object, got {other}"),
        }
    }

    #[test]
    fn config_deser_minimal_appends() {
        let yaml = r#"
            name: save_orders
            credentials_path: /etc/postgres/credentials.json
            table: public.orders
        "#;
        let config: Write = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.change_type, None);
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.query_timeout, Duration::from_secs(120));
    }

    #[test]
    fn config_deser_reads_the_change_type_and_rejects_unknown_fields() {
        let upsert: Write = serde_yaml::from_str(
            "{name: a, credentials_path: /c.json, table: t, change_type: upsert}",
        )
        .unwrap();
        assert_eq!(upsert.change_type, Some(ChangeType::Upsert));
        assert!(serde_yaml::from_str::<Write>(
            "{name: a, credentials_path: /c.json, table: t, key: [id]}"
        )
        .is_err());
    }

    fn columns(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn json_rows_are_one_object_or_an_array_of_objects_with_every_key_a_column() {
        let one = Batch::from_event(&EventData::Json(serde_json::json!({"a": 1}))).unwrap();
        let many = Batch::from_event(&EventData::Json(serde_json::json!([
            {"a": 1, "b": 2},
            {"b": 3, "c": 4}
        ])))
        .unwrap();

        assert_eq!(one.rows.len(), 1);
        assert_eq!(many.rows.len(), 2);
        assert_eq!(many.columns, ["a", "b", "c"]);
        assert!(matches!(
            Batch::from_event(&EventData::Json(serde_json::json!([{"a": 1}, 2]))),
            Err(Error::InvalidRows)
        ));
        assert!(matches!(
            Batch::from_event(&EventData::Json(Value::Null)),
            Err(Error::InvalidRows)
        ));
    }

    #[test]
    fn a_record_batch_writes_its_null_columns_and_binary_as_hex() {
        use arrow::array::{Array, BinaryArray, Int32Array, StringArray};
        let batch = arrow::record_batch::RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>),
            (
                "note",
                Arc::new(StringArray::from(vec![None::<&str>])) as Arc<dyn Array>,
            ),
            (
                "data",
                Arc::new(BinaryArray::from(vec![b"\x01\xff".as_slice()])) as Arc<dyn Array>,
            ),
        ])
        .unwrap();

        let batch = Batch::from_event(&EventData::ArrowRecordBatch(batch)).unwrap();

        let mut written = batch.columns.clone();
        written.sort();
        assert_eq!(written, ["data", "id", "note"]);
        assert_eq!(
            std::str::from_utf8(&csv(&batch.rows, &columns(&["id", "note", "data"]))).unwrap(),
            "\"1\",,\"\\x01ff\"\n"
        );
    }

    #[test]
    fn csv_quotes_every_value_and_leaves_null_empty() {
        let rows = [
            row(serde_json::json!({"a": "x,\"y\"\nz", "b": 1.5, "c": true, "d": {"k": [1]}})),
            row(serde_json::json!({"a": "", "b": null})),
        ];
        let out = csv(&rows, &columns(&["a", "b", "c", "d"]));
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "\"x,\"\"y\"\"\nz\",\"1.5\",\"true\",\"{\"\"k\"\":[1]}\"\n\"\",,,\n"
        );
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_table("orders"), r#""orders""#);
        assert_eq!(quote_table("sales.Orders"), r#""sales"."Orders""#);
        assert_eq!(quote_table(r#"a"b"#), r#""a""b""#);
    }

    #[test]
    fn the_last_row_of_each_key_is_kept_in_order() {
        let rows = [
            row(serde_json::json!({"id": 1, "v": "a"})),
            row(serde_json::json!({"id": 2, "v": "b"})),
            row(serde_json::json!({"id": "1", "v": "c"})),
        ];
        let kept: Vec<&Value> = last_per_key(&rows, &["id".to_string()])
            .iter()
            .map(|row| &row["v"])
            .collect();
        assert_eq!(kept, [&serde_json::json!("b"), &serde_json::json!("c")]);
    }

    #[test]
    fn statements_match_rows_by_the_key() {
        let key = ["id".to_string()];
        assert_eq!(
            upsert_statement(r#""t""#, &columns(&["id", "v"]), &key),
            r#"INSERT INTO "t" ("id", "v") SELECT "id", "v" FROM flowgen_write ON CONFLICT ("id") DO UPDATE SET "v" = EXCLUDED."v""#
        );
        assert_eq!(
            upsert_statement(r#""t""#, &columns(&["id"]), &key),
            r#"INSERT INTO "t" ("id") SELECT "id" FROM flowgen_write ON CONFLICT ("id") DO NOTHING"#
        );
        assert_eq!(
            delete_statement(r#""t""#, &key),
            r#"DELETE FROM "t" AS target USING flowgen_write AS staged WHERE target."id" = staged."id""#
        );
    }

    #[test]
    fn missing_keys_and_bad_rows_are_not_retried() {
        assert!(!Error::NoPrimaryKey {
            table: "t".to_string()
        }
        .is_retryable());
        assert!(!Error::InvalidRows.is_retryable());
    }
}
