//! The `postgres_query` task: PostgreSQL queries with Arrow RecordBatch output.

use crate::client::{Client, Error as ClientError};
use crate::rows::{Converter, Error as RowsError};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::BytesMut;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt, SharedCompletionTx};
use flowgen_core::task::context::TaskContext;
use flowgen_core::task::runner::Runner;
use flowgen_core::task::TaskConfig;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};
use tracing::error;

/// Configuration for the `postgres_query` task.
///
/// Runs a statement and emits its rows as Arrow record batches.
///
/// ```yaml
/// - postgres_query:
///     name: fetch_orders
///     credentials_path: /etc/postgres/credentials.json
///     query: "SELECT * FROM orders WHERE status = $1 AND created_at > $2"
///     parameters:
///       - "{{event.data.status}}"
///       - "{{event.data.since}}"
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Query {
    /// Unique task identifier.
    pub name: String,
    /// Path to the PostgreSQL credentials file (JSON).
    pub credentials_path: PathBuf,
    /// SQL statement, inline or from a resource file (supports Handlebars templating).
    pub query: flowgen_core::resource::Source,
    /// Values bound to `$1`, `$2`, ... as text, converted to the parameter types by the server.
    #[serde(default)]
    pub parameters: Option<Vec<String>>,
    /// Rows per record batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: NonZeroUsize,
    /// Maximum connections in the pool.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Maximum time to open a connection.
    #[serde(default = "default_connection_timeout", with = "humantime_serde")]
    pub connection_timeout: Duration,
    /// Maximum time a statement runs on the server, and to wait for a
    /// connection from the pool and the first rows.
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

impl ConfigExt for Query {}

fn default_batch_size() -> NonZeroUsize {
    NonZeroUsize::MIN.saturating_add(9_999)
}

pub(crate) fn default_max_connections() -> u32 {
    10
}

pub(crate) fn default_connection_timeout() -> Duration {
    Duration::from_secs(30)
}

pub(crate) fn default_query_timeout() -> Duration {
    Duration::from_secs(120)
}

/// Errors that can occur during query processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to render config: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Failed to load query from resource: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Failed to build event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to send event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("Query failed: {source}")]
    Query {
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("Reading the rows failed after some batches were sent: {source}")]
    Stream {
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("Query did not return rows within {timeout:?}")]
    QueryTimeout { timeout: Duration },
    #[error(transparent)]
    Rows(#[from] RowsError),
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(&'static str),
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

impl Error {
    /// Whether retrying might help: no connection, or a transient server
    /// error. A query that failed after sending batches is not retried, as
    /// that would send them again.
    fn is_retryable(&self) -> bool {
        match self {
            Self::Client(_) => true,
            Self::Query { source } => crate::client::is_transient(source),
            _ => false,
        }
    }
}

/// A parameter sent in text format, so the server parses it as whatever type
/// the statement expects.
#[derive(Debug)]
struct Parameter<'a>(&'a str);

impl ToSql for Parameter<'_> {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, _: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

/// Handles individual query execution events.
pub struct EventHandler {
    client: Arc<Client>,
    config: Arc<Query>,
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
            let event_value = serde_json::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;
            let statement = config
                .query
                .render(self.task_context.resource_loader.as_ref(), &event_value)
                .await
                .map_err(|source| Error::ResourceLoad { source })?;
            let subject = format!("{}.{}", event.subject, config.name);
            self.run_query(&config, &statement, &subject, &event.completion_tx)
                .await
        })
        .await
    }
}

impl EventHandler {
    /// Streams the rows of `statement`, emitting a record batch every
    /// `batch_size` rows. Completion travels with the last batch, so one
    /// batch is held back until the next one is full or the rows end.
    async fn run_query(
        &self,
        config: &Query,
        statement: &str,
        subject: &str,
        completion_tx: &Option<SharedCompletionTx>,
    ) -> Result<(), Error> {
        let connection = self.client.connection().await?;
        let timeout = self.client.query_timeout();
        let parameters: Vec<Parameter> = match &config.parameters {
            Some(parameters) => parameters.iter().map(|value| Parameter(value)).collect(),
            None => Vec::new(),
        };
        let started = async {
            let prepared = connection.prepare(statement).await?;
            let rows = connection
                .query_raw(
                    &prepared,
                    parameters.iter().map(|parameter| parameter as &dyn ToSql),
                )
                .await?;
            Ok::<_, tokio_postgres::Error>((prepared, rows))
        };
        let (prepared, rows) = match tokio::time::timeout(timeout, started).await {
            Ok(started) => started.map_err(|source| Error::Query { source })?,
            Err(_) => return Err(Error::QueryTimeout { timeout }),
        };
        let converter = Converter::new(prepared.columns())?;

        let mut rows = std::pin::pin!(rows);
        let batch_size = config.batch_size.get();
        let mut chunk = Vec::with_capacity(batch_size);
        let mut pending: Option<RecordBatch> = None;
        let mut emitted = false;
        loop {
            let row = match rows.try_next().await {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(source) if emitted => return Err(Error::Stream { source }),
                Err(source) => return Err(Error::Query { source }),
            };
            chunk.push(row);
            if chunk.len() == batch_size {
                let batch = converter.batch(&chunk)?;
                chunk.clear();
                if let Some(previous) = pending.replace(batch) {
                    self.emit(previous, subject, None).await?;
                    emitted = true;
                }
            }
        }
        let last = match (pending, chunk.is_empty()) {
            (Some(previous), true) => previous,
            (previous, _) => {
                if let Some(previous) = previous {
                    self.emit(previous, subject, None).await?;
                }
                converter.batch(&chunk)?
            }
        };
        self.emit(last, subject, Some(completion_tx)).await
    }

    /// Sends one batch; `completion_tx` is set on the last batch only.
    async fn emit(
        &self,
        batch: RecordBatch,
        subject: &str,
        completion_tx: Option<&Option<SharedCompletionTx>>,
    ) -> Result<(), Error> {
        let mut event = EventBuilder::new()
            .data(EventData::ArrowRecordBatch(batch))
            .subject(subject.to_string())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;
        match (completion_tx, &self.tx) {
            (Some(Some(completion_tx)), None) => {
                completion_tx.signal_completion(event.data_as_json().ok());
            }
            (Some(completion_tx), Some(_)) => event.completion_tx = completion_tx.clone(),
            _ => {}
        }
        event
            .send_with_logging(self.tx.as_ref())
            .await
            .map_err(|source| Error::SendMessage { source })
    }
}

/// Query processor implementing the Runner trait.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Query>,
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
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let client = self
            .task_context
            .client_registry
            .get_or_init(
                flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
                    .field("credentials_path", &init_config.credentials_path)
                    .field("max_connections", &init_config.max_connections)
                    .field("connection_timeout", &init_config.connection_timeout)
                    .field("query_timeout", &init_config.query_timeout)
                    .build(),
                || async {
                    let credentials =
                        crate::client::read_credentials(&init_config.credentials_path)?;
                    Ok(Client::new(
                        &credentials,
                        init_config.max_connections,
                        init_config.connection_timeout,
                        init_config.query_timeout,
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
                        error!(error = %e, "Failed to initialize PostgreSQL query processor");
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
    config: Option<Arc<Query>>,
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

    pub fn config(mut self, config: Arc<Query>) -> Self {
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
    use flowgen_core::resource::Source;

    #[test]
    fn config_deser_minimal() {
        let yaml = r#"
            name: fetch_orders
            credentials_path: /etc/postgres/credentials.json
            query: "SELECT * FROM orders"
        "#;
        let config: Query = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.query,
            Source::Inline("SELECT * FROM orders".to_string())
        );
        assert_eq!(config.batch_size.get(), 10000);
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
        assert_eq!(config.query_timeout, Duration::from_secs(120));
        assert!(config.parameters.is_none());
    }

    #[test]
    fn config_deser_resource_query_with_parameters() {
        let yaml = r#"
            name: fetch_orders
            credentials_path: /etc/postgres/credentials.json
            query:
              resource: queries/orders.sql
            parameters:
              - "{{event.data.status}}"
            batch_size: 500
            query_timeout: 5m
        "#;
        let config: Query = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.query,
            Source::Resource {
                resource: "queries/orders.sql".to_string()
            }
        );
        assert_eq!(
            config.parameters,
            Some(vec!["{{event.data.status}}".to_string()])
        );
        assert_eq!(config.batch_size.get(), 500);
        assert_eq!(config.query_timeout, Duration::from_secs(300));
    }

    #[test]
    fn config_deser_rejects_unknown_fields() {
        let yaml = r#"
            name: typo
            credentials_path: /creds.json
            query: "SELECT 1"
            params: ["1"]
        "#;
        assert!(serde_yaml::from_str::<Query>(yaml).is_err());
    }

    #[test]
    fn config_deser_missing_query_fails() {
        let yaml = r#"
            name: missing
            credentials_path: /creds.json
        "#;
        assert!(serde_yaml::from_str::<Query>(yaml).is_err());
    }

    #[test]
    fn timeouts_and_row_errors_are_not_retried() {
        assert!(!Error::QueryTimeout {
            timeout: Duration::from_secs(1)
        }
        .is_retryable());
        assert!(!Error::Rows(RowsError::UnsupportedType {
            column: "tags".to_string(),
            type_name: "_text".to_string(),
        })
        .is_retryable());
    }

    #[test]
    fn a_parameter_is_sent_as_text_for_any_type() {
        let parameter = Parameter("42");
        let mut out = BytesMut::new();
        assert!(Parameter::accepts(&Type::INT4));
        assert!(matches!(parameter.encode_format(&Type::INT4), Format::Text));
        parameter.to_sql_checked(&Type::INT4, &mut out).unwrap();
        assert_eq!(&out[..], b"42");
    }
}
