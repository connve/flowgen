//! BigQuery query processor for executing SQL queries.
//!
//! Handles BigQuery query execution with parameterized queries for SQL injection
//! protection. Processes events by executing queries and publishing results as
//! new events in Arrow RecordBatch format for efficient columnar processing.

use arrow::array::*;
use arrow::datatypes::*;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, SenderExt},
};
use gcloud_auth::credentials::CredentialsFile;
use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::query::{QueryRequest, QueryResponse};
use google_cloud_bigquery::http::table::TableFieldSchema;
use google_cloud_bigquery::http::tabledata::list::{Tuple, Value};
use google_cloud_bigquery::http::types::{QueryParameter, QueryParameterType, QueryParameterValue};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during BigQuery query operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Sending event to channel failed with error: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error("Query event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("JSON serialization/deserialization failed with error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Configuration template rendering failed with error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("BigQuery client authentication failed with error: {source}")]
    ClientAuth {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery client creation failed with error: {source}")]
    ClientCreation {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery client connection failed with error: {source}")]
    ClientConnection {
        #[source]
        source: gcloud_gax::conn::Error,
    },
    #[error("BigQuery query execution failed with error: {source}")]
    QueryExecution {
        #[source]
        source: google_cloud_bigquery::http::error::Error,
    },
    #[error("BigQuery response missing schema")]
    MissingSchema,
    #[error("Arrow data processing failed with error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

/// Event handler for processing individual query events.
pub struct EventHandler {
    client: Arc<Client>,
    task_id: usize,
    tx: Sender<Event>,
    config: Arc<super::config::Query>,
    task_type: &'static str,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        // Render config to support templates inside configuration.
        let event_value = serde_json::value::Value::try_from(&event)
            .map_err(|source| Error::EventBuilder { source })?;
        let config = self
            .config
            .render(&event_value)
            .map_err(|source| Error::ConfigRender { source })?;

        // Execute query
        let record_batch = execute_query(&self.client, &config).await?;

        // Build result event
        let result_event = EventBuilder::new()
            .data(EventData::ArrowRecordBatch(record_batch))
            .subject(format!("{}.{}", event.subject, config.name))
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        self.tx
            .send_with_logging(result_event)
            .map_err(|source| Error::SendMessage { source })?;

        Ok(())
    }
}

/// BigQuery query processor that executes SQL queries and publishes results as Arrow RecordBatch.
#[derive(Debug)]
pub struct Processor {
    /// Query configuration including credentials and SQL query.
    config: Arc<super::config::Query>,
    /// Receiver for incoming events to process.
    rx: Receiver<Event>,
    /// Channel sender for result events.
    tx: Sender<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the processor by establishing BigQuery client connection.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Loading credentials from file
    /// - Creating BigQuery client with authentication
    async fn init(&self) -> Result<EventHandler, Error> {
        let credentials = CredentialsFile::new_from_file(
            self.config.credentials_path.to_string_lossy().to_string(),
        )
        .await
        .map_err(|source| Error::ClientAuth { source })?;

        let (client_config, _project_id) = ClientConfig::new_with_credentials(credentials)
            .await
            .map_err(|source| Error::ClientCreation { source })?;

        let client = Arc::new(
            Client::new(client_config)
                .await
                .map_err(|source| Error::ClientConnection { source })?,
        );

        let event_handler = EventHandler {
            client,
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(
                    "{}",
                    Error::RetryExhausted {
                        source: Box::new(e)
                    }
                );
                return Ok(());
            }
        };

        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        error!("{}", e);
                                        Err(e)
                                    }
                                }
                            })
                            .await;

                            if let Err(e) = result {
                                error!(
                                    "{}",
                                    Error::RetryExhausted {
                                        source: Box::new(e)
                                    }
                                );
                            }
                        }
                        .in_current_span(),
                    );
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

/// Builder for creating BigQuery query processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Query>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            rx: None,
            tx: None,
            task_id: None,
            task_context: None,
            task_type: None,
        }
    }

    pub fn config(mut self, config: Arc<super::config::Query>) -> Self {
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

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingRequiredAttribute("task_id".to_string()))?,
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}

impl Default for ProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Executes a query and returns Arrow RecordBatch.
async fn execute_query(
    client: &Client,
    config: &super::config::Query,
) -> Result<arrow::array::RecordBatch, Error> {
    // Build query request
    let mut query_request = QueryRequest {
        query: config.query.clone(),
        use_legacy_sql: false,
        ..Default::default()
    };

    // Add parameters
    if let Some(ref params) = config.parameters {
        query_request.query_parameters = params
            .iter()
            .map(|(name, value)| build_query_parameter(name, value))
            .collect::<Result<Vec<_>, _>>()?;
    }

    // Set options
    if let Some(timeout) = config.timeout {
        query_request.timeout_ms = Some(timeout.as_millis() as i64);
    }
    if let Some(max) = config.max_results {
        query_request.max_results = Some(max as i64);
    }

    // Execute query
    let response: QueryResponse = client
        .job()
        .query(&config.project_id, &query_request)
        .await
        .map_err(|source| Error::QueryExecution { source })?;

    // Convert to RecordBatch
    response_to_record_batch(response)
}

/// Convert QueryResponse to Arrow RecordBatch.
fn response_to_record_batch(response: QueryResponse) -> Result<arrow::array::RecordBatch, Error> {
    // Get schema
    let bq_schema = response.schema.ok_or_else(|| Error::MissingSchema)?;

    // Get rows
    let rows = response.rows.unwrap_or_default();

    if rows.is_empty() {
        // Return empty batch
        let arrow_schema = Arc::new(Schema::empty());
        return Ok(arrow::array::RecordBatch::new_empty(arrow_schema));
    }

    // Convert BigQuery schema to Arrow schema
    let arrow_fields: Vec<Field> = bq_schema.fields.iter().map(field_to_arrow).collect();

    let arrow_schema = Arc::new(Schema::new(arrow_fields));

    // Build columns
    let columns: Vec<ArrayRef> = (0..bq_schema.fields.len())
        .map(|col_idx| {
            let field = &bq_schema.fields[col_idx];
            build_column(&rows, col_idx, field)
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Create RecordBatch
    arrow::array::RecordBatch::try_new(arrow_schema, columns)
        .map_err(|source| Error::Arrow { source })
}

/// Convert BigQuery field to Arrow field.
fn field_to_arrow(field: &TableFieldSchema) -> Field {
    use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};

    let dtype = match &field.data_type {
        TableFieldType::String => DataType::Utf8,
        TableFieldType::Int64 | TableFieldType::Integer => DataType::Int64,
        TableFieldType::Float64 | TableFieldType::Float => DataType::Float64,
        TableFieldType::Bool | TableFieldType::Boolean => DataType::Boolean,
        TableFieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        TableFieldType::Date => DataType::Date32,
        TableFieldType::Time => DataType::Time64(TimeUnit::Microsecond),
        TableFieldType::Bytes => DataType::Binary,
        _ => DataType::Utf8, // Fallback for unknown types
    };

    let nullable = field.mode != Some(TableFieldMode::Required);
    Field::new(&field.name, dtype, nullable)
}

/// Build a single Arrow column from BigQuery rows.
fn build_column(
    rows: &[Tuple],
    col_idx: usize,
    field: &TableFieldSchema,
) -> Result<ArrayRef, Error> {
    use google_cloud_bigquery::http::table::TableFieldType;

    Ok(match &field.data_type {
        TableFieldType::String => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => builder.append_value(s),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Int64 | TableFieldType::Integer => {
            let mut builder = Int64Builder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match s.parse::<i64>() {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Float64 | TableFieldType::Float => {
            let mut builder = Float64Builder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match s.parse::<f64>() {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Bool | TableFieldType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match s.parse::<bool>() {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Timestamp => {
            let mut builder = TimestampMicrosecondBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match s.parse::<f64>() {
                        Ok(seconds) => {
                            let micros = (seconds * 1_000_000.0) as i64;
                            builder.append_value(micros);
                        }
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        _ => {
            // Fallback to string for unknown types
            let mut builder = StringBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => builder.append_value(s),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
    })
}

/// Build a query parameter from name and JSON value.
fn build_query_parameter(name: &str, value: &JsonValue) -> Result<QueryParameter, Error> {
    let (parameter_type, parameter_value) = match value {
        JsonValue::String(s) => (
            QueryParameterType {
                parameter_type: "STRING".to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: Some(s.clone()),
                ..Default::default()
            },
        ),
        JsonValue::Number(n) => {
            if n.is_i64() || n.is_u64() {
                (
                    QueryParameterType {
                        parameter_type: "INT64".to_string(),
                        ..Default::default()
                    },
                    QueryParameterValue {
                        value: Some(n.to_string()),
                        ..Default::default()
                    },
                )
            } else {
                (
                    QueryParameterType {
                        parameter_type: "FLOAT64".to_string(),
                        ..Default::default()
                    },
                    QueryParameterValue {
                        value: Some(n.to_string()),
                        ..Default::default()
                    },
                )
            }
        }
        JsonValue::Bool(b) => (
            QueryParameterType {
                parameter_type: "BOOL".to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: Some(b.to_string()),
                ..Default::default()
            },
        ),
        JsonValue::Null => (
            QueryParameterType {
                parameter_type: "STRING".to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: None,
                ..Default::default()
            },
        ),
        _ => (
            QueryParameterType {
                parameter_type: "STRING".to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: Some(
                    serde_json::to_string(value).map_err(|source| Error::SerdeJson { source })?,
                ),
                ..Default::default()
            },
        ),
    };

    Ok(QueryParameter {
        name: Some(name.to_string()),
        parameter_type,
        parameter_value,
    })
}
