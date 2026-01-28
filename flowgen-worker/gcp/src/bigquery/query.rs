//! BigQuery query processor for executing SQL queries.
//!
//! Handles BigQuery query execution with parameterized queries for SQL injection
//! protection. Processes events by executing queries and publishing results as
//! new events in Arrow RecordBatch format for efficient columnar processing.

use arrow::array::*;
use arrow::datatypes::*;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
};
use gcloud_auth::credentials::CredentialsFile;
use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::get_query_results::{
    GetQueryResultsRequest, GetQueryResultsResponse,
};
use google_cloud_bigquery::http::job::query::{QueryRequest, QueryResponse};
use google_cloud_bigquery::http::table::TableFieldSchema;
use google_cloud_bigquery::http::tabledata::list::{Tuple, Value};
use google_cloud_bigquery::http::types::{QueryParameter, QueryParameterType, QueryParameterValue};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during BigQuery query operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Sending event to channel failed: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
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
    #[error("Failed to parse BigQuery date value: {source}")]
    DateParse {
        #[source]
        source: chrono::ParseError,
    },
    #[error("Failed to parse BigQuery time value: {source}")]
    TimeParse {
        #[source]
        source: chrono::ParseError,
    },
    #[error("Invalid date/time value")]
    InvalidDateTime,
}

/// Event handler for processing individual query events.
pub struct EventHandler {
    client: Arc<Client>,
    task_id: usize,
    tx: Option<Sender<Event>>,
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

        // Execute query.
        let record_batch = execute_query(&self.client, &config).await?;

        // Build result event.
        let result_event = EventBuilder::new()
            .data(EventData::ArrowRecordBatch(record_batch))
            .subject(format!("{}.{}", event.subject, config.name))
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        result_event
            .send_with_logging(self.tx.as_ref())
            .await
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
    tx: Option<Sender<Event>>,
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
                Some(event) => {
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
                None => return Ok(()),
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
            tx: self.tx,
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
    // Build query request.
    let mut query_request = QueryRequest {
        query: config.query.clone(),
        use_legacy_sql: config.use_legacy_sql,
        use_query_cache: Some(config.use_query_cache),
        ..Default::default()
    };

    // Add parameters.
    if let Some(ref params) = config.parameters {
        query_request.query_parameters = params
            .iter()
            .map(|(name, value)| build_query_parameter(name, value))
            .collect::<Result<Vec<_>, _>>()?;
    }

    // Set options.
    if let Some(timeout) = config.timeout {
        query_request.timeout_ms = Some(timeout.as_millis() as i64);
    }
    if let Some(max) = config.max_results {
        query_request.max_results = Some(max as i64);
    }
    if let Some(ref location) = config.location {
        query_request.location = location.clone();
    }
    if let Some(create_session) = config.create_session {
        query_request.create_session = Some(create_session);
    }
    if let Some(ref labels) = config.labels {
        query_request.labels = Some(labels.clone());
    }

    // Execute query.
    let response: QueryResponse = client
        .job()
        .query(&config.project_id, &query_request)
        .await
        .map_err(|source| Error::QueryExecution { source })?;

    // If query is not complete, poll for results using getQueryResults.
    let (schema, job_ref, mut all_rows, mut page_token) = if !response.job_complete {
        let result = poll_query_results(client, &response).await?;
        (
            result.schema,
            result.job_reference,
            result.rows.unwrap_or_default(),
            result.page_token,
        )
    } else {
        (
            response.schema,
            response.job_reference,
            response.rows.unwrap_or_default(),
            response.page_token,
        )
    };

    // Fetch all pages if there are more results.
    while let Some(token) = page_token {
        let page_response = get_query_results_page(client, &job_ref, &token).await?;

        if let Some(mut rows) = page_response.rows {
            all_rows.append(&mut rows);
        }

        page_token = page_response.page_token;
    }

    // Convert to RecordBatch with schema and all rows.
    response_to_record_batch(schema, all_rows)
}

/// Poll for query completion using getQueryResults API.
///
/// This function polls BigQuery to check if an asynchronous query job has completed.
/// Each call to getQueryResults has a 10-second server-side timeout, meaning BigQuery
/// waits up to 10 seconds for the job to complete before returning. If the job is not
/// complete, we wait 1 second before the next poll attempt.
///
/// Polling continues indefinitely until either the job completes or BigQuery returns
/// an error. Any errors from the BigQuery API will propagate up and be handled by the
/// retry system at the handler level.
async fn poll_query_results(
    client: &Client,
    initial_response: &QueryResponse,
) -> Result<GetQueryResultsResponse, Error> {
    let job_ref = &initial_response.job_reference;
    let poll_interval = std::time::Duration::from_secs(1);

    let request = GetQueryResultsRequest {
        start_index: 0,
        page_token: None,
        max_results: None,
        timeout_ms: Some(10000), // Server-side timeout: wait up to 10s for job completion.
        location: job_ref.location.clone(),
        format_options: None,
    };

    loop {
        let result_response = client
            .job()
            .get_query_results(&job_ref.project_id, &job_ref.job_id, &request)
            .await
            .map_err(|source| Error::QueryExecution { source })?;

        if result_response.job_complete {
            return Ok(result_response);
        }

        // Wait before next poll attempt.
        tokio::time::sleep(poll_interval).await;
    }
}

/// Fetch a single page of query results using pageToken.
async fn get_query_results_page(
    client: &Client,
    job_ref: &google_cloud_bigquery::http::job::JobReference,
    page_token: &str,
) -> Result<GetQueryResultsResponse, Error> {
    let request = GetQueryResultsRequest {
        start_index: 0,
        page_token: Some(page_token.to_string()),
        max_results: None,
        timeout_ms: None,
        location: job_ref.location.clone(),
        format_options: None,
    };

    client
        .job()
        .get_query_results(&job_ref.project_id, &job_ref.job_id, &request)
        .await
        .map_err(|source| Error::QueryExecution { source })
}

/// Convert BigQuery schema and rows to Arrow RecordBatch.
fn response_to_record_batch(
    schema: Option<google_cloud_bigquery::http::table::TableSchema>,
    rows: Vec<Tuple>,
) -> Result<arrow::array::RecordBatch, Error> {
    // Get schema.
    let bq_schema = schema.ok_or_else(|| Error::MissingSchema)?;

    // Convert BigQuery schema to Arrow schema.
    let arrow_fields: Vec<Field> = bq_schema.fields.iter().map(field_to_arrow).collect();
    let arrow_schema = Arc::new(Schema::new(arrow_fields));

    if rows.is_empty() {
        // Return empty batch with correct schema.
        return Ok(arrow::array::RecordBatch::new_empty(arrow_schema));
    }

    // Build columns.
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
        TableFieldType::Numeric => DataType::Decimal128(38, 9),
        TableFieldType::Bignumeric => DataType::Decimal256(76, 38),
        TableFieldType::Datetime => DataType::Utf8,
        TableFieldType::Json => DataType::Utf8,
        _ => DataType::Utf8,
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
        TableFieldType::Date => {
            let mut builder = Date32Builder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => {
                        // BigQuery returns dates as "YYYY-MM-DD"
                        // Date32 is days since Unix epoch (1970-01-01)
                        match parse_date_to_days(s) {
                            Ok(days) => builder.append_value(days),
                            Err(_) => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Time => {
            let mut builder = Time64MicrosecondBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => {
                        // BigQuery returns time as "HH:MM:SS[.ffffff]"
                        // Time64 is microseconds since midnight
                        match parse_time_to_micros(s) {
                            Ok(micros) => builder.append_value(micros),
                            Err(_) => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Bytes => {
            let mut builder = BinaryBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => {
                        match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                        {
                            Ok(bytes) => builder.append_value(&bytes),
                            Err(_) => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Numeric => {
            let mut builder = Decimal128Builder::new()
                .with_precision_and_scale(38, 9)
                .map_err(|e| Error::Arrow { source: e })?;
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match parse_decimal_128(s, 9) {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Bignumeric => {
            let mut builder = Decimal256Builder::new()
                .with_precision_and_scale(76, 38)
                .map_err(|e| Error::Arrow { source: e })?;
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match parse_decimal_256(s, 38) {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Datetime | TableFieldType::Json => {
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

/// Parse BigQuery NUMERIC string to Decimal128.
fn parse_decimal_128(decimal_str: &str, scale: i8) -> Result<i128, Box<dyn std::error::Error>> {
    let value: f64 = decimal_str.parse()?;
    let multiplier = 10_f64.powi(scale as i32);
    Ok((value * multiplier).round() as i128)
}

/// Parse BigQuery BIGNUMERIC string to Decimal256.
fn parse_decimal_256(
    decimal_str: &str,
    scale: i8,
) -> Result<arrow::datatypes::i256, Box<dyn std::error::Error>> {
    use arrow::datatypes::i256;
    let value: f64 = decimal_str.parse()?;
    let multiplier = 10_f64.powi(scale as i32);
    let scaled = (value * multiplier).round() as i128;
    Ok(i256::from_i128(scaled))
}

/// Parse BigQuery date string (YYYY-MM-DD) to days since Unix epoch.
fn parse_date_to_days(date_str: &str) -> Result<i32, Error> {
    use chrono::NaiveDate;
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|source| Error::DateParse { source })?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or(Error::InvalidDateTime)?;
    Ok((date - epoch).num_days() as i32)
}

/// Parse BigQuery time string (HH:MM:SS[.ffffff]) to microseconds since midnight.
fn parse_time_to_micros(time_str: &str) -> Result<i64, Error> {
    use chrono::NaiveTime;
    let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(time_str, "%H:%M:%S"))
        .map_err(|source| Error::TimeParse { source })?;

    let midnight = NaiveTime::from_hms_opt(0, 0, 0).ok_or(Error::InvalidDateTime)?;
    let duration = time - midnight;
    duration.num_microseconds().ok_or(Error::InvalidDateTime)
}

/// Build a query parameter from name and JSON value.
fn build_query_parameter(name: &str, value: &JsonValue) -> Result<QueryParameter, Error> {
    use super::config::{PARAM_TYPE_BOOL, PARAM_TYPE_FLOAT64, PARAM_TYPE_INT64, PARAM_TYPE_STRING};

    let (parameter_type, parameter_value) = match value {
        JsonValue::String(s) => (
            QueryParameterType {
                parameter_type: PARAM_TYPE_STRING.to_string(),
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
                        parameter_type: PARAM_TYPE_INT64.to_string(),
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
                        parameter_type: PARAM_TYPE_FLOAT64.to_string(),
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
                parameter_type: PARAM_TYPE_BOOL.to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: Some(b.to_string()),
                ..Default::default()
            },
        ),
        JsonValue::Null => (
            QueryParameterType {
                parameter_type: PARAM_TYPE_STRING.to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: None,
                ..Default::default()
            },
        ),
        _ => (
            QueryParameterType {
                parameter_type: PARAM_TYPE_STRING.to_string(),
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

#[cfg(test)]
mod tests {
    use super::super::config::{
        PARAM_TYPE_BOOL, PARAM_TYPE_FLOAT64, PARAM_TYPE_INT64, PARAM_TYPE_STRING,
    };
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_date_to_days() {
        assert_eq!(parse_date_to_days("1970-01-01").unwrap(), 0);
        assert_eq!(parse_date_to_days("1970-01-02").unwrap(), 1);
        assert_eq!(parse_date_to_days("2024-01-01").unwrap(), 19723);
        assert!(parse_date_to_days("invalid").is_err());
        assert!(parse_date_to_days("2024-13-01").is_err());
    }

    #[test]
    fn test_parse_time_to_micros() {
        assert_eq!(parse_time_to_micros("00:00:00").unwrap(), 0);
        assert_eq!(parse_time_to_micros("00:00:01").unwrap(), 1_000_000);
        assert_eq!(parse_time_to_micros("01:00:00").unwrap(), 3_600_000_000);
        assert_eq!(parse_time_to_micros("23:59:59").unwrap(), 86_399_000_000);
        assert_eq!(
            parse_time_to_micros("12:30:45.123456").unwrap(),
            45_045_123_456
        );
        assert!(parse_time_to_micros("invalid").is_err());
        assert!(parse_time_to_micros("25:00:00").is_err());
    }

    #[test]
    fn test_parse_decimal_128() {
        assert_eq!(parse_decimal_128("123.45", 2).unwrap(), 12345);
        assert_eq!(parse_decimal_128("0.001", 3).unwrap(), 1);
        assert_eq!(parse_decimal_128("1000.5", 1).unwrap(), 10005);
        assert!(parse_decimal_128("invalid", 2).is_err());
    }

    #[test]
    fn test_parse_decimal_256() {
        let result = parse_decimal_256("123.45", 2).unwrap();
        assert_eq!(result, arrow::datatypes::i256::from_i128(12345));
        assert!(parse_decimal_256("invalid", 2).is_err());
    }

    #[test]
    fn test_build_query_parameter_string() {
        let param = build_query_parameter("name", &json!("test")).unwrap();
        assert_eq!(param.name, Some("name".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_STRING);
        assert_eq!(param.parameter_value.value, Some("test".to_string()));
    }

    #[test]
    fn test_build_query_parameter_int64() {
        let param = build_query_parameter("count", &json!(42)).unwrap();
        assert_eq!(param.name, Some("count".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_INT64);
        assert_eq!(param.parameter_value.value, Some("42".to_string()));
    }

    #[test]
    fn test_build_query_parameter_float64() {
        let param = build_query_parameter("price", &json!(99.99)).unwrap();
        assert_eq!(param.name, Some("price".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_FLOAT64);
        assert_eq!(param.parameter_value.value, Some("99.99".to_string()));
    }

    #[test]
    fn test_build_query_parameter_bool() {
        let param = build_query_parameter("active", &json!(true)).unwrap();
        assert_eq!(param.name, Some("active".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_BOOL);
        assert_eq!(param.parameter_value.value, Some("true".to_string()));
    }

    #[test]
    fn test_build_query_parameter_null() {
        let param = build_query_parameter("optional", &json!(null)).unwrap();
        assert_eq!(param.name, Some("optional".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_STRING);
        assert_eq!(param.parameter_value.value, None);
    }

    #[test]
    fn test_field_to_arrow_string() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "test".to_string(),
            data_type: TableFieldType::String,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "test");
        assert_eq!(arrow_field.data_type(), &DataType::Utf8);
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_field_to_arrow_int64() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "count".to_string(),
            data_type: TableFieldType::Int64,
            mode: Some(TableFieldMode::Required),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "count");
        assert_eq!(arrow_field.data_type(), &DataType::Int64);
        assert!(!arrow_field.is_nullable());
    }

    #[test]
    fn test_field_to_arrow_timestamp() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "created_at".to_string(),
            data_type: TableFieldType::Timestamp,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "created_at");
        assert_eq!(
            arrow_field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_field_to_arrow_date() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "birth_date".to_string(),
            data_type: TableFieldType::Date,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "birth_date");
        assert_eq!(arrow_field.data_type(), &DataType::Date32);
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_field_to_arrow_numeric() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "amount".to_string(),
            data_type: TableFieldType::Numeric,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "amount");
        assert_eq!(arrow_field.data_type(), &DataType::Decimal128(38, 9));
        assert!(arrow_field.is_nullable());
    }

    #[tokio::test]
    async fn test_processor_builder_missing_config() {
        let result = ProcessorBuilder::new().build().await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingRequiredAttribute(_)
        ));
    }

    #[test]
    fn test_build_column_string() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("test1".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("test2".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
        ];

        let field = TableFieldSchema {
            name: "name".to_string(),
            data_type: TableFieldType::String,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let string_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "test1");
        assert_eq!(string_array.value(1), "test2");
        assert!(string_array.is_null(2));
    }

    #[test]
    fn test_build_column_int64() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("42".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("100".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("invalid".to_string()),
                }],
            },
        ];

        let field = TableFieldSchema {
            name: "count".to_string(),
            data_type: TableFieldType::Int64,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let int_array = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(int_array.len(), 4);
        assert_eq!(int_array.value(0), 42);
        assert_eq!(int_array.value(1), 100);
        assert!(int_array.is_null(2));
        assert!(int_array.is_null(3));
    }

    #[test]
    fn test_build_column_float64() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("99.99".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("123.45".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
        ];

        let field = TableFieldSchema {
            name: "price".to_string(),
            data_type: TableFieldType::Float64,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let float_array = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(float_array.len(), 3);
        assert!((float_array.value(0) - 99.99).abs() < f64::EPSILON);
        assert!((float_array.value(1) - 123.45).abs() < f64::EPSILON);
        assert!(float_array.is_null(2));
    }

    #[test]
    fn test_build_column_bool() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("true".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("false".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
        ];

        let field = TableFieldSchema {
            name: "active".to_string(),
            data_type: TableFieldType::Bool,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let bool_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(bool_array.len(), 3);
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));
        assert!(bool_array.is_null(2));
    }

    #[test]
    fn test_build_column_date() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("2024-01-15".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("1970-01-01".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("invalid".to_string()),
                }],
            },
        ];

        let field = TableFieldSchema {
            name: "date".to_string(),
            data_type: TableFieldType::Date,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let date_array = result.as_any().downcast_ref::<Date32Array>().unwrap();

        assert_eq!(date_array.len(), 4);
        assert_eq!(date_array.value(0), 19737);
        assert_eq!(date_array.value(1), 0);
        assert!(date_array.is_null(2));
        assert!(date_array.is_null(3));
    }

    #[test]
    fn test_build_column_time() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("12:30:45".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("00:00:00".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
        ];

        let field = TableFieldSchema {
            name: "time".to_string(),
            data_type: TableFieldType::Time,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let time_array = result
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();

        assert_eq!(time_array.len(), 3);
        assert_eq!(time_array.value(0), 45_045_000_000);
        assert_eq!(time_array.value(1), 0);
        assert!(time_array.is_null(2));
    }

    #[test]
    fn test_build_column_bytes() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("aGVsbG8=".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("invalid base64!".to_string()),
                }],
            },
        ];

        let field = TableFieldSchema {
            name: "data".to_string(),
            data_type: TableFieldType::Bytes,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let binary_array = result.as_any().downcast_ref::<BinaryArray>().unwrap();

        assert_eq!(binary_array.len(), 3);
        assert_eq!(binary_array.value(0), b"hello");
        assert!(binary_array.is_null(1));
        assert!(binary_array.is_null(2));
    }

    #[test]
    fn test_response_to_record_batch_empty() {
        use google_cloud_bigquery::http::table::{TableFieldType, TableSchema};

        let schema = Some(TableSchema {
            fields: vec![TableFieldSchema {
                name: "test".to_string(),
                data_type: TableFieldType::String,
                ..Default::default()
            }],
        });

        let result = response_to_record_batch(schema, vec![]).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_response_to_record_batch_with_data() {
        use google_cloud_bigquery::http::table::{TableFieldType, TableSchema};
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let schema = Some(TableSchema {
            fields: vec![
                TableFieldSchema {
                    name: "name".to_string(),
                    data_type: TableFieldType::String,
                    ..Default::default()
                },
                TableFieldSchema {
                    name: "count".to_string(),
                    data_type: TableFieldType::Int64,
                    ..Default::default()
                },
            ],
        });

        let rows = vec![
            Tuple {
                f: vec![
                    Cell {
                        v: Value::String("Alice".to_string()),
                    },
                    Cell {
                        v: Value::String("10".to_string()),
                    },
                ],
            },
            Tuple {
                f: vec![
                    Cell {
                        v: Value::String("Bob".to_string()),
                    },
                    Cell {
                        v: Value::String("20".to_string()),
                    },
                ],
            },
        ];

        let result = response_to_record_batch(schema, rows).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        let name_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");

        let count_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 10);
        assert_eq!(count_col.value(1), 20);
    }

    #[test]
    fn test_response_to_record_batch_missing_schema() {
        let result = response_to_record_batch(None, vec![]);
        assert!(matches!(result.unwrap_err(), Error::MissingSchema));
    }
}
