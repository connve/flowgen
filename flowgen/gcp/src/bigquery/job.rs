//! Unified BigQuery job processor.
//!
//! Handles all BigQuery job operations (create, get, cancel, delete) through a single
//! processor with operation-specific logic in the handle method.

use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
    task::TaskConfig,
};

use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::cancel::CancelJobRequest;
use google_cloud_bigquery::http::job::get::GetJobRequest;
use google_cloud_bigquery::http::job::{
    CreateDisposition as BqCreateDisposition, Job as BqJob, JobConfiguration, JobConfigurationLoad,
    JobReference, JobState, JobType, WriteDisposition as BqWriteDisposition,
};
use google_cloud_bigquery::http::table::{
    SourceFormat as BqSourceFormat, TableReference as BqTableReference, TableSchema,
};
use google_cloud_bigquery::http::types::ErrorProto;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

/// Job operation type for unified job processor.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobOperation {
    /// Create a new job (load, query, extract, copy).
    Create,
    /// Get job status and poll until completion.
    Get,
    /// Cancel a running job.
    Cancel,
    /// Delete job metadata.
    Delete,
}

/// Unified configuration for BigQuery job operations.
///
/// This single config handles all job operations: create, get, cancel, and delete.
/// The operation type determines which fields are required and what action is performed.
///
/// # Examples
///
/// Create a load job:
/// ```yaml
/// bigquery_job:
///   name: import_events
///   operation: create
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   source_uris:
///     - "gs://my-bucket/data/*.parquet"
///   destination_table:
///     project_id: my-project-id
///     dataset_id: analytics
///     table_id: events
///   source_format: parquet
///   write_disposition: write_append
///   autodetect: true
/// ```
///
/// Poll job status:
/// ```yaml
/// bigquery_job:
///   name: check_load_status
///   operation: get
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   job_id: "{{event.data.job_id}}"
///   poll_interval: 5s
///   max_poll_duration: 30m
/// ```
///
/// Cancel a job:
/// ```yaml
/// bigquery_job:
///   name: stop_job
///   operation: cancel
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   job_id: "{{event.data.job_id}}"
/// ```
///
/// Delete job metadata:
/// ```yaml
/// bigquery_job:
///   name: cleanup_job
///   operation: delete
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   job_id: "{{event.data.job_id}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Job {
    /// Unique task identifier.
    pub name: String,
    /// Job operation type (create, get, cancel, delete).
    pub operation: JobOperation,
    /// Path to GCP service account credentials JSON file.
    /// When omitted, falls back to Application Default Credentials (ADC).
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// GCP project ID where BigQuery resources are located.
    pub project_id: String,
    /// Optional GCP project ID for running the job (billing project).
    #[serde(default)]
    pub job_project_id: Option<String>,
    /// Optional BigQuery dataset location (e.g., "US", "EU", "us-central1").
    #[serde(default)]
    pub location: Option<String>,
    // Fields for create operation.
    /// Job type for create operation (load, query, extract, copy). Default: load.
    #[serde(default = "default_job_type")]
    pub job_type: String,
    /// GCS URIs of source files to load (create operation only).
    /// Supports both literal arrays and template expressions that resolve to arrays at runtime.
    #[serde(default)]
    pub source_uris: Option<serde_json::Value>,
    /// Destination BigQuery table (create operation only).
    #[serde(default)]
    pub destination_table: Option<TableReference>,
    /// Source file format (create operation only).
    #[serde(default)]
    pub source_format: Option<SourceFormat>,
    /// How to handle existing data (create operation only).
    #[serde(default)]
    pub write_disposition: Option<WriteDisposition>,
    /// Whether to create table if it doesn't exist (create operation only).
    #[serde(default)]
    pub create_disposition: Option<CreateDisposition>,
    /// Whether to automatically infer schema (create operation only).
    #[serde(default)]
    pub autodetect: Option<bool>,
    /// Explicit table schema (create operation only).
    /// When provided, overrides autodetect for precise schema control.
    #[serde(default)]
    pub schema: Option<Vec<TableField>>,
    /// Optional maximum number of bad records (create operation only).
    #[serde(default)]
    pub max_bad_records: Option<i32>,
    /// Optional labels for the job.
    #[serde(default)]
    pub labels: Option<HashMap<String, String>>,

    // Fields for get/cancel/delete operations.
    /// Job ID (get, cancel, delete operations).
    #[serde(default)]
    pub job_id: Option<String>,

    // Fields for get operation.
    /// Polling interval between status checks (get operation only).
    #[serde(default = "default_poll_interval", with = "humantime_serde")]
    pub poll_interval: Duration,
    /// Maximum duration to poll before giving up (get operation only).
    #[serde(default = "default_max_poll_duration", with = "humantime_serde")]
    pub max_poll_duration: Duration,

    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for Job {}

impl Job {
    /// Returns the project ID to use for running the job.
    pub fn get_job_project_id(&self) -> &str {
        self.job_project_id.as_deref().unwrap_or(&self.project_id)
    }
}

/// BigQuery table reference.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TableReference {
    /// GCP project ID containing the table.
    pub project_id: String,
    /// BigQuery dataset ID.
    pub dataset_id: String,
    /// BigQuery table ID.
    pub table_id: String,
}

/// Source file formats for BigQuery load jobs.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceFormat {
    Parquet,
    Csv,
    #[serde(rename = "newline_delimited_json")]
    #[default]
    NewlineDelimitedJson,
    Avro,
}

impl SourceFormat {
    pub fn as_str(&self) -> &str {
        match self {
            SourceFormat::Parquet => "PARQUET",
            SourceFormat::Csv => "CSV",
            SourceFormat::NewlineDelimitedJson => "NEWLINE_DELIMITED_JSON",
            SourceFormat::Avro => "AVRO",
        }
    }
}

/// BigQuery field type for schema definitions.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Bytes,
    Integer,
    Int64,
    Float,
    Float64,
    Numeric,
    Bignumeric,
    Boolean,
    Bool,
    Timestamp,
    Date,
    Time,
    Datetime,
    Geography,
    Json,
    Record,
}

impl From<FieldType> for google_cloud_bigquery::http::table::TableFieldType {
    fn from(ft: FieldType) -> Self {
        match ft {
            FieldType::String => Self::String,
            FieldType::Bytes => Self::Bytes,
            FieldType::Integer | FieldType::Int64 => Self::Integer,
            FieldType::Float | FieldType::Float64 => Self::Float,
            FieldType::Numeric | FieldType::Bignumeric => Self::Numeric,
            FieldType::Boolean | FieldType::Bool => Self::Boolean,
            FieldType::Timestamp => Self::Timestamp,
            FieldType::Date => Self::Date,
            FieldType::Time => Self::Time,
            FieldType::Datetime => Self::Datetime,
            FieldType::Geography => Self::Json,
            FieldType::Json => Self::Json,
            FieldType::Record => Self::Record,
        }
    }
}

/// BigQuery field mode for schema definitions.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldMode {
    #[default]
    Nullable,
    Required,
    Repeated,
}

impl From<FieldMode> for google_cloud_bigquery::http::table::TableFieldMode {
    fn from(fm: FieldMode) -> Self {
        match fm {
            FieldMode::Nullable => Self::Nullable,
            FieldMode::Required => Self::Required,
            FieldMode::Repeated => Self::Repeated,
        }
    }
}

/// Table field schema definition.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TableField {
    /// Field name.
    pub name: String,
    /// Field data type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Field mode (nullable, required, repeated).
    #[serde(default)]
    pub mode: FieldMode,
    /// Optional field description.
    #[serde(default)]
    pub description: Option<String>,
}

impl From<TableField> for google_cloud_bigquery::http::table::TableFieldSchema {
    fn from(tf: TableField) -> Self {
        Self {
            name: tf.name,
            data_type: tf.field_type.into(),
            mode: Some(tf.mode.into()),
            description: tf.description,
            fields: None,
            policy_tags: None,
            max_length: None,
            precision: None,
            scale: None,
            collation: None,
            default_value_expression: None,
            rounding_mode: None,
        }
    }
}

/// Write disposition for BigQuery load jobs.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WriteDisposition {
    /// Append data to existing table. Create table if it doesn't exist.
    #[serde(rename = "write_append")]
    #[default]
    WriteAppend,
    /// Truncate table before loading data. Create table if it doesn't exist.
    #[serde(rename = "write_truncate")]
    WriteTruncate,
    /// Only load if table is empty. Fail if table exists and has data.
    #[serde(rename = "write_empty")]
    WriteEmpty,
}

impl WriteDisposition {
    pub fn as_str(&self) -> &str {
        match self {
            WriteDisposition::WriteAppend => "WRITE_APPEND",
            WriteDisposition::WriteTruncate => "WRITE_TRUNCATE",
            WriteDisposition::WriteEmpty => "WRITE_EMPTY",
        }
    }
}

/// Create disposition for BigQuery load jobs.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CreateDisposition {
    /// Create table if it doesn't exist (default).
    #[serde(rename = "create_if_needed")]
    #[default]
    CreateIfNeeded,
    /// Never create table. Fail if table doesn't exist.
    #[serde(rename = "create_never")]
    CreateNever,
}

impl CreateDisposition {
    pub fn as_str(&self) -> &str {
        match self {
            CreateDisposition::CreateIfNeeded => "CREATE_IF_NEEDED",
            CreateDisposition::CreateNever => "CREATE_NEVER",
        }
    }
}

/// Default poll interval for job status checks (5 seconds).
fn default_poll_interval() -> Duration {
    Duration::from_secs(5)
}

/// Default maximum poll duration (30 minutes).
fn default_max_poll_duration() -> Duration {
    Duration::from_secs(1800)
}

/// Default job type for create operation.
fn default_job_type() -> String {
    "load".to_string()
}

/// Response for delete operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeleteJobResponse {
    job_id: String,
    project_id: String,
    deleted: bool,
}

/// Wrapper for ErrorProto to provide Display implementation.
#[derive(Debug, Clone)]
pub struct JobErrorWrapper(ErrorProto);

impl std::fmt::Display for JobErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();

        if let Some(ref message) = self.0.message {
            parts.push(message.clone());
        }

        if let Some(ref reason) = self.0.reason {
            parts.push(format!("reason: {reason}"));
        }

        if let Some(ref location) = self.0.location {
            parts.push(format!("location: {location}"));
        }

        write!(f, "{}", parts.join(", "))
    }
}

/// Errors that can occur during BigQuery job operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("BigQuery client authentication error: {source}")]
    ClientAuth {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery client creation error: {source}")]
    ClientCreation {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery client connection error: {source}")]
    ClientConnection {
        #[source]
        source: gcloud_gax::conn::Error,
    },
    #[error("BigQuery HTTP client build error: {source}")]
    HttpClientBuild {
        #[source]
        source: reqwest::Error,
    },
    #[error("BigQuery job operation error: {source}")]
    JobOperation {
        #[source]
        source: google_cloud_bigquery::http::error::Error,
    },
    #[error("Error serializing job response to JSON: {source}")]
    JobSerialization {
        #[source]
        source: serde_json::Error,
    },
    #[error("Job polling timed out after {duration:?}")]
    PollTimeout { duration: std::time::Duration },
    #[error("Job failed: {error}")]
    JobFailed { error: JobErrorWrapper },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Create operation requires source_uris")]
    MissingSourceUris,
    #[error("Invalid source_uris format after template rendering: expected array, got {value}")]
    InvalidSourceUrisFormat { value: String },
    #[error("Create operation requires destination_table")]
    MissingDestinationTable,
    #[error("Create operation requires source_format")]
    MissingSourceFormat,
    #[error("Get operation requires job_id")]
    MissingJobIdForGet,
    #[error("Cancel operation requires job_id")]
    MissingJobIdForCancel,
    #[error("Delete operation requires job_id")]
    MissingJobIdForDelete,
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// Event handler for processing individual job operation requests.
pub struct EventHandler {
    client: Arc<Client>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<Job>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config to support templates inside configuration.
            let config = self
                .config
                .render_for_event(event.as_ref())
                .map_err(|source| Error::ConfigRender { source })?;

            // Execute operation based on type.
            let (result_data, job_id) = match config.operation {
                JobOperation::Create => {
                    let mut job = self.create_job(&config).await?;
                    let data = serde_json::to_value(&job)
                        .map_err(|source| Error::JobSerialization { source })?;
                    let job_id = if !job.job_reference.job_id.is_empty() {
                        Some(std::mem::take(&mut job.job_reference.job_id))
                    } else {
                        None
                    };
                    (data, job_id)
                }
                JobOperation::Get => {
                    let mut job = self.get_job(&config).await?;
                    let data = serde_json::to_value(&job)
                        .map_err(|source| Error::JobSerialization { source })?;
                    let job_id = if !job.job_reference.job_id.is_empty() {
                        Some(std::mem::take(&mut job.job_reference.job_id))
                    } else {
                        None
                    };
                    (data, job_id)
                }
                JobOperation::Cancel => {
                    let mut job = self.cancel_job(&config).await?;
                    let data = serde_json::to_value(&job)
                        .map_err(|source| Error::JobSerialization { source })?;
                    let job_id = if !job.job_reference.job_id.is_empty() {
                        Some(std::mem::take(&mut job.job_reference.job_id))
                    } else {
                        None
                    };
                    (data, job_id)
                }
                JobOperation::Delete => {
                    let response = self.delete_job(&config).await?;
                    let data = serde_json::to_value(&response)
                        .map_err(|source| Error::JobSerialization { source })?;
                    (data, None)
                }
            };

            let mut event_builder = EventBuilder::new()
                .data(EventData::Json(result_data))
                .subject(format!("{}.{}", event.subject, config.name))
                .task_id(self.task_id)
                .task_type(self.task_type);

            if let Some(id) = job_id {
                event_builder = event_builder.id(id);
            }

            let mut result_event = event_builder
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            // Signal completion or pass through to next task.
            match self.tx {
                None => {
                    // Leaf task: signal completion.
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(result_event.data_as_json().ok());
                    }
                }
                Some(_) => {
                    // Pass through completion_tx to next task.
                    result_event.completion_tx = completion_tx_arc.clone();
                }
            }

            result_event
                .send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

impl EventHandler {
    /// Creates a BigQuery job and returns the complete job response.
    async fn create_job(&self, config: &Job) -> Result<BqJob, Error> {
        let source_uris_value = config
            .source_uris
            .as_ref()
            .ok_or(Error::MissingSourceUris)?;

        // Extract Vec<String> from Value after template rendering.
        let source_uris: Vec<String> = match source_uris_value {
            serde_json::Value::Array(arr) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            serde_json::Value::String(s) => {
                return Err(Error::InvalidSourceUrisFormat { value: s.clone() });
            }
            _ => {
                return Err(Error::InvalidSourceUrisFormat {
                    value: source_uris_value.to_string(),
                });
            }
        };
        let destination_table = config
            .destination_table
            .as_ref()
            .ok_or(Error::MissingDestinationTable)?;
        let source_format = config
            .source_format
            .as_ref()
            .ok_or(Error::MissingSourceFormat)?;

        // Build table reference from configuration.
        let table_ref = BqTableReference {
            project_id: destination_table.project_id.clone(),
            dataset_id: destination_table.dataset_id.clone(),
            table_id: destination_table.table_id.clone(),
        };

        // Map config enums to BigQuery library enums.
        let source_fmt = match source_format {
            SourceFormat::Parquet => BqSourceFormat::Parquet,
            SourceFormat::Csv => BqSourceFormat::Csv,
            SourceFormat::NewlineDelimitedJson => BqSourceFormat::NewlineDelimitedJson,
            SourceFormat::Avro => BqSourceFormat::Avro,
        };

        let write_disp = match config.write_disposition.as_ref() {
            Some(WriteDisposition::WriteAppend) | None => BqWriteDisposition::WriteAppend,
            Some(WriteDisposition::WriteTruncate) => BqWriteDisposition::WriteTruncate,
            Some(WriteDisposition::WriteEmpty) => BqWriteDisposition::WriteEmpty,
        };

        let create_disp = match config.create_disposition.as_ref() {
            Some(CreateDisposition::CreateIfNeeded) | None => BqCreateDisposition::CreateIfNeeded,
            Some(CreateDisposition::CreateNever) => BqCreateDisposition::CreateNever,
        };

        // Convert schema configuration to TableSchema if provided.
        let schema = config.schema.as_ref().map(|fields| TableSchema {
            fields: fields.iter().map(|f| f.clone().into()).collect(),
        });

        let load_config = JobConfigurationLoad {
            source_uris: source_uris.clone(),
            destination_table: table_ref,
            source_format: Some(source_fmt),
            write_disposition: Some(write_disp),
            create_disposition: Some(create_disp),
            autodetect: Some(config.autodetect.unwrap_or(false)),
            schema,
            max_bad_records: config.max_bad_records.map(|v| v as i64),
            ..Default::default()
        };

        let job_config = JobConfiguration {
            job_type: config.job_type.clone(),
            job: JobType::Load(load_config),
            labels: config.labels.clone(),
            ..Default::default()
        };

        let job = BqJob {
            job_reference: JobReference {
                project_id: config.get_job_project_id().to_string(),
                job_id: String::new(), // Let BigQuery generate job ID
                location: config.location.clone(),
            },
            configuration: job_config,
            ..Default::default()
        };

        self.client
            .job()
            .create(&job)
            .await
            .map_err(|source| Error::JobOperation { source })
    }

    /// Gets job status and polls until completion or timeout.
    async fn get_job(&self, config: &Job) -> Result<BqJob, Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobIdForGet)?;

        let start_time = Instant::now();

        loop {
            // Check if we've exceeded max poll duration.
            if start_time.elapsed() > config.max_poll_duration {
                return Err(Error::PollTimeout {
                    duration: config.max_poll_duration,
                });
            }

            // Get job status from BigQuery API.
            let request = GetJobRequest {
                location: config.location.clone(),
            };

            let response = self
                .client
                .job()
                .get(&config.project_id, job_id, &request)
                .await
                .map_err(|source| Error::JobOperation { source })?;

            if matches!(response.status.state, JobState::Done) {
                // Check if job failed and return error details.
                if let Some(error) = response.status.error_result {
                    return Err(Error::JobFailed {
                        error: JobErrorWrapper(error),
                    });
                }

                return Ok(response);
            }

            // Wait before next poll attempt.
            tokio::time::sleep(config.poll_interval).await;
        }
    }

    /// Cancels a BigQuery job and returns the job response.
    async fn cancel_job(&self, config: &Job) -> Result<BqJob, Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobIdForCancel)?;

        let request = CancelJobRequest {
            location: config.location.clone(),
        };

        let response = self
            .client
            .job()
            .cancel(&config.project_id, job_id, &request)
            .await
            .map_err(|source| Error::JobOperation { source })?;

        Ok(response.job)
    }

    /// Deletes a BigQuery job's metadata.
    async fn delete_job(&self, config: &Job) -> Result<DeleteJobResponse, Error> {
        let job_id = config.job_id.as_ref().ok_or(Error::MissingJobIdForDelete)?;

        self.client
            .job()
            .delete(&config.project_id, job_id)
            .await
            .map_err(|source| Error::JobOperation { source })?;

        Ok(DeleteJobResponse {
            job_id: job_id.clone(),
            project_id: config.project_id.clone(),
            deleted: true,
        })
    }
}

/// Unified BigQuery job processor.
#[derive(Debug)]
pub struct Processor {
    /// Job configuration including credentials and operation parameters.
    config: Arc<Job>,
    /// Receiver for incoming events to process.
    rx: Receiver<Event>,
    /// Channel sender for result events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the processor by establishing BigQuery client connection.
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let credentials_path = init_config.credentials_path.clone();
        let client = self
            .task_context
            .client_registry
            .get_or_init(
                flowgen_core::client_registry::ClientKey::new(self.task_type, &credentials_path),
                || async {
                    let credentials = crate::resolve_credentials(&credentials_path)
                        .await
                        .map_err(|source| Error::ClientAuth { source })?;
                    let (client_config, _project_id) =
                        ClientConfig::new_with_credentials(credentials)
                            .await
                            .map_err(|source| Error::ClientCreation { source })?;
                    let client_config = crate::with_keep_alive(client_config)
                        .map_err(|source| Error::HttpClientBuild { source })?;
                    Client::new(client_config)
                        .await
                        .map_err(|source| Error::ClientConnection { source })
                },
            )
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        let event_handler = EventHandler {
            client,
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize job processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        flowgen_core::task::runner::process_events(
            self.rx,
            event_handler,
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
    }
}

/// Builder for creating unified Job processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<Job>>,
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

    pub fn config(mut self, config: Arc<Job>) -> Self {
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
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingBuilderAttribute("task_id".to_string()))?,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

impl Default for ProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_job_response_serialization() {
        let response = DeleteJobResponse {
            job_id: "job_123".to_string(),
            project_id: "test-project".to_string(),
            deleted: true,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("job_123"));
        assert!(json.contains("test-project"));
        assert!(json.contains("true"));
    }
}
