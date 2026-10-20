//! BigQuery Storage Read API processor for high-throughput parallel table reads.
//!
//! Uses the BigQuery Storage Read API to efficiently read large tables in parallel
//! using multiple streams. Returns data in Arrow RecordBatch format for efficient
//! columnar processing. This API is optimized for reading large amounts of data
//! and does not support SQL queries.

use arrow::array::RecordBatch;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
    task::TaskConfig,
};

use gcloud_googleapis::cloud::bigquery::storage::v1::read_session::{
    TableModifiers, TableReadOptions,
};
use google_cloud_bigquery::client::{Client, ClientConfig, ReadTableOption};
use google_cloud_bigquery::http::table::TableReference;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

/// Data format for BigQuery Storage Read API.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum DataFormat {
    /// Arrow format (default, preferred for columnar processing).
    #[default]
    Arrow,
    /// Avro format (row-based).
    Avro,
}

/// Response compression codec for BigQuery Storage Read API.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum CompressionCodec {
    /// No compression (default).
    #[default]
    Unspecified,
    /// LZ4 compression.
    Lz4,
}

/// Configuration structure for BigQuery Storage Read API operations.
///
/// The Storage Read API provides high-throughput, parallel access to BigQuery tables
/// without running SQL queries. It is optimized for reading large tables with optional
/// column selection and row filtering.
///
/// # Key Benefits
/// - High-throughput parallel reads using multiple streams
/// - Column selection to reduce data transfer
/// - Server-side filtering with row_restriction
/// - Arrow format for efficient columnar processing
/// - Time-travel queries with snapshot_time
///
/// # Fields
/// - `name`: Unique name / identifier of the task.
/// - `credentials_path`: Path to GCP service account credentials JSON file.
/// - `project_id`: GCP project ID where BigQuery resources are located (data project).
/// - `job_project_id`: Optional GCP project ID for creating the read session (billing project). If not specified, uses `project_id`.
/// - `dataset_id`: BigQuery dataset ID containing the table.
/// - `table_id`: BigQuery table ID to read from.
/// - `selected_fields`: Optional list of column names to read (reads all if not specified).
/// - `row_restriction`: Optional SQL WHERE clause (without WHERE keyword) for server-side filtering.
/// - `sample_percentage`: Optional random sampling percentage (0.0 to 100.0).
/// - `compression_codec`: Optional response compression. Default: unspecified.
/// - `snapshot_time`: Optional timestamp for time-travel queries (RFC3339 format).
/// - `max_stream_count`: Optional maximum number of parallel read streams.
/// - `preferred_min_stream_count`: Optional minimum streams hint for optimization.
/// - `data_format`: Data format for results. Default: arrow.
/// - `retry`: Optional retry configuration (overrides app-level retry config).
///
/// # Examples
///
/// Basic table read with all columns:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: read_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: orders
/// ```
///
/// Read with column selection and filtering:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: read_recent_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: orders
///   selected_fields:
///     - order_id
///     - customer_id
///     - amount
///   row_restriction: "order_date >= '2024-01-01' AND status = 'completed'"
/// ```
///
/// Parallel read with compression:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: fast_table_scan
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: warehouse
///   table_id: large_table
///   max_stream_count: 10
///   compression_codec: lz4
/// ```
///
/// Time-travel query with sampling:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: historical_sample
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: events
///   snapshot_time: "2024-01-15T00:00:00Z"
///   sample_percentage: 10.0
/// ```
///
/// Cross-project read (data in one project, billing in another):
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: cross_project_read
///   credentials_path: /etc/gcp/credentials.json
///   project_id: data-project-id
///   job_project_id: billing-project-id
///   dataset_id: warehouse
///   table_id: large_table
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct StorageRead {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to GCP service account credentials JSON file.
    /// When omitted, falls back to Application Default Credentials (ADC).
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// GCP project ID where BigQuery resources are located (data project).
    pub project_id: String,
    /// Optional GCP project ID for creating the read session (billing project).
    /// If not specified, uses `project_id`.
    #[serde(default)]
    pub job_project_id: Option<String>,
    /// BigQuery dataset ID containing the table.
    pub dataset_id: String,
    /// BigQuery table ID to read from.
    pub table_id: String,
    /// Optional list of column names to read. If not specified, reads all columns.
    /// Supports nested field selection using dot notation (e.g., "struct_field.nested_field").
    #[serde(default)]
    pub selected_fields: Option<Vec<String>>,
    /// Optional SQL WHERE clause for server-side row filtering (without WHERE keyword).
    /// Example: "order_date >= '2024-01-01' AND status = 'completed'".
    #[serde(default)]
    pub row_restriction: Option<String>,
    /// Optional random sampling percentage (0.0 to 100.0).
    /// Samples rows randomly from the table before applying filters.
    #[serde(default)]
    pub sample_percentage: Option<f64>,
    /// Optional response compression codec. Default: unspecified (no compression).
    #[serde(default)]
    pub compression_codec: CompressionCodec,
    /// Optional timestamp for time-travel queries (RFC3339 format).
    /// Example: "2024-01-15T00:00:00Z".
    #[serde(default)]
    pub snapshot_time: Option<String>,
    /// Optional maximum number of parallel read streams.
    /// Higher values can improve throughput for large tables.
    #[serde(default)]
    pub max_stream_count: Option<i32>,
    /// Optional preferred minimum number of streams (optimization hint).
    #[serde(default)]
    pub preferred_min_stream_count: Option<i32>,
    /// Data format for results. Default: arrow (preferred for columnar processing).
    #[serde(default)]
    pub data_format: DataFormat,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for StorageRead {}

impl StorageRead {
    /// Returns the project ID to use for creating the read session.
    /// Uses `job_project_id` if specified, otherwise falls back to `project_id`.
    pub fn get_job_project_id(&self) -> &str {
        self.job_project_id.as_deref().unwrap_or(&self.project_id)
    }
}

/// Errors that can occur during BigQuery Storage Read operations.
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
    #[error("BigQuery Storage Read client authentication error: {source}")]
    ClientAuth {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery Storage Read client creation error: {source}")]
    ClientCreation {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery Storage Read client connection error: {source}")]
    ClientConnection {
        #[source]
        source: gcloud_gax::conn::Error,
    },
    #[error("BigQuery Storage Read HTTP client build error: {source}")]
    HttpClientBuild {
        #[source]
        source: reqwest::Error,
    },
    #[error("Storage Read operation error: {source}")]
    StorageRead {
        #[source]
        source: google_cloud_bigquery::storage::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Invalid timestamp format in snapshot_time")]
    InvalidTimestamp,
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// Event handler for processing individual storage read events.
pub struct EventHandler {
    client: Arc<Client>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<StorageRead>,
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

            // Stream RecordBatches from the Storage Read API. Peek one batch ahead
            // so the final batch can carry completion_tx without buffering the
            // whole result set. If no batches are returned, emit an empty one so
            // downstream tasks still observe end-of-batch.
            let mut iterator = open_read_iterator(&self.client, &config).await?;
            let mut pending: Option<RecordBatch> = iterator
                .next()
                .await
                .map_err(|source| Error::StorageRead { source })?;

            if pending.is_none() {
                pending = Some(arrow::array::RecordBatch::new_empty(Arc::new(
                    arrow::datatypes::Schema::empty(),
                )));
            }

            loop {
                let batch = match pending.take() {
                    Some(b) => b,
                    None => break,
                };
                let next = iterator
                    .next()
                    .await
                    .map_err(|source| Error::StorageRead { source })?;
                let is_last = next.is_none();
                let num_rows = batch.num_rows();

                let mut result_event = EventBuilder::new()
                    .data(EventData::ArrowRecordBatch(batch))
                    .subject(format!("{}.{}", event.subject, config.name))
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|source| Error::EventBuilder { source })?;

                if is_last {
                    match self.tx {
                        None => {
                            if let Some(arc) = completion_tx_arc.as_ref() {
                                arc.signal_completion(result_event.data_as_json().ok());
                            }
                        }
                        Some(_) => {
                            result_event.completion_tx = completion_tx_arc.clone();
                        }
                    }
                }

                result_event
                    .send_with_logging(self.tx.as_ref())
                    .context("num_records", num_rows)
                    .await
                    .map_err(|source| Error::SendMessage { source })?;

                pending = next;
            }

            Ok(())
        })
        .await
    }
}

/// BigQuery Storage Read processor that reads tables using Storage Read API.
#[derive(Debug)]
pub struct Processor {
    /// Storage read configuration including credentials and table identifiers.
    config: Arc<StorageRead>,
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
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Loading credentials from file
    /// - Creating BigQuery client with authentication
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
                        error!(error = %e, "Failed to initialize storage read processor");
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

/// Builder for creating BigQuery Storage Read processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<StorageRead>>,
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

    pub fn config(mut self, config: Arc<StorageRead>) -> Self {
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

/// Opens a streaming `RecordBatchIterator` against the BigQuery Storage Read API.
/// Each `next()` returns one Arrow `RecordBatch` as it arrives from the wire,
/// so callers can emit downstream events without buffering the whole result set.
async fn open_read_iterator(
    client: &Client,
    config: &StorageRead,
) -> Result<google_cloud_bigquery::storage::RecordBatchIterator, Error> {
    let table_ref = TableReference {
        project_id: config.project_id.clone(),
        dataset_id: config.dataset_id.clone(),
        table_id: config.table_id.clone(),
    };

    let mut read_options = TableReadOptions::default();

    if let Some(ref fields) = config.selected_fields {
        read_options.selected_fields = fields.clone();
    }

    if let Some(ref restriction) = config.row_restriction {
        read_options.row_restriction = restriction.clone();
    }

    if let Some(sample_pct) = config.sample_percentage {
        read_options.sample_percentage = Some(sample_pct);
    }

    read_options.response_compression_codec = Some(match config.compression_codec {
        CompressionCodec::Unspecified => 0,
        CompressionCodec::Lz4 => 2,
    });

    let table_modifiers = if let Some(ref snapshot_time) = config.snapshot_time {
        let timestamp = chrono::DateTime::parse_from_rfc3339(snapshot_time)
            .map_err(|_| Error::InvalidTimestamp)?;

        Some(TableModifiers {
            snapshot_time: Some(prost_types::Timestamp {
                seconds: timestamp.timestamp(),
                nanos: timestamp.timestamp_subsec_nanos() as i32,
            }),
        })
    } else {
        None
    };

    let mut option = ReadTableOption::default();
    option = option.with_session_read_options(read_options);

    if let Some(modifiers) = table_modifiers {
        option = option.with_session_table_modifiers(modifiers);
    }

    if let Some(max_streams) = config.max_stream_count {
        option = option.with_max_stream_count(max_streams);
    }

    option = option.with_job_project_id(config.get_job_project_id().to_string());

    client
        .read_table_record_batches(&table_ref, Some(option))
        .await
        .map_err(|source| Error::StorageRead { source })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_timestamp() {
        let result = chrono::DateTime::parse_from_rfc3339("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_timestamp_parsing() {
        let result = chrono::DateTime::parse_from_rfc3339("2024-01-15T12:00:00Z");
        assert!(result.is_ok());
        let timestamp = result.unwrap();
        assert_eq!(timestamp.timestamp(), 1705320000);
    }

    #[test]
    fn test_compression_codec_values() {
        let unspecified = CompressionCodec::Unspecified;
        let lz4 = CompressionCodec::Lz4;

        let unspecified_value = match unspecified {
            CompressionCodec::Unspecified => 0,
            CompressionCodec::Lz4 => 2,
        };

        let lz4_value = match lz4 {
            CompressionCodec::Unspecified => 0,
            CompressionCodec::Lz4 => 2,
        };

        assert_eq!(unspecified_value, 0);
        assert_eq!(lz4_value, 2);
    }

    #[test]
    fn test_table_reference_creation() {
        use google_cloud_bigquery::http::table::TableReference;

        let table_ref = TableReference {
            project_id: "my-project".to_string(),
            dataset_id: "my-dataset".to_string(),
            table_id: "my-table".to_string(),
        };

        assert_eq!(table_ref.project_id, "my-project");
        assert_eq!(table_ref.dataset_id, "my-dataset");
        assert_eq!(table_ref.table_id, "my-table");
    }

    #[test]
    fn test_storage_read_get_job_project_id_with_override() {
        let storage_read = StorageRead {
            name: "cross_project_read".to_string(),
            credentials_path: Some(PathBuf::from("/creds.json")),
            project_id: "data-project".to_string(),
            job_project_id: Some("billing-project".to_string()),
            dataset_id: "warehouse".to_string(),
            table_id: "large_table".to_string(),
            ..Default::default()
        };

        assert_eq!(storage_read.get_job_project_id(), "billing-project");
    }

    #[test]
    fn test_storage_read_get_job_project_id_default() {
        let storage_read = StorageRead {
            name: "single_project_read".to_string(),
            credentials_path: Some(PathBuf::from("/creds.json")),
            project_id: "my-project".to_string(),
            job_project_id: None,
            dataset_id: "analytics".to_string(),
            table_id: "events".to_string(),
            ..Default::default()
        };

        assert_eq!(storage_read.get_job_project_id(), "my-project");
    }
}
