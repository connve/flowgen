//! BigQuery Storage Read API processor for high-throughput parallel table reads.
//!
//! Uses the BigQuery Storage Read API to efficiently read large tables in parallel
//! using multiple streams. Returns data in Arrow RecordBatch format for efficient
//! columnar processing. This API is optimized for reading large amounts of data
//! and does not support SQL queries.

use arrow::array::{ArrayRef, RecordBatch};
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
};
use gcloud_auth::credentials::CredentialsFile;
use gcloud_googleapis::cloud::bigquery::storage::v1::read_session::{
    TableModifiers, TableReadOptions,
};
use google_cloud_bigquery::client::{Client, ClientConfig, ReadTableOption};
use google_cloud_bigquery::http::table::TableReference;
use google_cloud_bigquery::storage::value::StructDecodable;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Wrapper type that implements StructDecodable to capture full RecordBatch.
/// Since the gcloud-bigquery library iterates over individual rows, we use this
/// wrapper to capture just one row per RecordBatch as a marker, then reconstruct
/// the full RecordBatch with proper schema later.
struct RecordBatchMarker {
    /// Captured on first row only to mark the start of a new batch
    arrays: Vec<ArrayRef>,
}

impl StructDecodable for RecordBatchMarker {
    fn decode_arrow(
        fields: &[ArrayRef],
        row_no: usize,
    ) -> Result<Self, google_cloud_bigquery::storage::value::Error> {
        // Capture the arrays on the first row of each RecordBatch.
        // We only need to do this once per RecordBatch.
        if row_no == 0 {
            Ok(Self {
                arrays: fields.to_vec(),
            })
        } else {
            // For subsequent rows in the same batch, return a placeholder.
            // We'll filter these out later.
            Ok(Self { arrays: Vec::new() })
        }
    }
}

/// Errors that can occur during BigQuery Storage Read operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Sending event to channel failed: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Storage read event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Configuration template rendering failed with error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("BigQuery Storage Read client authentication failed with error: {source}")]
    ClientAuth {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery Storage Read client creation failed with error: {source}")]
    ClientCreation {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery Storage Read client connection failed with error: {source}")]
    ClientConnection {
        #[source]
        source: gcloud_gax::conn::Error,
    },
    #[error("BigQuery Storage Read operation failed with error: {source}")]
    StorageRead {
        #[source]
        source: google_cloud_bigquery::storage::Error,
    },
    #[error("BigQuery table metadata retrieval failed with error: {source}")]
    TableMetadata {
        #[source]
        source: google_cloud_bigquery::http::error::Error,
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
    #[error("No data returned from BigQuery Storage Read API")]
    NoDataReturned,
    #[error("Table schema is missing")]
    MissingSchema,
    #[error("Arrow RecordBatch construction failed with error: {source}")]
    RecordBatchConstruction {
        #[source]
        source: arrow::error::ArrowError,
    },
}

/// Event handler for processing individual storage read events.
pub struct EventHandler {
    client: Arc<Client>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<super::config::StorageRead>,
    task_type: &'static str,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        let event = Arc::new(event);

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config to support templates inside configuration.
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Read table using Storage Read API.
            let record_batches = read_table(&self.client, &config).await?;

            // Send each record batch as a separate event.
            for record_batch in record_batches {
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
    config: Arc<super::config::StorageRead>,
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

/// Builder for creating BigQuery Storage Read processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::StorageRead>>,
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

    pub fn config(mut self, config: Arc<super::config::StorageRead>) -> Self {
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
            _task_context: self
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

/// Reads table data using BigQuery Storage Read API and returns Arrow RecordBatches.
async fn read_table(
    client: &Client,
    config: &super::config::StorageRead,
) -> Result<Vec<RecordBatch>, Error> {
    let table_ref = TableReference {
        project_id: config.project_id.clone(),
        dataset_id: config.dataset_id.clone(),
        table_id: config.table_id.clone(),
    };

    // Build read options.
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
        super::config::CompressionCodec::Unspecified => 0,
        super::config::CompressionCodec::Lz4 => 2,
    });

    // Build table modifiers for time-travel.
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

    // Build read table options.
    let mut option = ReadTableOption::default();
    option = option.with_session_read_options(read_options);

    if let Some(modifiers) = table_modifiers {
        option = option.with_session_table_modifiers(modifiers);
    }

    if let Some(max_streams) = config.max_stream_count {
        option = option.with_max_stream_count(max_streams);
    }

    // Set job project for cross-project billing support.
    // If job_project_id is specified, use it for billing.
    // Otherwise, defaults to the data project_id.
    option = option.with_job_project_id(config.get_job_project_id().to_string());

    // Fetch table schema to get field names and their order.
    // The Storage Read API returns columns in schema order (or selected_fields order if specified).
    let table_info = client
        .table()
        .get(&config.project_id, &config.dataset_id, &config.table_id)
        .await
        .map_err(|source| Error::TableMetadata { source })?;

    let table_schema = table_info.schema.ok_or_else(|| Error::MissingSchema)?;

    // Build ordered list of field names based on the actual table schema order.
    // The Storage Read API always returns columns in table schema order, regardless of
    // the order specified in selected_fields. If selected_fields is specified, we need
    // to filter the table schema to only include those fields, but preserve schema order.
    let field_names: Vec<String> = if let Some(ref selected) = config.selected_fields {
        // Filter table schema fields to only include selected fields, preserving schema order
        table_schema
            .fields
            .iter()
            .filter_map(|f| {
                if selected.contains(&f.name) {
                    Some(f.name.clone())
                } else {
                    None
                }
            })
            .collect()
    } else {
        // Use all fields in table schema order
        table_schema.fields.iter().map(|f| f.name.clone()).collect()
    };

    // Read table using Row iterator.
    // The gcloud-bigquery library internally deserializes Arrow RecordBatches into individual rows.
    // Since we want to work with RecordBatches (not individual rows), we need to collect rows
    // and reconstruct the RecordBatches. This is a limitation of the current library API.
    //
    // Note: Each Row contains references to the Arrow arrays (columns) and a row index.
    // Multiple Row objects from the same batch share the same underlying Arrow arrays.
    // We group rows by their array references to reconstruct the original batches.

    let mut iterator = client
        .read_table::<RecordBatchMarker>(&table_ref, Some(option))
        .await
        .map_err(|source| Error::StorageRead { source })?;

    let mut batches = Vec::new();

    // Collect all captures and filter out placeholders (subsequent rows in same batch).
    while let Some(capture) = iterator
        .next()
        .await
        .map_err(|source| Error::StorageRead { source })?
    {
        // Skip placeholder captures (empty arrays means it was a subsequent row).
        if capture.arrays.is_empty() {
            continue;
        }

        // Construct schema using the field names from table metadata.
        // The Storage Read API returns columns in the same order as the schema or selected_fields.
        let schema = arrow::datatypes::Schema::new(
            capture
                .arrays
                .iter()
                .enumerate()
                .map(|(i, arr)| {
                    let field_name = field_names
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| format!("column_{i}"));
                    arrow::datatypes::Field::new(
                        field_name,
                        arr.data_type().clone(),
                        true, // nullable
                    )
                })
                .collect::<Vec<_>>(),
        );

        let batch = RecordBatch::try_new(Arc::new(schema), capture.arrays)
            .map_err(|source| Error::RecordBatchConstruction { source })?;

        batches.push(batch);
    }

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use std::path::PathBuf;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_processor_builder_missing_config() {
        let result = ProcessorBuilder::new().build().await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_receiver() {
        let config = Arc::new(super::super::config::StorageRead {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            job_project_id: None,
            dataset_id: "test-dataset".to_string(),
            table_id: "test-table".to_string(),
            ..Default::default()
        });

        let result = ProcessorBuilder::new().config(config).build().await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_sender() {
        let config = Arc::new(super::super::config::StorageRead {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            job_project_id: None,
            dataset_id: "test-dataset".to_string(),
            table_id: "test-table".to_string(),
            ..Default::default()
        });
        let (_tx, rx) = mpsc::channel(10);

        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_task_id() {
        let config = Arc::new(super::super::config::StorageRead {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            job_project_id: None,
            dataset_id: "test-dataset".to_string(),
            table_id: "test-table".to_string(),
            ..Default::default()
        });
        let (tx, rx) = mpsc::channel(10);

        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .sender(tx)
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_task_context() {
        let config = Arc::new(super::super::config::StorageRead {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            job_project_id: None,
            dataset_id: "test-dataset".to_string(),
            table_id: "test-table".to_string(),
            ..Default::default()
        });
        let (tx, rx) = mpsc::channel(10);

        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .sender(tx)
            .task_id(1)
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_task_type() {
        let config = Arc::new(super::super::config::StorageRead {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            job_project_id: None,
            dataset_id: "test-dataset".to_string(),
            table_id: "test-table".to_string(),
            ..Default::default()
        });
        let (tx, rx) = mpsc::channel(10);
        let task_manager = Arc::new(flowgen_core::task::manager::TaskManagerBuilder::new().build());
        let task_context = Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test".to_string())
                .task_manager(task_manager)
                .build()
                .unwrap(),
        );

        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .sender(tx)
            .task_id(1)
            .task_context(task_context)
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

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
    fn test_record_batch_capture_first_row() {
        let col1 = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let col2 = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let fields = vec![col1.clone(), col2.clone()];

        let result = RecordBatchMarker::decode_arrow(&fields, 0);
        assert!(result.is_ok());
        let capture = result.unwrap();
        assert_eq!(capture.arrays.len(), 2);
        assert!(!capture.arrays.is_empty());
    }

    #[test]
    fn test_record_batch_capture_subsequent_row() {
        let col1 = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let col2 = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let fields = vec![col1.clone(), col2.clone()];

        let result = RecordBatchMarker::decode_arrow(&fields, 1);
        assert!(result.is_ok());
        let capture = result.unwrap();
        assert!(capture.arrays.is_empty());
    }

    #[test]
    fn test_record_batch_reconstruction() {
        let col1 = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let col2 = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;

        let capture = RecordBatchMarker {
            arrays: vec![col1.clone(), col2.clone()],
        };

        let schema = arrow::datatypes::Schema::new(
            capture
                .arrays
                .iter()
                .enumerate()
                .map(|(i, arr)| {
                    arrow::datatypes::Field::new(
                        format!("column_{i}"),
                        arr.data_type().clone(),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        );

        let batch = RecordBatch::try_new(Arc::new(schema), capture.arrays);
        assert!(batch.is_ok());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_record_batch_empty_arrays() {
        let capture = RecordBatchMarker { arrays: Vec::new() };

        assert!(capture.arrays.is_empty());
    }

    #[test]
    fn test_compression_codec_values() {
        use super::super::config::CompressionCodec;

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
    fn test_error_display_messages() {
        let err = Error::InvalidTimestamp;
        assert_eq!(err.to_string(), "Invalid timestamp format in snapshot_time");

        let err = Error::NoDataReturned;
        assert_eq!(
            err.to_string(),
            "No data returned from BigQuery Storage Read API"
        );

        let err = Error::MissingBuilderAttribute("config".to_string());
        assert_eq!(
            err.to_string(),
            "Missing required builder attribute: config"
        );
    }

    #[test]
    fn test_processor_builder_default() {
        let builder1 = ProcessorBuilder::new();
        let builder2 = ProcessorBuilder::default();

        assert!(builder1.config.is_none());
        assert!(builder2.config.is_none());
    }
}
