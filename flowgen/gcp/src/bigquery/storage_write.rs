//! BigQuery Storage Write API processor for high-throughput streaming inserts.
//!
//! Provides direct streaming writes to BigQuery tables using the Storage Write API,
//! which offers lower latency and higher throughput compared to traditional load jobs.
//! Supports both default streams (for immediate availability) and committed streams
//! (for exactly-once semantics with deduplication).

use base64::Engine;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
};
use futures_util::StreamExt;
use gcloud_auth::credentials::CredentialsFile;
use google_cloud_bigquery::{
    client::{Client, ClientConfig},
    http::table::{TableFieldMode, TableFieldSchema, TableFieldType},
    storage_write::{stream::default::DefaultStream, AppendRowsRequestBuilder},
};
use prost_types::{field_descriptor_proto, DescriptorProto, FieldDescriptorProto};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during BigQuery Storage Write operations.
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
    #[error("Error converting event data: {source}")]
    EventData {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
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
    #[error("BigQuery storage write error: {source}")]
    StorageWrite {
        #[source]
        source: gcloud_gax::grpc::Status,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Failed to encode JSON to protobuf: {source}")]
    ProtoEncode {
        #[source]
        source: serde_json::Error,
    },
    #[error("Arrow conversion error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Table metadata retrieval error: {source}")]
    TableMetadata {
        #[source]
        source: google_cloud_bigquery::http::error::Error,
    },
    #[error("Table schema missing")]
    MissingSchema,
    #[error("Protobuf encoding error: {0}")]
    ProtobufEncode(String),
    #[error("Row write failed with errors")]
    RowWriteError,
}

impl From<flowgen_core::event::Error> for Error {
    fn from(source: flowgen_core::event::Error) -> Self {
        Error::EventData { source }
    }
}

/// Maps BigQuery table field type to protobuf field type.
fn bq_type_to_proto_type(bq_type: &TableFieldType) -> field_descriptor_proto::Type {
    match bq_type {
        TableFieldType::String
        | TableFieldType::Datetime
        | TableFieldType::Json
        | TableFieldType::Date
        | TableFieldType::Time
        | TableFieldType::Timestamp
        | TableFieldType::Numeric
        | TableFieldType::Bignumeric => field_descriptor_proto::Type::String,
        TableFieldType::Int64 | TableFieldType::Integer => field_descriptor_proto::Type::Int64,
        TableFieldType::Float64 | TableFieldType::Float => field_descriptor_proto::Type::Double,
        TableFieldType::Bool | TableFieldType::Boolean => field_descriptor_proto::Type::Bool,
        TableFieldType::Bytes => field_descriptor_proto::Type::Bytes,
        _ => field_descriptor_proto::Type::String,
    }
}

/// Builds a protobuf DescriptorProto from a BigQuery table schema.
/// Maps BigQuery field modes to protobuf labels (REPEATED for ARRAY fields, OPTIONAL otherwise).
fn build_proto_descriptor(
    fields: &[TableFieldSchema],
    include_change_type: bool,
) -> DescriptorProto {
    let mut proto_fields: Vec<FieldDescriptorProto> = fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let label = match field.mode {
                Some(TableFieldMode::Repeated) => {
                    Some(field_descriptor_proto::Label::Repeated.into())
                }
                _ => Some(field_descriptor_proto::Label::Optional.into()),
            };
            FieldDescriptorProto {
                name: Some(field.name.clone()),
                number: Some((i + 1) as i32),
                label,
                r#type: Some(bq_type_to_proto_type(&field.data_type).into()),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: None,
                options: None,
                proto3_optional: None,
            }
        })
        .collect();

    if include_change_type {
        proto_fields.push(FieldDescriptorProto {
            name: Some("_CHANGE_TYPE".to_string()),
            number: Some((fields.len() + 1) as i32),
            label: Some(field_descriptor_proto::Label::Optional.into()),
            r#type: Some(field_descriptor_proto::Type::String.into()),
            type_name: None,
            extendee: None,
            default_value: None,
            oneof_index: None,
            json_name: None,
            options: None,
            proto3_optional: None,
        });
    }

    DescriptorProto {
        name: Some("BqMessage".to_string()),
        field: proto_fields,
        extension: vec![],
        nested_type: vec![],
        enum_type: vec![],
        extension_range: vec![],
        oneof_decl: vec![],
        options: None,
        reserved_range: vec![],
        reserved_name: vec![],
    }
}

/// Encodes a single scalar JSON value as protobuf bytes for the given field tag and type.
fn encode_scalar_value(
    tag: u32,
    value: &JsonValue,
    proto_type: field_descriptor_proto::Type,
    buf: &mut Vec<u8>,
) {
    match proto_type {
        field_descriptor_proto::Type::String => {
            let s = match value {
                JsonValue::String(s) => s.clone(),
                _ => value.to_string(),
            };
            prost::encoding::string::encode(tag, &s, buf);
        }
        field_descriptor_proto::Type::Int64 => {
            if let Some(n) = value.as_i64() {
                prost::encoding::int64::encode(tag, &n, buf);
            }
        }
        field_descriptor_proto::Type::Double => {
            if let Some(n) = value.as_f64() {
                prost::encoding::double::encode(tag, &n, buf);
            }
        }
        field_descriptor_proto::Type::Bool => {
            if let Some(b) = value.as_bool() {
                prost::encoding::bool::encode(tag, &b, buf);
            }
        }
        field_descriptor_proto::Type::Bytes => {
            if let Some(s) = value.as_str() {
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(s) {
                    prost::encoding::bytes::encode(tag, &decoded, buf);
                }
            }
        }
        _ => {
            let s = match value {
                JsonValue::String(s) => s.clone(),
                _ => value.to_string(),
            };
            prost::encoding::string::encode(tag, &s, buf);
        }
    }
}

/// Encodes a JSON value as protobuf bytes according to the table schema.
/// Handles both scalar and repeated (ARRAY) fields.
fn json_to_proto_bytes(data: &JsonValue, fields: &[TableFieldSchema]) -> Result<Vec<u8>, Error> {
    let obj = data
        .as_object()
        .ok_or_else(|| Error::ProtobufEncode("Expected JSON object".to_string()))?;

    let mut buf = Vec::new();

    for (i, field) in fields.iter().enumerate() {
        let tag = (i + 1) as u32;
        let value = match obj.get(&field.name) {
            Some(v) if !v.is_null() => v,
            _ => continue, // Skip missing or null fields.
        };

        let proto_type = bq_type_to_proto_type(&field.data_type);
        let is_repeated = field.mode == Some(TableFieldMode::Repeated);

        if is_repeated {
            // Repeated fields encode each array element with the same tag.
            if let Some(arr) = value.as_array() {
                for elem in arr {
                    encode_scalar_value(tag, elem, proto_type, &mut buf);
                }
            }
        } else {
            encode_scalar_value(tag, value, proto_type, &mut buf);
        }
    }

    Ok(buf)
}

/// Event handler for processing individual storage write events.
pub struct EventHandler {
    stream: Arc<DefaultStream>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<super::config::StorageWrite>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    proto_descriptor: DescriptorProto,
    table_fields: Vec<TableFieldSchema>,
}

impl EventHandler {
    #[tracing::instrument(skip(self, event), name = "task.handle")]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config to support templates.
            let event_value = JsonValue::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Encode event data as protobuf rows.
            // Supports both JSON (single row) and Arrow RecordBatch (multiple rows).
            let proto_rows = match &event.data {
                EventData::ArrowRecordBatch(batch) => {
                    let mut rows = Vec::with_capacity(batch.num_rows());
                    let mut json_writer = arrow_json::ArrayWriter::new(Vec::new());
                    json_writer
                        .write(batch)
                        .map_err(|source| Error::Arrow { source })?;
                    json_writer
                        .finish()
                        .map_err(|source| Error::Arrow { source })?;
                    let json_bytes = json_writer.into_inner();
                    let json_rows: Vec<JsonValue> = serde_json::from_slice(&json_bytes)
                        .map_err(|source| Error::ProtoEncode { source })?;

                    for row in &json_rows {
                        let mut proto_bytes = json_to_proto_bytes(row, &self.table_fields)?;
                        if let Some(ref change_type) = config.change_type {
                            let tag = (self.table_fields.len() + 1) as u32;
                            prost::encoding::string::encode(
                                tag,
                                &change_type.to_string(),
                                &mut proto_bytes,
                            );
                        }
                        rows.push(proto_bytes);
                    }
                    rows
                }
                _ => {
                    let data = event.data_as_json()?;

                    // Support both single JSON objects and JSON arrays as input.
                    let json_rows: Vec<&JsonValue> = match &data {
                        JsonValue::Array(arr) => arr.iter().collect(),
                        _ => vec![&data],
                    };

                    let mut rows = Vec::with_capacity(json_rows.len());
                    for row in json_rows {
                        let mut proto_bytes =
                            json_to_proto_bytes(row, &self.table_fields)?;
                        if let Some(ref change_type) = config.change_type {
                            let tag = (self.table_fields.len() + 1) as u32;
                            prost::encoding::string::encode(
                                tag,
                                &change_type.to_string(),
                                &mut proto_bytes,
                            );
                        }
                        rows.push(proto_bytes);
                    }
                    rows
                }
            };

            // Create append request with proper proto descriptor.
            let builder = AppendRowsRequestBuilder::new(
                self.proto_descriptor.clone(),
                proto_rows,
            );

            // Append rows to the reusable default write stream.
            let mut responses = self
                .stream
                .append_rows(vec![builder])
                .await
                .map_err(|source| Error::StorageWrite { source })?;

            // Process append response.
            let mut rows_written = 0;
            let mut offset = None;

            while let Some(response_result) = responses.next().await {
                let response = response_result.map_err(|source| Error::StorageWrite { source })?;

                if !response.row_errors.is_empty() {
                    // Log each row error.
                    for row_error in &response.row_errors {
                        error!(
                            row_index = row_error.index,
                            error_code = ?row_error.code,
                            message = ?row_error.message,
                            "Row write failed"
                        );
                    }
                    return Err(Error::RowWriteError);
                }

                // Success - extract offset if available from the response oneof field.
                if let Some(resp) = response.response {
                    use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::Response;
                    match resp {
                        Response::AppendResult(append_result) => {
                            offset = append_result.offset;
                            rows_written += 1;
                        }
                        Response::Error(_) => {
                            // Error case is already captured in row_errors above
                        }
                    }
                }
            }

            // Build result event with write metadata.
            let result = serde_json::json!({
                "rows_written": rows_written,
                "offset": offset,
                "stream_name": config.stream_name.unwrap_or_else(|| format!(
                    "projects/{}/datasets/{}/tables/{}/streams/_default",
                    config.project_id,
                    config.dataset_id,
                    config.table_id
                ))
            });

            let event_builder = EventBuilder::new()
                .data(EventData::Json(result))
                .subject(format!("{}.{}", event.subject, config.name))
                .task_id(self.task_id)
                .task_type(self.task_type);

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
                .context("num_records", rows_written)
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

/// BigQuery Storage Write processor that streams data directly to BigQuery tables.
#[derive(Debug)]
pub struct Processor {
    /// Storage Write configuration including credentials and table details.
    config: Arc<super::config::StorageWrite>,
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

    /// Initializes the processor by establishing BigQuery client and default write stream.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Loading credentials from file
    /// - Creating BigQuery client with authentication
    /// - Creating/getting the default write stream for the target table
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let credentials = CredentialsFile::new_from_file(
            init_config.credentials_path.to_string_lossy().to_string(),
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

        // Fetch table schema to build proto descriptor for Storage Write API.
        let table_info = client
            .table()
            .get(
                &init_config.project_id,
                &init_config.dataset_id,
                &init_config.table_id,
            )
            .await
            .map_err(|source| Error::TableMetadata { source })?;

        let table_schema = table_info.schema.ok_or_else(|| Error::MissingSchema)?;
        let proto_descriptor =
            build_proto_descriptor(&table_schema.fields, init_config.change_type.is_some());
        let table_fields = table_schema.fields;

        // Build full table path for default write stream.
        // Default streams provide immediate data availability (suitable for webhooks).
        let table_path = format!(
            "projects/{}/datasets/{}/tables/{}",
            init_config.project_id, init_config.dataset_id, init_config.table_id
        );

        // Get the default storage writer and create the default write stream once.
        // The default stream is reused for all events for efficiency.
        let writer = client.default_storage_writer();
        let stream = Arc::new(
            writer
                .create_write_stream(&table_path)
                .await
                .map_err(|source| Error::StorageWrite { source })?,
        );

        let event_handler = EventHandler {
            stream,
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
            proto_descriptor,
            table_fields,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize storage write processor");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
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
                                        error!(error = %e, "Failed to write to storage");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(e) = result {
                                error!(error = %e, "Storage write failed after all retry attempts");
                                // Emit error event downstream for error handling.
                                let mut error_event = event.clone();
                                error_event.error = Some(e.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                None => return Ok(()),
            }
        }
    }
}

/// Builder for creating BigQuery Storage Write processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::StorageWrite>>,
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

    pub fn config(mut self, config: Arc<super::config::StorageWrite>) -> Self {
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
        let config = Arc::new(super::super::config::StorageWrite {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            dataset_id: "test-dataset".to_string(),
            table_id: "test-table".to_string(),
            stream_name: None,
            trace_id: None,
            change_type: None,
            depends_on: None,
            retry: None,
        });

        let result = ProcessorBuilder::new().config(config).build().await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_task_id() {
        let config = Arc::new(super::super::config::StorageWrite {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            dataset_id: "test-dataset".to_string(),
            table_id: "test-table".to_string(),
            stream_name: None,
            trace_id: None,
            change_type: None,
            depends_on: None,
            retry: None,
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
    async fn test_processor_builder_missing_task_context() {
        let config = Arc::new(super::super::config::StorageWrite {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            dataset_id: "test-dataset".to_string(),
            table_id: "test-table".to_string(),
            stream_name: None,
            trace_id: None,
            change_type: None,
            depends_on: None,
            retry: None,
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
        let config = Arc::new(super::super::config::StorageWrite {
            name: "test".to_string(),
            credentials_path: PathBuf::from("/test/creds.json"),
            project_id: "test-project".to_string(),
            dataset_id: "test-dataset".to_string(),
            table_id: "test-table".to_string(),
            stream_name: None,
            trace_id: None,
            change_type: None,
            depends_on: None,
            retry: None,
        });
        let (tx, rx) = mpsc::channel(10);
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        let task_context = Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test".to_string())
                .task_manager(task_manager)
                .cache(cache)
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
    fn test_error_types() {
        let err = Error::MissingBuilderAttribute("config".to_string());
        assert!(matches!(err, Error::MissingBuilderAttribute(_)));

        let err = Error::RowWriteError;
        assert!(matches!(err, Error::RowWriteError));
    }

    #[test]
    fn test_error_display_messages() {
        let err = Error::MissingBuilderAttribute("config".to_string());
        assert_eq!(
            err.to_string(),
            "Missing required builder attribute: config"
        );

        let err = Error::RowWriteError;
        assert_eq!(err.to_string(), "Row write failed with errors");
    }

    #[test]
    fn test_processor_builder_default() {
        let builder1 = ProcessorBuilder::new();
        let builder2 = ProcessorBuilder::default();

        assert!(builder1.config.is_none());
        assert!(builder2.config.is_none());
        assert!(builder1.rx.is_none());
        assert!(builder2.rx.is_none());
    }

    #[test]
    fn test_event_error_conversion() {
        let event_err =
            flowgen_core::event::Error::MissingBuilderAttribute("test error".to_string());
        let storage_err: Error = event_err.into();
        assert!(matches!(storage_err, Error::EventData { .. }));
    }
}
