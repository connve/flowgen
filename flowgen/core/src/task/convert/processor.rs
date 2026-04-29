//! Event data format conversion processor.
//!
//! Processes events from the pipeline and converts their data between different formats
//! such as JSON to Avro with schema validation and key normalization.

use crate::event::{AvroData, Event, EventBuilder, EventData, EventExt};
use arrow::array::ArrayRef;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use serde_avro_fast::ser;
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};
use tracing::{error, Instrument};

/// Errors that can occur during event conversion operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: crate::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("ArrowRecordBatch to JSON conversion error: {source}")]
    ArrowToJson {
        #[source]
        source: crate::event::Error,
    },
    #[error("Avro serialization error: {source}")]
    SerdeAvro {
        #[source]
        source: serde_avro_fast::ser::SerError,
    },
    #[error("Avro deserialization error: {source}")]
    SerdeAvroDe {
        #[source]
        source: serde_avro_fast::de::DeError,
    },
    #[error("Avro schema parsing error: {source}")]
    SerdeSchema {
        #[source]
        source: serde_avro_fast::schema::SchemaError,
    },
    #[error(
        "ArrowRecordBatch to Avro conversion is not supported. Please convert data to JSON first."
    )]
    ArrowToAvroNotSupported,
    #[error("Arrow schema casting error: {source}")]
    ArrowCast {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Arrow schema parsing error: {source}")]
    ArrowSchema {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Schema is required for Avro target format")]
    MissingAvroSchema,
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Error loading resource: {source}")]
    ResourceLoad {
        #[source]
        source: crate::resource::Error,
    },
}

/// Transforms JSON object keys by replacing hyphens with underscores.
/// Required for Avro compatibility as Avro field names cannot contain hyphens.
fn transform_keys(value: &mut Value) {
    if let Value::Object(map) = value {
        let mut new_map = Map::new();
        let keys_to_rename: Vec<String> = map.keys().filter(|k| k.contains("-")).cloned().collect();

        for old_key in keys_to_rename {
            if let Some(val) = map.remove(&old_key) {
                let new_key = old_key.replace("-", "_");
                new_map.insert(new_key, val);
            }
        }

        for (key, val) in new_map {
            map.insert(key, val);
        }
    }
}

/// Handles individual event conversion operations.
pub struct EventHandler {
    /// Processor configuration settings.
    config: Arc<crate::task::convert::config::Processor>,
    /// Channel sender for processed events.
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Schema configuration for target format.
    schema_config: SchemaConfig,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
}

/// Avro serialization configuration with schema and thread-safe serializer.
struct AvroSerializerOptions {
    /// Avro schema definition in JSON format.
    schema_string: String,
    /// Thread-safe Avro serializer configuration.
    serializer_config: Mutex<ser::SerializerConfig<'static>>,
}

/// Schema configuration for different target formats.
enum SchemaConfig {
    /// Avro serialization configuration.
    Avro(Arc<AvroSerializerOptions>),
    /// Arrow schema for RecordBatch casting.
    Arrow(Arc<Schema>),
    /// No schema configuration.
    None,
}

impl EventHandler {
    /// Processes an event and converts to selected target format.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();
        crate::event::with_event_context(&Arc::clone(&event), async move {
            let event_builder = EventBuilder::new();
            let data = match &event.data {
                EventData::Json(data) => match self.config.target_format {
                    crate::task::convert::config::TargetFormat::Avro => match &self.schema_config {
                        SchemaConfig::Avro(serializer_opts) => {
                            let mut data = data.clone();
                            transform_keys(&mut data);

                            let mut serializer_config =
                                serializer_opts.serializer_config.lock().await;
                            let raw_bytes: Vec<u8> =
                                serde_avro_fast::to_datum_vec(&data, &mut serializer_config)
                                    .map_err(|source| Error::SerdeAvro { source })?;

                            EventData::Avro(AvroData {
                                schema: serializer_opts.schema_string.clone(),
                                raw_bytes,
                            })
                        }
                        _ => EventData::Json(data.clone()),
                    },
                    crate::task::convert::config::TargetFormat::Json => {
                        EventData::Json(data.clone())
                    }
                    crate::task::convert::config::TargetFormat::Arrow => {
                        EventData::Json(data.clone())
                    }
                },
                EventData::ArrowRecordBatch(ref batch) => match self.config.target_format {
                    crate::task::convert::config::TargetFormat::Json => {
                        let value = event
                            .data_as_json()
                            .map_err(|source| Error::ArrowToJson { source })?;
                        EventData::Json(value)
                    }
                    crate::task::convert::config::TargetFormat::Avro => {
                        return Err(Error::ArrowToAvroNotSupported)
                    }
                    crate::task::convert::config::TargetFormat::Arrow => {
                        match &self.schema_config {
                            SchemaConfig::Arrow(target_schema) => {
                                // Handle empty batches (e.g., API returns only headers with no data rows)
                                // by creating an empty batch with the target schema.
                                if batch.num_columns() == 0 || batch.num_rows() == 0 {
                                    let empty_columns: Vec<ArrayRef> = target_schema
                                        .fields()
                                        .iter()
                                        .map(|field| {
                                            arrow::array::new_empty_array(field.data_type())
                                        })
                                        .collect();

                                    let empty_batch = RecordBatch::try_new(
                                        Arc::clone(target_schema),
                                        empty_columns,
                                    )
                                    .map_err(|source| Error::ArrowCast { source })?;

                                    EventData::ArrowRecordBatch(empty_batch)
                                } else {
                                    let casted_columns: Result<Vec<ArrayRef>, _> = target_schema
                                        .fields()
                                        .iter()
                                        .map(|target_field| {
                                            match batch.schema().index_of(target_field.name()) {
                                                Ok(index) => {
                                                    let source_column = batch.column(index);
                                                    compute::cast(
                                                        source_column,
                                                        target_field.data_type(),
                                                    )
                                                }
                                                Err(_) => Ok(arrow::array::new_null_array(
                                                    target_field.data_type(),
                                                    batch.num_rows(),
                                                )),
                                            }
                                        })
                                        .collect();

                                    let casted_batch = RecordBatch::try_new(
                                        Arc::clone(target_schema),
                                        casted_columns
                                            .map_err(|source| Error::ArrowCast { source })?,
                                    )
                                    .map_err(|source| Error::ArrowCast { source })?;

                                    EventData::ArrowRecordBatch(casted_batch)
                                }
                            }
                            _ => EventData::ArrowRecordBatch(batch.clone()),
                        }
                    }
                },
                EventData::Avro(avro_data) => match self.config.target_format {
                    crate::task::convert::config::TargetFormat::Json => {
                        let schema: serde_avro_fast::Schema = avro_data
                            .schema
                            .parse()
                            .map_err(|source| Error::SerdeSchema { source })?;

                        let json_value: Value =
                            serde_avro_fast::from_datum_slice(&avro_data.raw_bytes, &schema)
                                .map_err(|source| Error::SerdeAvroDe { source })?;

                        EventData::Json(json_value)
                    }
                    crate::task::convert::config::TargetFormat::Avro => {
                        EventData::Avro(avro_data.clone())
                    }
                    crate::task::convert::config::TargetFormat::Arrow => {
                        EventData::Avro(avro_data.clone())
                    }
                },
            };

            let mut e = event_builder
                .data(data)
                .subject(self.config.name.to_owned())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            // Signal completion or pass through to next task.
            match self.tx {
                None => {
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => {
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;
            Ok(())
        })
        .await
    }
}

/// Event format conversion processor that transforms data between formats.
#[derive(Debug)]
pub struct Processor {
    /// Conversion task configuration.
    config: Arc<crate::task::convert::config::Processor>,
    /// Channel sender for converted events.
    tx: Option<Sender<Event>>,
    /// Channel receiver for incoming events to convert.
    rx: Receiver<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the processor by parsing and configuring the schema.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Parsing Avro schema if converting to Avro format
    /// - Parsing Arrow schema if converting to Arrow format with schema casting
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        let schema_config = match self.config.target_format {
            crate::task::convert::config::TargetFormat::Avro => {
                let schema_string = match &self.config.schema {
                    Some(crate::resource::Source::Inline(schema)) => schema.clone(),
                    Some(crate::resource::Source::Resource { resource }) => {
                        let loader =
                            self.task_context.resource_loader.as_ref().ok_or_else(|| {
                                Error::ResourceLoad {
                                    source: crate::resource::Error::ResourcePathNotConfigured,
                                }
                            })?;
                        loader
                            .load(resource)
                            .await
                            .map_err(|source| Error::ResourceLoad { source })?
                    }
                    None => {
                        return Err(Error::MissingAvroSchema);
                    }
                };

                let schema: serde_avro_fast::Schema = schema_string
                    .parse()
                    .map_err(|source| Error::SerdeSchema { source })?;

                // Leak the schema to get a 'static reference.
                // This is intentional and safe in this context since the schema
                // is effectively program-lifetime data.
                let leaked_schema: &'static serde_avro_fast::Schema = Box::leak(Box::new(schema));

                let serializer_config = ser::SerializerConfig::new(leaked_schema);

                SchemaConfig::Avro(Arc::new(AvroSerializerOptions {
                    schema_string,
                    serializer_config: Mutex::new(serializer_config),
                }))
            }
            crate::task::convert::config::TargetFormat::Arrow => {
                match &self.config.schema {
                    Some(crate::resource::Source::Inline(schema_json)) => {
                        let schema: Schema =
                            serde_json::from_str(schema_json).map_err(|e| Error::ArrowSchema {
                                source: arrow::error::ArrowError::JsonError(e.to_string()),
                            })?;
                        SchemaConfig::Arrow(Arc::new(schema))
                    }
                    Some(crate::resource::Source::Resource { resource }) => {
                        let loader =
                            self.task_context.resource_loader.as_ref().ok_or_else(|| {
                                Error::ResourceLoad {
                                    source: crate::resource::Error::ResourcePathNotConfigured,
                                }
                            })?;
                        let schema_json = loader
                            .load(resource)
                            .await
                            .map_err(|source| Error::ResourceLoad { source })?;
                        let schema: Schema =
                            serde_json::from_str(&schema_json).map_err(|e| Error::ArrowSchema {
                                source: arrow::error::ArrowError::JsonError(e.to_string()),
                            })?;
                        SchemaConfig::Arrow(Arc::new(schema))
                    }
                    None => SchemaConfig::None, // No schema means passthrough
                }
            }
            _ => SchemaConfig::None,
        };

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            schema_config,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize convert processor");
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
                                        error!(error = %e, "Failed to convert event");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Convert failed after all retry attempts.");
                                // Emit error event downstream for error handling.
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
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

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Processor configuration (required for build).
    config: Option<Arc<crate::task::convert::config::Processor>>,
    /// Event sender for passing events to next task (optional if this is the last task).
    tx: Option<Sender<Event>>,
    /// Event receiver for incoming events (required for build).
    rx: Option<Receiver<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<crate::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<crate::task::convert::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
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
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::mpsc;

    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_transform_keys() {
        let mut value = json!({
            "normal-key": "value1",
            "another_key": "value2",
            "nested": {
                "inner-key": "nested_value"
            }
        });

        transform_keys(&mut value);

        assert_eq!(value["normal_key"], "value1");
        assert_eq!(value["another_key"], "value2");
        assert_eq!(value["nested"]["inner-key"], "nested_value");
    }

    #[test]
    fn test_transform_keys_no_hyphens() {
        let mut value = json!({
            "normal_key": "value1",
            "another_key": "value2"
        });

        let original = value.clone();
        transform_keys(&mut value);

        assert_eq!(value, original);
    }

    #[tokio::test]
    async fn test_processor_builder() {
        let config = Arc::new(crate::task::convert::config::Processor {
            name: "test".to_string(),
            target_format: crate::task::convert::config::TargetFormat::Avro,
            schema: Some(crate::resource::Source::Inline(
                r#"{"type": "string"}"#.to_string(),
            )),
            depends_on: None,
            retry: None,
        });
        let (tx, rx) = mpsc::channel(100);

        // Success case.
        let processor = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .receiver(rx)
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(processor.is_ok());

        // Error case - missing config.
        let (tx2, rx2) = mpsc::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx2)
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_event_handler_json_passthrough() {
        let config = Arc::new(crate::task::convert::config::Processor {
            name: "test".to_string(),
            target_format: crate::task::convert::config::TargetFormat::Avro,
            schema: None,
            depends_on: None,
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            schema_config: SchemaConfig::None,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"test": "value"})),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let output_event = rx.recv().await.unwrap();

        match output_event.data {
            EventData::Json(value) => {
                assert_eq!(value, json!({"test": "value"}));
            }
            _ => panic!("Expected JSON passthrough"),
        }
        assert_eq!(output_event.subject, "test");
        assert_eq!(output_event.task_id, 1);
    }

    #[tokio::test]
    async fn test_event_handler_avro_to_json() {
        let config = Arc::new(crate::task::convert::config::Processor {
            name: "test".to_string(),
            target_format: crate::task::convert::config::TargetFormat::Json,
            schema: None,
            depends_on: None,
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            schema_config: SchemaConfig::None,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        // Create a simple Avro schema and serialize test data
        let schema_str = r#"{"type": "string"}"#;
        let schema: serde_avro_fast::Schema = schema_str.parse().unwrap();
        let leaked_schema: &'static serde_avro_fast::Schema = Box::leak(Box::new(schema));
        let mut serializer_config = ser::SerializerConfig::new(leaked_schema);

        let test_data = json!("test_value");
        let raw_bytes = serde_avro_fast::to_datum_vec(&test_data, &mut serializer_config).unwrap();

        let input_event = Event {
            data: EventData::Avro(AvroData {
                schema: schema_str.to_string(),
                raw_bytes,
            }),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let output_event = rx.recv().await.unwrap();

        match output_event.data {
            EventData::Json(value) => {
                assert_eq!(value, "test_value");
            }
            _ => panic!("Expected JSON output from Avro conversion"),
        }
        assert_eq!(output_event.subject, "test");
        assert_eq!(output_event.task_id, 1);
    }

    #[tokio::test]
    async fn test_event_handler_avro_passthrough() {
        let config = Arc::new(crate::task::convert::config::Processor {
            name: "test".to_string(),
            target_format: crate::task::convert::config::TargetFormat::Avro,
            schema: None,
            depends_on: None,
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            schema_config: SchemaConfig::None,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let schema_str = r#"{"type": "string"}"#;
        let raw_bytes = vec![1, 2, 3, 4];

        let input_event = Event {
            data: EventData::Avro(AvroData {
                schema: schema_str.to_string(),
                raw_bytes: raw_bytes.clone(),
            }),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let output_event = rx.recv().await.unwrap();

        match output_event.data {
            EventData::Avro(avro_data) => {
                assert_eq!(avro_data.schema, schema_str);
                assert_eq!(avro_data.raw_bytes, raw_bytes);
            }
            _ => panic!("Expected Avro passthrough"),
        }
        assert_eq!(output_event.subject, "test");
        assert_eq!(output_event.task_id, 1);
    }
}
