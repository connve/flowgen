//! Event data format conversion processor.
//!
//! Processes events from the pipeline and converts their data between different formats
//! such as JSON to Avro with schema validation and key normalization.

use crate::event::{AvroData, Event, EventBuilder, EventData, SenderExt};
use serde_avro_fast::ser;
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    Mutex,
};
use tracing::{error, Instrument};

/// Errors that can occur during event conversion operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Sending event to channel failed with error: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error("Processor event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("ArrowRecordBatch to JSON conversion failed with error: {source}")]
    ArrowToJson {
        #[source]
        source: crate::event::Error,
    },
    #[error("Avro serialization failed with error: {source}")]
    SerdeAvro {
        #[source]
        source: serde_avro_fast::ser::SerError,
    },
    #[error("Avro deserialization failed with error: {source}")]
    SerdeAvroDe {
        #[source]
        source: serde_avro_fast::de::DeError,
    },
    #[error("Avro schema parsing failed with error: {source}")]
    SerdeSchema {
        #[source]
        source: serde_avro_fast::schema::SchemaError,
    },
    #[error(
        "ArrowRecordBatch to Avro conversion is not supported. Please convert data to JSON first."
    )]
    ArrowToAvroNotSupported,
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
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
    tx: Sender<Event>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Optional Avro serialization configuration.
    serializer: Option<Arc<AvroSerializerOptions>>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task context (unused but kept for consistency).
    _task_context: Arc<crate::task::context::TaskContext>,
}

/// Avro serialization configuration with schema and thread-safe serializer.
struct AvroSerializerOptions {
    /// Avro schema definition in JSON format.
    schema_string: String,
    /// Thread-safe Avro serializer configuration.
    serializer_config: Mutex<ser::SerializerConfig<'static>>,
}

impl EventHandler {
    /// Processes an event and converts to selected target format.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        let data = match event.data {
            EventData::Json(mut data) => match self.config.target_format {
                crate::task::convert::config::TargetFormat::Avro => match &self.serializer {
                    Some(serializer_opts) => {
                        transform_keys(&mut data);

                        let mut serializer_config = serializer_opts.serializer_config.lock().await;
                        let raw_bytes: Vec<u8> =
                            serde_avro_fast::to_datum_vec(&data, &mut serializer_config)
                                .map_err(|source| Error::SerdeAvro { source })?;

                        EventData::Avro(AvroData {
                            schema: serializer_opts.schema_string.clone(),
                            raw_bytes,
                        })
                    }
                    None => EventData::Json(data),
                },
                crate::task::convert::config::TargetFormat::Json => EventData::Json(data),
            },
            EventData::ArrowRecordBatch(ref _batch) => match self.config.target_format {
                crate::task::convert::config::TargetFormat::Json => {
                    let value = serde_json::Value::try_from(&event.data)
                        .map_err(|source| Error::ArrowToJson { source })?;
                    EventData::Json(value)
                }
                crate::task::convert::config::TargetFormat::Avro => {
                    return Err(Error::ArrowToAvroNotSupported)
                }
            },
            EventData::Avro(avro_data) => match self.config.target_format {
                crate::task::convert::config::TargetFormat::Json => {
                    // Parse the schema
                    let schema: serde_avro_fast::Schema = avro_data
                        .schema
                        .parse()
                        .map_err(|source| Error::SerdeSchema { source })?;

                    // Deserialize Avro bytes to JSON value
                    let json_value: Value =
                        serde_avro_fast::from_datum_slice(&avro_data.raw_bytes, &schema)
                            .map_err(|source| Error::SerdeAvroDe { source })?;

                    EventData::Json(json_value)
                }
                crate::task::convert::config::TargetFormat::Avro => {
                    // Avro to Avro passthrough
                    EventData::Avro(avro_data)
                }
            },
        };

        // Build and send event.
        let e = EventBuilder::new()
            .data(data)
            .subject(self.config.name.to_owned())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        self.tx
            .send_with_logging(e)
            .map_err(|source| Error::SendMessage { source })?;
        Ok(())
    }
}

/// Event format conversion processor that transforms data between formats.
#[derive(Debug)]
pub struct Processor {
    /// Conversion task configuration.
    config: Arc<crate::task::convert::config::Processor>,
    /// Channel sender for converted events.
    tx: Sender<Event>,
    /// Channel receiver for incoming events to convert.
    rx: Receiver<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<crate::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the processor by parsing and configuring the serializer.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Parsing Avro schema if converting to Avro format
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        let serializer = match self.config.target_format {
            crate::task::convert::config::TargetFormat::Avro => {
                let schema_string = self
                    .config
                    .as_ref()
                    .schema
                    .clone()
                    .ok_or_else(|| Error::MissingRequiredAttribute("schema".to_string()))?;

                let schema: serde_avro_fast::Schema = schema_string
                    .parse()
                    .map_err(|source| Error::SerdeSchema { source })?;

                // Leak the schema to get a 'static reference.
                // This is intentional and safe in this context since the schema
                // is effectively program-lifetime data.
                let leaked_schema: &'static serde_avro_fast::Schema = Box::leak(Box::new(schema));

                let serializer_config = ser::SerializerConfig::new(leaked_schema);

                Some(Arc::new(AvroSerializerOptions {
                    schema_string,
                    serializer_config: Mutex::new(serializer_config),
                }))
            }
            _ => None,
        };

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            serializer,
            task_type: self.task_type,
            _task_context: Arc::clone(&self._task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

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
                                event_handler.handle(event.clone()).await
                            })
                            .await;

                            if let Err(err) = result {
                                error!(
                                    "{}",
                                    Error::RetryExhausted {
                                        source: Box::new(err)
                                    }
                                );
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Processor configuration (required for build).
    config: Option<Arc<crate::task::convert::config::Processor>>,
    /// Event broadcast sender (required for build).
    tx: Option<Sender<Event>>,
    /// Event broadcast receiver (required for build).
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
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            task_id: self.task_id,
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::broadcast;

    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(crate::task::manager::TaskManagerBuilder::new().build());
        Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
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
            schema: Some(r#"{"type": "string"}"#.to_string()),
            retry: None,
        });
        let (tx, rx) = broadcast::channel(100);

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
        let (tx2, rx2) = broadcast::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx2)
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingRequiredAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_event_handler_json_passthrough() {
        let config = Arc::new(crate::task::convert::config::Processor {
            name: "test".to_string(),
            target_format: crate::task::convert::config::TargetFormat::Avro,
            schema: None,
            retry: None,
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            task_id: 1,
            serializer: None,
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"test": "value"})),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
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
            retry: None,
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            task_id: 1,
            serializer: None,
            task_type: "test",
            _task_context: create_mock_task_context(),
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
            retry: None,
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            task_id: 1,
            serializer: None,
            task_type: "test",
            _task_context: create_mock_task_context(),
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
