//! Event data format conversion processor.
//!
//! Processes events from the pipeline and converts their data between different formats
//! such as JSON to Avro with schema validation and key normalization.

use crate::event::{
    generate_subject, AvroData, Event, EventBuilder, EventData, SubjectSuffix, DEFAULT_LOG_MESSAGE,
};
use serde_avro_fast::ser;
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    Mutex,
};
use tracing::{debug, error, info, Instrument};

/// Default subject prefix for converted events.
const DEFAULT_MESSAGE_SUBJECT: &str = "convert";

/// Errors that can occur during event conversion operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to send converted event through broadcast channel.
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    /// Event construction or processing failed.
    #[error(transparent)]
    Event(#[from] crate::event::Error),
    /// Avro serialization failed.
    #[error(transparent)]
    SerdeAvro(#[from] serde_avro_fast::ser::SerError),
    /// Avro schema parsing failed.
    #[error(transparent)]
    SerdeSchema(#[from] serde_avro_fast::schema::SchemaError),
    /// Required builder attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Host coordination error.
    #[error("Host coordination error")]
    Host(#[source] crate::host::Error),
    /// Task manager error.
    #[error(transparent)]
    TaskManager(#[from] crate::task::manager::Error),
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
struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events.
    tx: Sender<Event>,
    /// Task identifier for event tracking.
    current_task_id: usize,
    /// Optional Avro serialization configuration.
    serializer: Option<Arc<AvroSerializerOptions>>,
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
        let data = match event.data {
            EventData::Json(mut data) => match &self.serializer {
                Some(serializer_opts) => {
                    transform_keys(&mut data);

                    let mut serializer_config = serializer_opts.serializer_config.lock().await;
                    let raw_bytes: Vec<u8> =
                        serde_avro_fast::to_datum_vec(&data, &mut serializer_config)?;

                    EventData::Avro(AvroData {
                        schema: serializer_opts.schema_string.clone(),
                        raw_bytes,
                    })
                }
                None => EventData::Json(data),
            },
            // Conversion to other types are currently not supported and not configurable.
            _ => todo!(),
        };

        // Generate event subject.
        let subject = generate_subject(
            Some(&self.config.name),
            DEFAULT_MESSAGE_SUBJECT,
            SubjectSuffix::Timestamp,
        );

        // Build and send event.
        let e = EventBuilder::new()
            .data(data)
            .subject(subject)
            .current_task_id(self.current_task_id)
            .build()?;

        info!("{}: {}", DEFAULT_LOG_MESSAGE, e.subject);
        self.tx.send(e)?;
        Ok(())
    }
}

/// Event format conversion processor that transforms data between formats.
#[derive(Debug)]
pub struct Processor {
    /// Conversion task configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for converted events.
    tx: Sender<Event>,
    /// Channel receiver for incoming events to convert.
    rx: Receiver<Event>,
    /// Current task identifier for event filtering.
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
}

impl crate::task::runner::Runner for Processor {
    type Error = Error;

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.current_task_id))]
    async fn run(mut self) -> Result<(), Error> {
        // Register task with task manager.
        let task_id = format!(
            "{}.{}.{}",
            self.task_context.flow.name, DEFAULT_MESSAGE_SUBJECT, self.config.name
        );
        let mut task_manager_rx = self
            .task_context
            .task_manager
            .register(
                task_id,
                Some(crate::task::manager::LeaderElectionOptions {}),
            )
            .await?;

        let serializer = match self.config.target_format {
            super::config::TargetFormat::Avro => {
                let schema_string = self
                    .config
                    .as_ref()
                    .schema
                    .clone()
                    .ok_or_else(|| Error::MissingRequiredAttribute("schema".to_string()))?;

                let schema: serde_avro_fast::Schema = schema_string.parse()?;

                // Leak the schema to get a 'static reference
                // This is intentional and safe in this context since the schema
                // is effectively program-lifetime data.
                let leaked_schema: &'static serde_avro_fast::Schema = Box::leak(Box::new(schema));

                let serializer_config = ser::SerializerConfig::new(leaked_schema);

                Some(Arc::new(AvroSerializerOptions {
                    schema_string,
                    serializer_config: Mutex::new(serializer_config),
                }))
            }
        };

        let event_handler = Arc::new(EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            current_task_id: self.current_task_id,
            serializer,
        });

        loop {
            tokio::select! {
                biased;

                // Check for leadership changes.
                Some(status) = task_manager_rx.recv() => {
                    if status == crate::task::manager::LeaderElectionResult::NotLeader {
                        debug!("Lost leadership for task: {}", self.config.name);
                        return Ok(());
                    }
                }

                // Process events.
                result = self.rx.recv() => {
                    match result {
                        Ok(event) => {
                            if event.current_task_id == Some(self.current_task_id - 1) {
                                let handler = Arc::clone(&event_handler);
                                tokio::spawn(async move {
                                    if let Err(err) = handler.handle(event).await {
                                        error!("{}", err);
                                    }
                                }.instrument(tracing::Span::current()));
                            }
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Processor configuration (required for build).
    config: Option<Arc<super::config::Processor>>,
    /// Event broadcast sender (required for build).
    tx: Option<Sender<Event>>,
    /// Event broadcast receiver (required for build).
    rx: Option<Receiver<Event>>,
    /// Current task identifier for event filtering.
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<crate::task::context::TaskContext>>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
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

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
        self.task_context = Some(task_context);
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
            current_task_id: self.current_task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::broadcast;

    /// Creates a mock TaskContext for testing.
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

    #[test]
    fn test_processor_builder_new() {
        let builder = ProcessorBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert!(builder.task_context.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_build_success() {
        let config = Arc::new(crate::convert::config::Processor {
            name: "test".to_string(),
            target_format: crate::convert::config::TargetFormat::Avro,
            schema: Some(r#"{"type": "string"}"#.to_string()),
        });

        let (tx, _rx) = broadcast::channel(100);
        let rx2 = tx.subscribe();

        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx2)
            .current_task_id(1)
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        assert_eq!(processor.current_task_id, 1);
    }

    #[tokio::test]
    async fn test_processor_builder_missing_config() {
        let (tx, rx) = broadcast::channel(100);

        let result = ProcessorBuilder::new()
            .sender(tx)
            .receiver(rx)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: config"));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_sender() {
        let config = Arc::new(crate::convert::config::Processor::default());
        let (_, rx) = broadcast::channel(100);

        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: sender"));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_receiver() {
        let config = Arc::new(crate::convert::config::Processor::default());
        let (tx, _) = broadcast::channel(100);

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: receiver"));
    }

    #[tokio::test]
    async fn test_processor_builder_missing_task_context() {
        let config = Arc::new(crate::convert::config::Processor::default());
        let (tx, rx) = broadcast::channel(100);

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .build()
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: task_context"));
    }

    #[test]
    fn test_avro_serializer_options_creation() {
        let options = AvroSerializerOptions {
            schema_string: r#"{"type": "string"}"#.to_string(),
            serializer_config: Mutex::new(serde_avro_fast::ser::SerializerConfig::new(Box::leak(
                Box::new(r#"{"type": "string"}"#.parse().unwrap()),
            ))),
        };

        assert_eq!(options.schema_string, r#"{"type": "string"}"#);
    }

    #[tokio::test]
    async fn test_event_handler_json_passthrough() {
        let config = Arc::new(crate::convert::config::Processor {
            name: "test".to_string(),
            target_format: crate::convert::config::TargetFormat::Avro,
            schema: None,
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            current_task_id: 1,
            serializer: None,
        };

        let input_event = Event {
            data: EventData::Json(json!({"test": "value"})),
            subject: "input.subject".to_string(),
            current_task_id: Some(0),
            id: None,
            timestamp: 123456789,
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
        assert!(output_event.subject.starts_with("test."));
        assert_eq!(output_event.current_task_id, Some(1));
    }
}
