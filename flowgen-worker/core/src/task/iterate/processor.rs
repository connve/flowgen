//! Loop processor for iterating over JSON arrays.
//!
//! Processes events containing JSON arrays and emits individual events
//! for each array element, enabling fan-out processing patterns.

use crate::event::{generate_subject, Event, EventBuilder, EventData, SenderExt, SubjectSuffix};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{error, Instrument};

/// Default subject prefix for loop processor events.
const DEFAULT_MESSAGE_SUBJECT: &str = "loop";

/// Errors that can occur during loop processing operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to send event through broadcast channel.
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: tokio::sync::broadcast::error::SendError<Event>,
    },
    /// Event construction or processing failed.
    #[error(transparent)]
    Event(#[from] crate::event::Error),
    /// Required builder attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Expected array but got different type.
    #[error("Expected array at key '{key}', got: {got}")]
    ExpectedArray { key: String, got: String },
    /// Key not found in JSON object.
    #[error("Key '{}' not found in JSON object", _0)]
    KeyNotFound(String),
    /// Expected JSON event data but received ArrowRecordBatch.
    #[error("Expected JSON event data, got ArrowRecordBatch")]
    ExpectedJsonGotArrowRecordBatch,
    /// Expected JSON event data but received Avro.
    #[error("Expected JSON event data, got Avro")]
    ExpectedJsonGotAvro,
}

/// Handles individual event processing by iterating over JSON arrays.
pub struct EventHandler {
    /// Loop processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events.
    tx: Sender<Event>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event by iterating over a JSON array and emitting individual events.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        let json_data = match event.data {
            EventData::Json(data) => data,
            EventData::ArrowRecordBatch(_) => return Err(Error::ExpectedJsonGotArrowRecordBatch),
            EventData::Avro(_) => return Err(Error::ExpectedJsonGotAvro),
        };

        let array = match &self.config.iterate_key {
            Some(key) => {
                let value = json_data
                    .get(key)
                    .ok_or_else(|| Error::KeyNotFound(key.clone()))?;
                match value {
                    Value::Array(arr) => arr.clone(),
                    _ => {
                        return Err(Error::ExpectedArray {
                            key: key.clone(),
                            got: format!("{value:?}"),
                        })
                    }
                }
            }
            None => match json_data {
                Value::Array(arr) => arr,
                _ => {
                    return Err(Error::ExpectedArray {
                        key: "root".to_string(),
                        got: format!("{json_data:?}"),
                    })
                }
            },
        };

        for element in array {
            let subject = generate_subject(
                Some(&self.config.name),
                DEFAULT_MESSAGE_SUBJECT,
                SubjectSuffix::Timestamp,
            );

            let e = EventBuilder::new()
                .data(EventData::Json(element))
                .subject(subject)
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()?;

            self.tx
                .send_with_logging(e)
                .map_err(|e| Error::SendMessage { source: e })?;
        }

        Ok(())
    }
}

/// Loop processor that iterates over JSON arrays.
#[derive(Debug)]
pub struct Processor {
    /// Loop processor configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events.
    tx: Sender<Event>,
    /// Channel receiver for incoming events.
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

    /// Initializes the loop processor.
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.task_id))]
    async fn run(mut self) -> Result<(), Error> {
        let event_handler = match self.init().await {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!("{}", e);
                return Ok(());
            }
        };

        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    tokio::spawn(
                        async move {
                            if let Err(err) = event_handler.handle(event).await {
                                error!("{}", err);
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
    /// Loop processor configuration (required for build).
    config: Option<Arc<super::config::Processor>>,
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
    use serde_json::{json, Map};
    use tokio::sync::broadcast;

    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Loop Test".to_string()),
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
    fn test_processor_builder_new() {
        let builder = ProcessorBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert!(builder.task_context.is_none());
        assert_eq!(builder.task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_build_success() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
        });

        let (tx, _rx) = broadcast::channel(100);
        let rx2 = tx.subscribe();

        let loop_proc = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx2)
            .task_id(1)
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        assert_eq!(loop_proc.task_id, 1);
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
    async fn test_event_handler_iterate_root_array() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            task_id: 1,
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!([{"id": 1}, {"id": 2}, {"id": 3}])),
            subject: "input.subject".to_string(),
            task_id: Some(0),
            id: None,
            timestamp: 123456789,
            task_type: "test",
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let mut count = 0;
        while let Ok(output_event) = rx.recv().await {
            match output_event.data {
                EventData::Json(value) => {
                    assert!(value.get("id").is_some());
                    count += 1;
                }
                _ => panic!("Expected JSON data"),
            }
            if count == 3 {
                break;
            }
        }

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_event_handler_iterate_nested_array() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: Some("items".to_string()),
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            task_id: 1,
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({
                "items": [{"name": "a"}, {"name": "b"}],
                "total": 2
            })),
            subject: "input.subject".to_string(),
            task_id: Some(0),
            id: None,
            timestamp: 123456789,
            task_type: "test",
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let mut count = 0;
        while let Ok(output_event) = rx.recv().await {
            match output_event.data {
                EventData::Json(value) => {
                    assert!(value.get("name").is_some());
                    count += 1;
                }
                _ => panic!("Expected JSON data"),
            }
            if count == 2 {
                break;
            }
        }

        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_event_handler_key_not_found() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: Some("missing".to_string()),
        });

        let (tx, _rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            task_id: 1,
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"items": [1, 2, 3]})),
            subject: "input.subject".to_string(),
            task_id: Some(0),
            id: None,
            timestamp: 123456789,
            task_type: "test",
        };

        let result = event_handler.handle(input_event).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::KeyNotFound(_)));
    }

    #[tokio::test]
    async fn test_event_handler_expected_array_error() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
        });

        let (tx, _rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            task_id: 1,
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"not": "an array"})),
            subject: "input.subject".to_string(),
            task_id: Some(0),
            id: None,
            timestamp: 123456789,
            task_type: "test",
        };

        let result = event_handler.handle(input_event).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ExpectedArray { .. }));
    }
}
