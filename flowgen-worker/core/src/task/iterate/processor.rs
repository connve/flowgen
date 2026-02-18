//! Loop processor for iterating over JSON arrays.
//!
//! Processes events containing JSON arrays and emits individual events
//! for each array element, enabling fan-out processing patterns.

use crate::event::{Event, EventBuilder, EventData, EventExt};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during loop processing operations.
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
    #[error("Expected array at key '{key}', got: {got}")]
    ExpectedArray { key: String, got: String },
    #[error("Key '{}' not found in JSON object", _0)]
    KeyNotFound(String),
    #[error("Expected JSON event data, got ArrowRecordBatch")]
    ExpectedJsonGotArrowRecordBatch,
    #[error("Expected JSON event data, got Avro")]
    ExpectedJsonGotAvro,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Handles individual event processing by iterating over JSON arrays.
pub struct EventHandler {
    /// Loop processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events (optional if this is the last task).
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task context (unused but kept for consistency).
    _task_context: Arc<crate::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes an event by iterating over a JSON array and emitting individual events.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let event = Arc::new(event);
        crate::event::with_event_context(&Arc::clone(&event), async move {
            let json_data = match &event.data {
                EventData::Json(data) => data,
                EventData::ArrowRecordBatch(_) => {
                    return Err(Error::ExpectedJsonGotArrowRecordBatch)
                }
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
                    Value::Array(arr) => arr.clone(),
                    _ => {
                        return Err(Error::ExpectedArray {
                            key: "root".to_string(),
                            got: format!("{json_data:?}"),
                        })
                    }
                },
            };

            for element in array.iter() {
                // EventBuilder::new() automatically preserves meta from the current event context
                let e = EventBuilder::new()
                    .data(EventData::Json(element.clone()))
                    .subject(self.config.name.to_owned())
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|source| Error::EventBuilder { source })?;

                e.send_with_logging(self.tx.as_ref())
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }

            Ok(())
        })
        .await
    }
}

/// Loop processor that iterates over JSON arrays.
#[derive(Debug)]
pub struct Processor {
    /// Loop processor configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events (optional if this is the last task).
    tx: Option<Sender<Event>>,
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
            _task_context: Arc::clone(&self._task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize iterate processor");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(error = %e, "Iterate processor failed after all retry attempts");
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
                                        error!(error = %e, "Failed to iterate event");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Iterate failed after all retry attempts");
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
    /// Loop processor configuration (required for build).
    config: Option<Arc<super::config::Processor>>,
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
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            _task_context: self
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
    use serde_json::{json, Map};
    use tokio::sync::mpsc;

    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Loop Test".to_string()),
        );
        let task_manager = Arc::new(crate::task::manager::TaskManagerBuilder::new().build());
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

    #[tokio::test]
    async fn test_processor_builder() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
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
    async fn test_event_handler_iterate_root_array() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!([{"id": 1}, {"id": 2}, {"id": 3}])),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let mut count = 0;
        while let Some(output_event) = rx.recv().await {
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
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({
                "items": [{"name": "a"}, {"name": "b"}],
                "total": 2
            })),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let mut count = 0;
        while let Some(output_event) = rx.recv().await {
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
            retry: None,
        });

        let (tx, _rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"items": [1, 2, 3]})),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
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
            retry: None,
        });

        let (tx, _rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"not": "an array"})),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
        };

        let result = event_handler.handle(input_event).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ExpectedArray { .. }));
    }
}
