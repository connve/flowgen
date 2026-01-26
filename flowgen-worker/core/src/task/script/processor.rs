//! Script-based event transformation processor.
//!
//! Executes Rhai scripts to transform, filter, or manipulate event data.
//! Scripts can return objects, arrays, or null to control event emission.

use crate::event::{Event, EventBuilder, EventData, SenderExt};
use rhai::{Dynamic, Engine, Scope};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during script execution.
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
    #[error("Script execution failed with error: {source}")]
    ScriptExecution {
        #[source]
        source: Box<rhai::EvalAltResult>,
    },
    #[error("Event conversion failed with error: {source}")]
    EventConversion {
        #[source]
        source: crate::event::Error,
    },
    #[error("Script returned invalid type: {0}")]
    InvalidReturnType(String),
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Failed to parse RFC 2822 timestamp '{timestamp}': {source}")]
    ParseRFC2822Timestamp {
        timestamp: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("Failed to parse RFC 3339 timestamp '{timestamp}': {source}")]
    ParseRFC3339Timestamp {
        timestamp: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Handles individual script execution operations.
pub struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events.
    tx: Sender<Event>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Rhai script engine instance.
    engine: Engine,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task context (unused but kept for consistency).
    _task_context: Arc<crate::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes an event by executing the script on its data.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        // Store the original event for comparison after script execution.
        let original_event = event.clone();

        // Convert the event to JSON for script execution.
        let value = Value::try_from(&event).map_err(|source| Error::EventConversion { source })?;
        let event_obj = value["event"].to_owned();

        // Execute the script with the event in scope.
        let mut scope = Scope::new();
        scope.push("event", json_to_dynamic(&event_obj)?);

        let result: Dynamic = self
            .engine
            .eval_with_scope(&mut scope, &self.config.code)
            .map_err(|e| Error::ScriptExecution { source: e })?;

        // Convert the script result back to JSON.
        let result_json = dynamic_to_json(result)?;

        // Process the script result based on its type.
        match result_json {
            Value::Null => Ok(()),
            Value::Array(arr) => {
                // Emit multiple events, one per array element.
                for value in arr {
                    let new_event = self.generate_script_event(value, &original_event)?;
                    self.emit_event(new_event).await?;
                }
                Ok(())
            }
            value => {
                // Emit a single event.
                let new_event = self.generate_script_event(value, &original_event)?;
                self.emit_event(new_event).await
            }
        }
    }

    /// Generates a new event from the script result by comparing with the original event.
    ///
    /// Preserves the original data format (Avro, Arrow, or JSON) when the script has not
    /// modified the data content. This allows scripts to modify metadata fields like subject
    /// or id while maintaining efficient binary formats through the pipeline.
    fn generate_script_event(&self, result: Value, original_event: &Event) -> Result<Event, Error> {
        // Convert the original event data to JSON for comparison.
        let original_data_json = Value::try_from(&original_event.data)
            .map_err(|source| Error::EventConversion { source })?;

        let (subject, data, id, meta) = match result {
            Value::Object(ref obj) => {
                // Script returned an object, which may contain subject, data, id, or _meta.
                let subject = obj
                    .get("subject")
                    .and_then(|s| s.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| original_event.subject.clone());

                let data_json = obj.get("data").unwrap_or(&result);

                // Keep the original data format if the content has not changed.
                let data = if data_json == &original_data_json {
                    original_event.data.clone()
                } else if obj.contains_key("data") {
                    EventData::Json(data_json.clone())
                } else {
                    // No explicit data key, treat the whole object as data.
                    EventData::Json(result.clone())
                };

                let id = obj
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| original_event.id.clone());

                // Extract _meta if present in script output.
                // If _meta key exists and is an object, use it.
                // If _meta is null, clear metadata (set to None).
                // If _meta is not an object, preserve original metadata.
                // If _meta key is absent, preserve original metadata.
                let meta = if obj.contains_key("_meta") {
                    match obj.get("_meta") {
                        Some(Value::Object(m)) => Some(m.clone()),
                        Some(Value::Null) => None,
                        _ => original_event.meta.clone(), // Invalid type, preserve original.
                    }
                } else {
                    original_event.meta.clone()
                };

                (subject, data, id, meta)
            }
            value => {
                // Script returned a non-object value, treat it as data.
                // Keep the original data format if the content has not changed.
                let data = if value == original_data_json {
                    original_event.data.clone()
                } else {
                    EventData::Json(value)
                };

                (
                    original_event.subject.clone(),
                    data,
                    original_event.id.clone(),
                    original_event.meta.clone(),
                )
            }
        };

        // Build the new event with the processed fields.
        let mut builder = EventBuilder::new()
            .data(data)
            .subject(subject)
            .task_id(self.task_id)
            .task_type(self.task_type);

        if let Some(id) = id {
            builder = builder.id(id);
        }

        if let Some(meta) = meta {
            builder = builder.meta(meta);
        }

        builder
            .build()
            .map_err(|source| Error::EventBuilder { source })
    }

    /// Emits a single event to the broadcast channel.
    async fn emit_event(&self, event: Event) -> Result<(), Error> {
        self.tx
            .send_with_logging(event)
            .map_err(|source| Error::SendMessage { source })?;
        Ok(())
    }
}

/// Converts serde_json::Value to rhai::Dynamic.
fn json_to_dynamic(value: &Value) -> Result<Dynamic, Error> {
    let dynamic =
        rhai::serde::to_dynamic(value).map_err(|e| Error::InvalidReturnType(e.to_string()))?;
    Ok(dynamic)
}

/// Converts rhai::Dynamic to serde_json::Value.
fn dynamic_to_json(dynamic: Dynamic) -> Result<Value, Error> {
    let value =
        rhai::serde::from_dynamic(&dynamic).map_err(|e| Error::InvalidReturnType(e.to_string()))?;
    Ok(value)
}

/// Script processor that executes Rhai code on events.
#[derive(Debug)]
pub struct Processor {
    /// Script task configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for transformed events.
    tx: Sender<Event>,
    /// Channel receiver for incoming events to transform.
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

    /// Initializes the processor by setting up the Rhai engine.
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        let mut engine = Engine::new();

        // Register function to parse RFC 2822 timestamps to Unix milliseconds.
        engine.register_fn(
            "parse_rfc2822_timestamp",
            |timestamp_str: &str| -> Result<i64, Box<rhai::EvalAltResult>> {
                // Parse RFC 2822 format like "Mon, 5 Jan 2026 15:03:34 +0100".
                // Returns Unix timestamp in milliseconds.
                chrono::DateTime::parse_from_rfc2822(timestamp_str)
                    .map(|dt| dt.timestamp_millis())
                    .map_err(|source| {
                        let err = Error::ParseRFC2822Timestamp {
                            timestamp: timestamp_str.to_string(),
                            source,
                        };
                        err.to_string().into()
                    })
            },
        );

        // Register function to parse ISO 8601 timestamps to Unix milliseconds.
        // Uses RFC 3339 format (ISO 8601 profile for internet timestamps).
        engine.register_fn(
            "parse_timestamp",
            |timestamp_str: &str| -> Result<i64, Box<rhai::EvalAltResult>> {
                // Parse ISO 8601 timestamp format like "2026-01-07T11:16:13.869Z".
                // Returns Unix timestamp in milliseconds.
                chrono::DateTime::parse_from_rfc3339(timestamp_str)
                    .map(|dt| dt.timestamp_millis())
                    .map_err(|source| {
                        let err = Error::ParseRFC3339Timestamp {
                            timestamp: timestamp_str.to_string(),
                            source,
                        };
                        err.to_string().into()
                    })
            },
        );

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            engine,
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
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        error!("{}", e);
                                        Err(e)
                                    }
                                }
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
    use serde_json::{json, Map, Value};
    use tokio::sync::broadcast;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Script Test".to_string()),
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

    #[tokio::test]
    async fn test_processor_builder() {
        let config = Arc::new(crate::task::script::config::Processor {
            name: "test".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: "event".to_string(),
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
    async fn test_script_simple_transformation() {
        let config = Arc::new(crate::task::script::config::Processor {
            name: "test".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: r#"#{ original: event.data, transformed: true }"#.to_string(),
            retry: None,
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx: tx.clone(),
            task_id: 1,
            engine: Engine::new(),
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"x": 5})),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
        };

        // Drop the original tx so recv can complete
        drop(tx);

        event_handler.handle(input_event).await.unwrap();

        let output_event = rx.recv().await.unwrap();

        match output_event.data {
            EventData::Json(value) => {
                assert_eq!(value["original"]["x"], 5);
                assert_eq!(value["transformed"], true);
            }
            _ => panic!("Expected JSON output"),
        }
    }

    #[tokio::test]
    async fn test_script_filter_null() {
        let config = Arc::new(crate::task::script::config::Processor {
            name: "test".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: r#"if data.age < 18 { null } else { data }"#.to_string(),
            retry: None,
        });

        let (tx, mut rx) = broadcast::channel(100);
        let tx_clone = tx.clone();

        let event_handler = EventHandler {
            config,
            tx: tx_clone,
            task_id: 1,
            engine: Engine::new(),
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"age": 15})),
            subject: "input.subject".to_string(),
            task_type: "test",
            meta: None,
            task_id: 0,
            id: None,
            timestamp: 123456789,
        };

        tokio::spawn(async move {
            event_handler.handle(input_event).await.unwrap();
        });

        // Should not receive any event (filtered out)
        tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv())
            .await
            .expect_err("Should timeout, no event emitted");
    }

    #[tokio::test]
    async fn test_script_array_output() {
        let config = Arc::new(crate::task::script::config::Processor {
            name: "test".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: r#"[#{ id: 1 }, #{ id: 2 }, #{ id: 3 }]"#.to_string(),
            retry: None,
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            task_id: 1,
            engine: Engine::new(),
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({})),
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

        // Should receive 3 events
        let event1 = rx.recv().await.unwrap();
        let event2 = rx.recv().await.unwrap();
        let event3 = rx.recv().await.unwrap();

        match event1.data {
            EventData::Json(value) => assert_eq!(value["id"], 1),
            _ => panic!("Expected JSON output"),
        }
        match event2.data {
            EventData::Json(value) => assert_eq!(value["id"], 2),
            _ => panic!("Expected JSON output"),
        }
        match event3.data {
            EventData::Json(value) => assert_eq!(value["id"], 3),
            _ => panic!("Expected JSON output"),
        }
    }

    #[tokio::test]
    async fn test_event_handler_arrow_input() {
        let config = Arc::new(crate::task::script::config::Processor {
            name: "test".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: "event".to_string(),
            retry: None,
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            task_id: 1,
            engine: Engine::new(),
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        // Create an ArrowRecordBatch event
        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "test",
            arrow::datatypes::DataType::Int32,
            false,
        )]);
        let batch = arrow::array::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let input_event = Event {
            data: EventData::ArrowRecordBatch(batch),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
        };

        let result = event_handler.handle(input_event).await;
        assert!(result.is_ok());

        let output_event = rx.try_recv().unwrap();
        assert_eq!(output_event.subject, "input.subject");
        assert_eq!(output_event.task_id, 1);
    }
}
