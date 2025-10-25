//! Script-based event transformation processor.
//!
//! Executes Rhai scripts to transform, filter, or manipulate event data.
//! Scripts can return objects, arrays, or null to control event emission.

use crate::event::{generate_subject, Event, EventBuilder, EventData, SenderExt, SubjectSuffix};
use rhai::{Dynamic, Engine, Scope};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{error, Instrument};

/// Default subject prefix for script-transformed events.
const DEFAULT_MESSAGE_SUBJECT: &str = "script";

/// Errors that can occur during script execution.
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
    /// Script execution failed.
    #[error("Script execution failed: {source}")]
    ScriptExecution {
        #[source]
        source: Box<rhai::EvalAltResult>,
    },
    /// Script returned invalid type.
    #[error("Script returned invalid type: {0}")]
    InvalidReturnType(String),
    /// Required builder attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Expected JSON input but got different event type.
    #[error("Expected JSON input, got ArrowRecordBatch")]
    ExpectedJsonGotArrowRecordBatch,
    /// Expected JSON input but got Avro.
    #[error("Expected JSON input, got Avro")]
    ExpectedJsonGotAvro,
}

/// Handles individual script execution operations.
pub struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events.
    tx: Sender<Event>,
    /// Task identifier for event tracking.
    current_task_id: usize,
    /// Rhai script engine instance.
    engine: Engine,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes an event by executing the script on its data.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if event.current_task_id != self.current_task_id.checked_sub(1) {
            return Ok(());
        }

        // Extract JSON data from event
        let json_data = match event.data {
            EventData::Json(data) => data,
            EventData::ArrowRecordBatch(_) => return Err(Error::ExpectedJsonGotArrowRecordBatch),
            EventData::Avro(_) => return Err(Error::ExpectedJsonGotAvro),
        };

        // Convert JSON to Rhai Dynamic
        let mut scope = Scope::new();
        scope.push("data", json_to_dynamic(&json_data)?);

        // Execute script
        let result: Dynamic = self
            .engine
            .eval_with_scope(&mut scope, &self.config.code)
            .map_err(|e| Error::ScriptExecution { source: e })?;

        // Convert result back to JSON
        let result_json = dynamic_to_json(result)?;

        // Handle different result types
        match result_json {
            Value::Null => {
                // Filter out - don't emit event
                Ok(())
            }
            Value::Array(arr) => {
                // Emit multiple events, one per array element
                for element in arr {
                    self.emit_event(element).await?;
                }
                Ok(())
            }
            value => {
                // Emit single event
                self.emit_event(value).await
            }
        }
    }

    /// Emits a single event with the given data.
    async fn emit_event(&self, data: Value) -> Result<(), Error> {
        let subject = generate_subject(
            Some(&self.config.name),
            DEFAULT_MESSAGE_SUBJECT,
            SubjectSuffix::Timestamp,
        );

        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(subject)
            .current_task_id(self.current_task_id)
            .task_type(self.task_context.task_type)
            .build()?;

        self.tx
            .send_with_logging(e)
            .map_err(|e| Error::SendMessage { source: e })?;
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
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<crate::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the processor by setting up the Rhai engine.
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        let engine = Engine::new();

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            current_task_id: self.current_task_id,
            engine,
            task_context: Arc::clone(&self._task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.current_task_id))]
    async fn run(mut self) -> Result<(), Error> {
        // Initialize runner task.
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
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
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
                .task_type("test")
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
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_build_success() {
        let config = Arc::new(crate::task::script::config::Processor {
            name: "test".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: "data".to_string(),
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
    async fn test_script_simple_transformation() {
        let config = Arc::new(crate::task::script::config::Processor {
            name: "test".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: r#"#{ original: data, transformed: true }"#.to_string(),
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx: tx.clone(),
            current_task_id: 1,
            engine: Engine::new(),
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"x": 5})),
            subject: "input.subject".to_string(),
            current_task_id: Some(0),
            id: None,
            timestamp: 123456789,
            task_type: "test",
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
        });

        let (tx, mut rx) = broadcast::channel(100);
        let tx_clone = tx.clone();

        let event_handler = EventHandler {
            config,
            tx: tx_clone,
            current_task_id: 1,
            engine: Engine::new(),
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"age": 15})),
            subject: "input.subject".to_string(),
            task_type: "test",
            current_task_id: Some(0),
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
        });

        let (tx, mut rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            current_task_id: 1,
            engine: Engine::new(),
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({})),
            subject: "input.subject".to_string(),
            current_task_id: Some(0),
            id: None,
            timestamp: 123456789,
            task_type: "test",
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
    async fn test_event_handler_wrong_input_type_arrow() {
        let config = Arc::new(crate::task::script::config::Processor {
            name: "test".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: "data".to_string(),
        });

        let (tx, _rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            tx,
            current_task_id: 1,
            engine: Engine::new(),
            task_context: create_mock_task_context(),
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
            current_task_id: Some(0),
            id: None,
            timestamp: 123456789,
            task_type: "test",
        };

        let result = event_handler.handle(input_event).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::ExpectedJsonGotArrowRecordBatch
        ));
    }
}
