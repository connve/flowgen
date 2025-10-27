//! Log processor for outputting event data to application logs.

use crate::event::Event;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{debug, error, info, trace, warn, Instrument};

/// Default subject prefix for log events.
const DEFAULT_MESSAGE_SUBJECT: &str = "log";

/// Errors that can occur during log processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Required builder attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

/// Handles individual log operations.
pub struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task type identifier (unused but kept for consistency).
    _task_type: &'static str,
    /// Task context (unused but kept for consistency).
    _task_context: Arc<crate::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes an event by logging its data.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        if self.config.structured {
            // Structured logging mode for Grafana/Loki
            match &event.data {
                crate::event::EventData::Json(json) => {
                    let parsed_json = match json {
                        serde_json::Value::String(json_str) => {
                            serde_json::from_str::<serde_json::Value>(json_str)
                                .unwrap_or_else(|_| json.clone())
                        }
                        other => other.clone(),
                    };

                    match self.config.level {
                        super::config::LogLevel::Trace => trace!(data = ?parsed_json),
                        super::config::LogLevel::Debug => debug!(data = ?parsed_json),
                        super::config::LogLevel::Info => info!(data = ?parsed_json),
                        super::config::LogLevel::Warn => warn!(data = ?parsed_json),
                        super::config::LogLevel::Error => error!(data = ?parsed_json),
                    }
                }
                other => match self.config.level {
                    super::config::LogLevel::Trace => trace!(data = ?other),
                    super::config::LogLevel::Debug => debug!(data = ?other),
                    super::config::LogLevel::Info => info!(data = ?other),
                    super::config::LogLevel::Warn => warn!(data = ?other),
                    super::config::LogLevel::Error => error!(data = ?other),
                },
            }
        } else {
            // Pretty-printed mode for console readability
            let log_message = match &event.data {
                crate::event::EventData::Json(json) => match json {
                    serde_json::Value::String(json_str) => {
                        match serde_json::from_str::<serde_json::Value>(json_str) {
                            Ok(parsed) => format!(
                                "\n{}",
                                serde_json::to_string_pretty(&parsed)
                                    .unwrap_or_else(|_| json_str.clone())
                            ),
                            Err(_) => json_str.clone(),
                        }
                    }
                    other_json => format!(
                        "\n{}",
                        serde_json::to_string_pretty(other_json)
                            .unwrap_or_else(|_| format!("{other_json:?}"))
                    ),
                },
                other => format!("{other:?}"),
            };

            match self.config.level {
                super::config::LogLevel::Trace => trace!("{}", log_message),
                super::config::LogLevel::Debug => debug!("{}", log_message),
                super::config::LogLevel::Info => info!("{}", log_message),
                super::config::LogLevel::Warn => warn!("{}", log_message),
                super::config::LogLevel::Error => error!("{}", log_message),
            }
        }

        Ok(())
    }
}

/// Log processor that outputs event data to logs.
#[derive(Debug)]
pub struct Processor {
    /// Log task configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for events (not used but kept for consistency).
    _tx: Sender<Event>,
    /// Channel receiver for incoming events to log.
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

    /// Initializes the processor.
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            _task_type: self.task_type,
            _task_context: Arc::clone(&self._task_context),
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
            _tx: self
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
    use crate::event::{EventBuilder, EventData};
    use serde_json::{json, Map, Value};
    use tokio::sync::broadcast;

    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Log Test".to_string()),
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
        let config = Arc::new(crate::task::log::config::Processor {
            name: "test".to_string(),
            level: crate::task::log::config::LogLevel::Info,
            structured: false,
        });

        let (tx, _rx) = broadcast::channel(100);
        let rx2 = tx.subscribe();

        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx2)
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        assert_eq!(processor.task_id, 1);
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
    async fn test_event_handler_logs_data() {
        let config = Arc::new(crate::task::log::config::Processor {
            name: "test".to_string(),
            level: crate::task::log::config::LogLevel::Info,
            structured: false,
        });

        let event_handler = EventHandler {
            config,
            task_id: 1,
            _task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = EventBuilder::new()
            .data(EventData::Json(json!({"message": "test log"})))
            .subject("test.subject".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();

        let result = event_handler.handle(input_event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_event_handler_filters_wrong_task_id() {
        let config = Arc::new(crate::task::log::config::Processor {
            name: "test".to_string(),
            level: crate::task::log::config::LogLevel::Info,
            structured: false,
        });

        let event_handler = EventHandler {
            config,
            task_id: 1,
            _task_type: "test",
            _task_context: create_mock_task_context(),
        };

        let input_event = EventBuilder::new()
            .data(EventData::Json(json!({"message": "test log"})))
            .subject("test.subject".to_string())
            .task_id(5)
            .task_type("test")
            .build()
            .unwrap();

        let result = event_handler.handle(input_event).await;
        assert!(result.is_ok());
    }
}
