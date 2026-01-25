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
    #[error("Sending event to channel failed with error: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error("Event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Handles individual log operations.
pub struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Event sender for passing through logged events.
    tx: Sender<Event>,
    /// Task type identifier (unused but kept for consistency).
    _task_type: &'static str,
    /// Task context (unused but kept for consistency).
    _task_context: Arc<crate::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes an event by logging its data and passing it through.
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

        // Pass the event through to the next task with updated task_id
        let event = crate::event::EventBuilder::new()
            .data(event.data)
            .subject(event.subject)
            .task_id(self.task_id)
            .task_type(event.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        self.tx.send(event).map_err(|e| Error::SendMessage {
            source: Box::new(e),
        })?;

        Ok(())
    }
}

/// Log processor that outputs event data to logs.
#[derive(Debug)]
pub struct Processor {
    /// Log task configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for passing through events.
    tx: Sender<Event>,
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
            tx: self.tx.clone(),
            _task_type: self.task_type,
            _task_context: Arc::clone(&self._task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.task_id))]
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

    #[tokio::test]
    async fn test_processor_builder() {
        let config = Arc::new(crate::task::log::config::Processor {
            name: "test".to_string(),
            level: crate::task::log::config::LogLevel::Info,
            structured: false,
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
    async fn test_event_handler_logs_data() {
        let config = Arc::new(crate::task::log::config::Processor {
            name: "test".to_string(),
            level: crate::task::log::config::LogLevel::Info,
            structured: false,
            retry: None,
        });

        let (tx, _rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            task_id: 1,
            tx,
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
            retry: None,
        });

        let (tx, _rx) = broadcast::channel(100);

        let event_handler = EventHandler {
            config,
            task_id: 1,
            tx,
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
