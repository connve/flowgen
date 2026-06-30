use super::message::MongoEventsExt;
use crate::client::MongoClientBuilder;
use flowgen_core::event::{new_completion_channel, Event, EventExt};
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{error, warn, Instrument};

/// Errors that can occur during MongoDB change stream operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: crate::client::Error,
    },
    #[error("Send event message error: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Event error: {source}")]
    Event {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("MongoDB error: {source}")]
    MongoDB {
        #[source]
        source: mongodb::error::Error,
    },
    #[error("Message conversion failed with error: {source}")]
    MessageConversion {
        #[source]
        source: crate::message::Error,
    },
    #[error("Full document is not available for this operation")]
    NoFullDocument(),
    #[error("Stream ended unexpectedly")]
    StreamEnded,
}

impl From<mongodb::error::Error> for Error {
    fn from(source: mongodb::error::Error) -> Self {
        Error::MongoDB { source }
    }
}

/// Event handler that watches a MongoDB change stream and forwards events downstream.
#[derive(Debug)]
pub struct EventHandler {
    /// Change stream configuration.
    config: Arc<super::config::ChangeStream>,
    /// MongoDB client connection.
    client: mongodb::Client,
    /// Current task identifier.
    task_id: usize,
    /// Optional channel sender for downstream events.
    tx: Option<Sender<Event>>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Watches the MongoDB change stream and emits change events downstream.
    #[tracing::instrument(skip(self), name = "task.handle")]
    async fn handle(&self) -> Result<(), Error> {
        let db = self.client.database(&self.config.db_name);

        let mut change_stream = db.watch().await?;

        while let Some(result) = change_stream.next().await {
            if self.task_context.cancellation_token.is_cancelled() {
                return Ok(());
            }

            let change_event = result?;

            let doc = change_event
                .full_document
                .as_ref()
                .ok_or(Error::NoFullDocument())?;

            let (completion_state, _completion_rx) =
                new_completion_channel(self.task_context.leaf_count);

            let mut e = doc
                .to_event(self.task_type, self.task_id)
                .map_err(|source| Error::MessageConversion { source })?;

            e.completion_tx = Some(completion_state);

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;
        }

        Err(Error::StreamEnded)
    }
}

/// MongoDB change stream reader that watches for real-time changes.
#[derive(Debug)]
pub struct ChangeStreamReader {
    /// Reader configuration settings.
    config: Arc<super::config::ChangeStream>,
    /// Optional channel sender for downstream events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event tracking.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for ChangeStreamReader {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the reader by establishing a MongoDB client connection.
    async fn init(&self) -> Result<EventHandler, Error> {
        let client = MongoClientBuilder::new()
            .credentials_path(self.config.credentials_path.clone())
            .map_err(|e| Error::Auth { source: e })?
            .default_host("cluster0.mongodb.net".to_string())
            .build()
            .map_err(|e| Error::Auth { source: e })?
            .connect()
            .await
            .map_err(|e| Error::Auth { source: e })?;

        Ok(EventHandler {
            client,
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        tokio::spawn(
            async move {
                let mut reconnect_backoff = retry_config.reconnect_strategy();

                // Infinite retry loop: change stream must maintain connectivity indefinitely.
                loop {
                    // Initialize with circuit breaker to detect permanent errors.
                    let event_handler = match tokio_retry::Retry::spawn(
                        retry_config.init_strategy(self.task_context.startup_delay),
                        || async {
                            match self.init().await {
                                Ok(handler) => Ok(handler),
                                Err(e) => {
                                    error!(error = %e, "Failed to initialize change stream reader");
                                    Err(tokio_retry::RetryError::transient(e))
                                }
                            }
                        },
                    )
                    .await
                    {
                        Ok(handler) => {
                            reconnect_backoff = retry_config.reconnect_strategy();
                            handler
                        }
                        Err(e) => {
                            let delay = match reconnect_backoff.next() {
                                Some(d) => d,
                                None => retry_config.initial_backoff,
                            };
                            error!(
                                error = %e,
                                delay_ms = %delay.as_millis(),
                                "Change stream reader initialization exhausted retry attempts, will retry after backoff"
                            );
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    };

                    match event_handler.handle().await {
                        Ok(()) => {
                            if self.task_context.cancellation_token.is_cancelled() {
                                return;
                            }
                            warn!("Change stream ended unexpectedly, reconnecting");
                        }
                        Err(e) => {
                            if self.task_context.cancellation_token.is_cancelled() {
                                return;
                            }
                            error!(error = %e, "Change stream error, reconnecting");
                        }
                    }

                    let delay = match reconnect_backoff.next() {
                        Some(d) => d,
                        None => retry_config.initial_backoff,
                    };
                    tokio::time::sleep(delay).await;
                }
            }
            .instrument(tracing::Span::current()),
        );

        Ok(())
    }
}

/// Builder for constructing ChangeStreamReader instances.
#[derive(Default)]
pub struct ChangeStreamReaderBuilder {
    config: Option<Arc<super::config::ChangeStream>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ChangeStreamReaderBuilder {
    pub fn new() -> ChangeStreamReaderBuilder {
        ChangeStreamReaderBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::ChangeStream>) -> Self {
        self.config = Some(config);
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

    pub async fn build(self) -> Result<ChangeStreamReader, Error> {
        Ok(ChangeStreamReader {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
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
    use std::path::PathBuf;
    use tokio::sync::mpsc;

    fn create_mock_config() -> super::super::config::ChangeStream {
        super::super::config::ChangeStream {
            name: "test_change_stream".to_string(),
            db_name: "test_database".to_string(),
            credentials_path: PathBuf::from("/mock/path/credentials.json"),
            depends_on: Some(Vec::new()),
            retry: Default::default(),
        }
    }

    fn mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    // ==========================================
    // 1. ERROR DISPLAY FORMATTING TESTS
    // ==========================================

    #[test]
    fn test_error_display_auth() {
        let err = Error::Auth {
            source: crate::client::Error::MissingCredentials,
        };
        assert_eq!(
            err.to_string(),
            "Authentication error: Missing credentials error."
        );
    }

    #[test]
    fn test_error_display_send_message() {
        let err = Error::SendMessage {
            source: flowgen_core::event::Error::SendMessage,
        };
        assert_eq!(
            err.to_string(),
            "Send event message error: Error sending event to channel (receiver dropped)"
        );
    }

    #[test]
    fn test_error_display_event() {
        let err = Error::Event {
            source: flowgen_core::event::Error::SendMessage,
        };
        assert_eq!(
            err.to_string(),
            "Event error: Error sending event to channel (receiver dropped)"
        );
    }

    #[test]
    fn test_error_display_missing_builder_attribute() {
        let err = Error::MissingBuilderAttribute("test_field".to_string());
        assert_eq!(err.to_string(), "Missing required attribute: test_field");
    }

    #[test]
    fn test_error_display_retry_exhausted() {
        let inner = Box::new(Error::MissingBuilderAttribute("inner".to_string()));
        let err = Error::RetryExhausted { source: inner };
        assert_eq!(
            err.to_string(),
            "Task failed after all retry attempts: Missing required attribute: inner"
        );
    }

    #[test]
    fn test_error_display_mongodb() {
        let mongo_err = mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        let err = Error::MongoDB { source: mongo_err };
        assert!(err.to_string().contains("MongoDB error:"));
    }

    #[test]
    fn test_error_display_message_conversion() {
        let err = Error::MessageConversion {
            source: crate::message::Error::NoRecordBatch(),
        };
        assert_eq!(
            err.to_string(),
            "Message conversion failed with error: Error getting record batch"
        );
    }

    #[test]
    fn test_error_display_no_full_document() {
        let err = Error::NoFullDocument();
        assert_eq!(
            err.to_string(),
            "Full document is not available for this operation"
        );
    }

    #[test]
    fn test_error_display_stream_ended() {
        let err = Error::StreamEnded;
        assert_eq!(err.to_string(), "Stream ended unexpectedly");
    }

    #[test]
    fn test_error_from_mongodb() {
        let mongo_err = mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        let err: Error = mongo_err.into();
        assert!(matches!(err, Error::MongoDB { .. }));
        assert!(err.to_string().contains("MongoDB error:"));
    }

    // ==========================================
    // 2. BUILDER VALIDATION TESTS
    // ==========================================

    #[tokio::test]
    async fn test_builder_missing_config() {
        let result = ChangeStreamReaderBuilder::new()
            .task_context(mock_task_context())
            .task_type("test")
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(ref attr) if attr == "config"
        ));
    }

    #[tokio::test]
    async fn test_builder_missing_task_context() {
        let config = Arc::new(create_mock_config());
        let result = ChangeStreamReaderBuilder::new()
            .config(config)
            .task_type("test")
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(ref attr) if attr == "task_context"
        ));
    }

    #[tokio::test]
    async fn test_builder_missing_task_type() {
        let config = Arc::new(create_mock_config());
        let result = ChangeStreamReaderBuilder::new()
            .config(config)
            .task_context(mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(ref attr) if attr == "task_type"
        ));
    }

    #[tokio::test]
    async fn test_builder_success() {
        let (tx, _rx) = mpsc::channel(10);
        let config = Arc::new(create_mock_config());
        let reader = ChangeStreamReaderBuilder::new()
            .config(config)
            .sender(tx)
            .task_id(42)
            .task_context(mock_task_context())
            .task_type("change_stream_test")
            .build()
            .await
            .expect("Builder should create ChangeStreamReader successfully");
        assert_eq!(reader.task_id, 42);
        assert_eq!(reader.task_type, "change_stream_test");
        assert!(reader.tx.is_some());
    }

    #[tokio::test]
    async fn test_builder_success_without_sender() {
        let config = Arc::new(create_mock_config());
        let reader = ChangeStreamReaderBuilder::new()
            .config(config)
            .task_id(7)
            .task_context(mock_task_context())
            .task_type("no_sender")
            .build()
            .await
            .expect("Builder should create ChangeStreamReader without sender");
        assert_eq!(reader.task_id, 7);
        assert_eq!(reader.task_type, "no_sender");
        assert!(reader.tx.is_none());
    }

    // ==========================================
    // 3. STRUCT INSTANTIATION TESTS
    // ==========================================

    #[tokio::test]
    async fn test_event_handler_struct_can_be_constructed() {
        let config = Arc::new(create_mock_config());
        let client = mongodb::Client::with_uri_str("mongodb://localhost:27017")
            .await
            .expect("Should create client with URI");
        let (_tx, _rx) = mpsc::channel(10);
        let handler = EventHandler {
            config,
            client,
            task_id: 1,
            tx: Some(_tx),
            task_type: "test_handler",
            task_context: mock_task_context(),
        };
        assert_eq!(handler.task_id, 1);
        assert_eq!(handler.task_type, "test_handler");
    }

    #[test]
    fn test_change_stream_reader_debug() {
        let config = Arc::new(create_mock_config());
        let reader = ChangeStreamReader {
            config,
            tx: None,
            task_id: 1,
            task_context: mock_task_context(),
            task_type: "test",
        };
        let debug_str = format!("{:?}", reader);
        assert!(debug_str.contains("ChangeStreamReader"));
        assert!(debug_str.contains("task_id: 1"));
    }

    // ==========================================
    // 4. INIT AND HANDLER PATH TESTS
    // ==========================================

    #[tokio::test]
    async fn test_init_auth_failure() {
        use flowgen_core::task::runner::Runner;

        let config = Arc::new(super::super::config::ChangeStream {
            name: "auth_fail_test".to_string(),
            db_name: "test_db".to_string(),
            credentials_path: PathBuf::from("/invalid/credentials/path.json"),
            depends_on: Some(Vec::new()),
            retry: Default::default(),
        });
        let reader = ChangeStreamReaderBuilder::new()
            .config(config)
            .task_context(mock_task_context())
            .task_type("auth_test")
            .build()
            .await
            .unwrap();
        let result = reader.init().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Auth { .. }));
    }

    #[tokio::test]
    async fn test_handle_fails_without_connection() {
        let config = Arc::new(create_mock_config());
        let client = mongodb::Client::with_uri_str("mongodb://localhost:27017")
            .await
            .expect("Should create client with URI");
        let handler = EventHandler {
            config,
            client,
            task_id: 1,
            tx: None,
            task_type: "handle_test",
            task_context: mock_task_context(),
        };
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), handler.handle()).await;
        match result {
            Ok(Err(_)) => {}
            Err(_elapsed) => {}
            Ok(Ok(())) => panic!("Expected handle to fail without a MongoDB connection"),
        }
    }

    #[tokio::test]
    async fn test_run_returns_ok_immediately() {
        use flowgen_core::task::runner::Runner;

        let config = Arc::new(super::super::config::ChangeStream {
            name: "run_test".to_string(),
            db_name: "test_db".to_string(),
            credentials_path: PathBuf::from("/mock/path/creds.json"),
            depends_on: Some(Vec::new()),
            retry: Some(flowgen_core::retry::RetryConfig {
                max_attempts: Some(1),
                initial_backoff: std::time::Duration::from_millis(50),
            }),
        });
        let reader = ChangeStreamReaderBuilder::new()
            .config(config)
            .task_context(mock_task_context())
            .task_type("run_test")
            .build()
            .await
            .unwrap();
        let result = reader.run().await;
        assert!(result.is_ok());
    }
}
