use super::message::MongoEventsExt;
use crate::client::MongoClientBuilder;
use flowgen_core::event::Event;
use flowgen_core::event::EventExt;
use futures::TryStreamExt;
use mongodb::bson::Document;
use mongodb::{bson::doc, Collection};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: crate::client::Error,
    },
    #[error("Sending event to channel failed with error: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Reader event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Configuration template rendering failed with error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Message conversion failed with error: {source}")]
    MessageConversion {
        #[source]
        source: crate::message::Error,
    },
    #[error("Other subscriber error")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("MongoDB error: {0}")]
    MongoDB(#[from] mongodb::error::Error),
}

#[derive(Debug)]
/// Handles processing of individual events by querying MongoDB and emitting results.
pub struct EventHandler {
    client: mongodb::Client,
    config: Arc<super::config::Reader>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    task_type: &'static str,
}

impl EventHandler {
    /// Processes a single document result and sends it downstream.
    async fn process_message(
        &self,
        message_result: Result<Document, Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), Error> {
        match message_result {
            Ok(message) => {
                let e = message
                    .to_event(self.task_type, self.task_id)
                    .map_err(|source| Error::MessageConversion { source })?;

                e.send_with_logging(self.tx.as_ref())
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
                Ok(())
            }
            Err(err) => Err(Error::Other(err)),
        }
    }

    /// Queries MongoDB and emits each document as an event.
    async fn handle(&self, _event: Event) -> Result<(), Error> {
        let document_collection: Collection<Document> = self
            .client
            .database(&self.config.db_name)
            .collection(&self.config.collection_name);

        let doc = build_filter_doc(&self.config.filter);

        let mut cursor = document_collection.find(doc).await?;
        while let Some(doc) = cursor.try_next().await? {
            self.process_message(Ok(doc)).await?;
        }

        Ok(())
    }
}

/// MongoDB reader that processes events from an mpsc receiver.
#[derive(Debug)]
pub struct Reader {
    /// Reader configuration settings.
    config: Arc<super::config::Reader>,
    /// Receiver for incoming events.
    rx: Receiver<Event>,
    /// Optional channel sender for processed events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Reader {
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
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
        })
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize MongoDB reader");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => return Err(e),
        };

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    if Some(event.task_id) == event_handler.task_id.checked_sub(1) {
                        let event_handler = Arc::clone(&event_handler);
                        let retry_strategy = retry_config.strategy();
                        let event_clone = event.clone();

                        tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event_clone.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!(error = %e, "Failed to process MongoDB event");
                                            Err(tokio_retry::RetryError::transient(e))
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(
                                        error = %err,
                                        "MongoDB reader failed after all retry attempts"
                                    );
                                    // Emit error event downstream for error handling.
                                    let mut error_event = event_clone.clone();
                                    error_event.error = Some(err.to_string());
                                    if let Some(ref tx) = event_handler.tx {
                                        tx.send(error_event).await.ok();
                                    }
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );
                    }
                }
                None => return Ok(()),
            }
        }
    }
}

fn build_filter_doc(filter: &std::collections::HashMap<String, String>) -> Document {
    let mut doc = Document::new();
    for (key, value) in filter {
        doc.insert(key, value);
    }
    doc
}

/// Builder pattern for constructing Reader instances.
#[derive(Default)]
pub struct ReaderBuilder {
    config: Option<Arc<super::config::Reader>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ReaderBuilder {
    pub fn new() -> ReaderBuilder {
        ReaderBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Reader>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets an optional downstream sender. Omit for terminal readers.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
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

    pub fn build(self) -> Result<Reader, Error> {
        Ok(Reader {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self.task_id.unwrap_or(0),
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
    use flowgen_core::task::runner::Runner;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::mpsc::channel;

    /// Helper utility to construct a mock Reader config matching the flowgen repository specification.
    fn create_mock_config(credentials: PathBuf) -> super::super::config::Reader {
        super::super::config::Reader {
            name: "test_mongo_reader".to_string(),
            db_name: "test_database".to_string(),
            collection_name: "test_collection".to_string(),
            filter: HashMap::new(),
            credentials_path: credentials,
            depends_on: Some(Vec::new()),
            retry: Default::default(),
        }
    }

    /// Forges a heap-allocated ArcInner structure layout matching Arc<TaskContext>.
    /// Initializes reference counts to 1,000,000 so downstream drops merely decrement
    /// the counter instead of triggering uninitialized drops or allocator faults.
    fn mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let layout = std::alloc::Layout::from_size_align(2048, 8).unwrap();
        unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Mock allocation failed");
            }

            // Set strong reference count to 1,000,000
            let strong_ptr = ptr as *mut usize;
            *strong_ptr = 1_000_000;

            // Set weak reference count to 1,000,000
            let weak_ptr = strong_ptr.add(1);
            *weak_ptr = 1_000_000;

            // Transmute the pointer directly into a valid Arc instance
            std::mem::transmute::<*mut u8, Arc<flowgen_core::task::context::TaskContext>>(ptr)
        }
    }

    // ==========================================
    // 1. PURE FUNCTION & UTILITY TESTS
    // ==========================================

    #[test]
    fn test_build_filter_doc_with_data() {
        let mut filter = HashMap::new();
        filter.insert("status".to_string(), "active".to_string());
        filter.insert("environment".to_string(), "production".to_string());

        let doc = build_filter_doc(&filter);

        assert_eq!(doc.len(), 2);
        assert_eq!(doc.get_str("status").unwrap(), "active");
        assert_eq!(doc.get_str("environment").unwrap(), "production");
    }

    #[test]
    fn test_build_filter_doc_empty() {
        let filter = HashMap::new();
        let doc = build_filter_doc(&filter);
        assert!(doc.is_empty());
    }

    // ==========================================
    // 2. ERROR DISPLAY FORMATTING TESTS
    // ==========================================

    #[test]
    fn test_error_enum_formatting() {
        let err = Error::MissingRequiredAttribute("test_field".to_string());
        assert_eq!(
            err.to_string(),
            "Missing required builder attribute: test_field"
        );

        let err = Error::Other(Box::new(std::io::Error::other("mock")));
        assert_eq!(err.to_string(), "Other subscriber error");

        let err = Error::Auth {
            source: crate::client::Error::MissingCredentials,
        };
        assert_eq!(
            err.to_string(),
            "Authentication error: Missing credentials error."
        );

        let err = Error::MongoDB(mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        )));
        assert!(err.to_string().contains("MongoDB error:"));

        let err = Error::SendMessage {
            source: flowgen_core::event::Error::SendMessage,
        };
        assert_eq!(
            err.to_string(),
            "Sending event to channel failed with error: Error sending event to channel (receiver dropped)"
        );

        let err = Error::MessageConversion {
            source: crate::message::Error::NoRecordBatch(),
        };
        assert_eq!(
            err.to_string(),
            "Message conversion failed with error: Error getting record batch"
        );

        let serde_err = serde_json::from_str::<()>("invalid").unwrap_err();
        let err = Error::ConfigRender {
            source: flowgen_core::config::Error::SerdeJson { source: serde_err },
        };
        assert!(err
            .to_string()
            .contains("Configuration template rendering failed with error:"));

        let inner = Box::new(Error::MissingRequiredAttribute("inner".to_string()));
        let err = Error::RetryExhausted { source: inner };
        assert_eq!(
            err.to_string(),
            "Task failed after all retry attempts: Missing required builder attribute: inner"
        );

        let inner = flowgen_core::event::Error::MissingBuilderAttribute("test".to_string());
        let err = Error::EventBuilder { source: inner };
        assert_eq!(
            err.to_string(),
            "Reader event builder failed with error: Missing required builder attribute: test"
        );
    }

    // ==========================================
    // 3. BUILDER VALIDATION & SUCCESS TESTS
    // ==========================================

    #[test]
    fn test_reader_builder_missing_all_attributes() {
        let builder = ReaderBuilder::new();
        let result = builder.build();

        assert!(result.is_err());
        if let Err(Error::MissingRequiredAttribute(attr)) = result {
            assert_eq!(attr, "config");
        } else {
            panic!("Expected MissingRequiredAttribute error for 'config'");
        }
    }

    #[test]
    fn test_reader_builder_missing_receiver() {
        let config = Arc::new(create_mock_config(PathBuf::new()));
        let builder = ReaderBuilder::new().config(config);

        let result = builder.build();
        assert!(result.is_err());
        if let Err(Error::MissingRequiredAttribute(attr)) = result {
            assert_eq!(attr, "receiver");
        } else {
            panic!("Expected MissingRequiredAttribute error for 'receiver'");
        }
    }

    #[test]
    fn test_reader_builder_missing_task_type() {
        let (_tx, rx) = channel(1);
        let config = Arc::new(create_mock_config(PathBuf::new()));

        let builder = ReaderBuilder::new().config(config).receiver(rx);

        let result = builder.build();
        assert!(result.is_err());
        if let Err(Error::MissingRequiredAttribute(attr)) = result {
            assert_eq!(attr, "task_context");
        } else {
            panic!("Expected MissingRequiredAttribute error variant for property 'task_context'");
        }
    }

    #[test]
    fn test_reader_builder_success() {
        let (tx, rx) = channel(1);
        let config = Arc::new(create_mock_config(PathBuf::new()));

        let reader = ReaderBuilder::new()
            .config(config)
            .receiver(rx)
            .sender(tx)
            .task_id(42)
            .task_context(mock_task_context())
            .task_type("test_type")
            .build()
            .expect("Builder should successfully create Reader");

        assert_eq!(reader.task_id, 42);
        assert_eq!(reader.task_type, "test_type");
        assert!(reader.tx.is_some());
    }

    #[test]
    fn test_reader_builder_no_sender() {
        let (_tx, rx) = channel(1);
        let config = Arc::new(create_mock_config(PathBuf::new()));

        let reader = ReaderBuilder::new()
            .config(config)
            .receiver(rx)
            .task_id(7)
            .task_context(mock_task_context())
            .task_type("no_sender")
            .build()
            .expect("Builder should create Reader without sender");

        assert_eq!(reader.task_id, 7);
        assert_eq!(reader.task_type, "no_sender");
        assert!(reader.tx.is_none());
    }

    #[test]
    fn test_build_filter_doc_with_special_keys() {
        let mut filter = HashMap::new();
        filter.insert("field.with.dots".to_string(), "value1".to_string());
        filter.insert("nested:field".to_string(), "value2".to_string());

        let doc = build_filter_doc(&filter);

        assert_eq!(doc.len(), 2);
        assert_eq!(doc.get_str("field.with.dots").unwrap(), "value1");
        assert_eq!(doc.get_str("nested:field").unwrap(), "value2");
    }

    // ==========================================
    // 4. RUNTIME, ERROR & STATE HANDLING TESTS
    // ==========================================

    #[tokio::test]
    async fn test_event_handler_process_message_success() {
        let client = mongodb::Client::with_uri_str("mongodb://localhost:27017")
            .await
            .unwrap();
        let config = Arc::new(create_mock_config(PathBuf::new()));

        let handler = EventHandler {
            client,
            config,
            task_id: 1,
            tx: None,
            task_type: "reader_test",
        };

        let mut doc = Document::new();
        doc.insert("name", "test_user");
        doc.insert("value", 42_i64);

        let result = handler.process_message(Ok(doc)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_event_handler_process_message_error_forwarding() {
        let client = mongodb::Client::with_uri_str("mongodb://localhost:27017")
            .await
            .unwrap();
        let config = Arc::new(create_mock_config(PathBuf::new()));

        let handler = EventHandler {
            client,
            config,
            task_id: 1,
            tx: None,
            task_type: "error_test",
        };

        let structural_error = Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "Mock network failure",
        ));

        let result = handler.process_message(Err(structural_error)).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(Error::Other(_))));
    }

    #[tokio::test]
    async fn test_reader_init_auth_failure() {
        let (_tx, rx) = channel(1);
        let config = Arc::new(create_mock_config(PathBuf::from(
            "/invalid/credentials/path.json",
        )));

        let reader = ReaderBuilder::new()
            .config(config)
            .receiver(rx)
            .task_context(mock_task_context())
            .task_type("auth_test")
            .build()
            .unwrap();

        let result = reader.init().await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Auth { .. }));
    }

    #[tokio::test]
    async fn test_reader_init_connect_failure() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "flowgen_test_connect_fail_{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, r#"{"MONGODB_URI": "not-a-valid-uri"}"#).unwrap();

        let (_tx, rx) = channel(1);
        let config = Arc::new(create_mock_config(path.clone()));

        let reader = ReaderBuilder::new()
            .config(config)
            .receiver(rx)
            .task_context(mock_task_context())
            .task_type("connect_fail")
            .build()
            .unwrap();

        let result = reader.init().await;

        std::fs::remove_file(&path).ok();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Auth { .. }));
    }

    #[tokio::test]
    async fn test_reader_builder_missing_task_type_with_context() {
        let (_tx, rx) = channel(1);
        let config = Arc::new(create_mock_config(PathBuf::new()));

        let builder = ReaderBuilder::new()
            .config(config)
            .receiver(rx)
            .task_context(mock_task_context());

        let result = builder.build();
        assert!(result.is_err());
        if let Err(Error::MissingRequiredAttribute(attr)) = result {
            assert_eq!(attr, "task_type");
        } else {
            panic!("Expected MissingRequiredAttribute error for 'task_type'");
        }
    }
}
