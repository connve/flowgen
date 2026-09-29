use super::message::MongoEventsExt;
use crate::client::MongoClientBuilder;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{new_completion_channel, Event, EventBuilder, EventData, EventExt};
use futures_util::StreamExt;
use mongodb::options::FullDocumentType;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{error, warn};

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
    #[error("Stream ended unexpectedly")]
    StreamEnded,
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
}

type ChangeEvent = mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>;

/// Change metadata the event has no field for, merged into `event.meta`.
#[derive(Debug, serde::Serialize)]
struct ChangeMeta<'a> {
    database: Option<&'a str>,
    operation_type: &'a mongodb::change_stream::event::OperationType,
    document_key: Option<&'a mongodb::bson::Document>,
}

/// Where a change lands on the event: its subject, id, and meta.
#[derive(Debug)]
struct ChangeDetails {
    collection: Option<String>,
    id: String,
    meta: serde_json::Map<String, serde_json::Value>,
}

/// Key under a resume token holding its value.
const RESUME_TOKEN_DATA_KEY: &str = "_data";

fn change_details(change_event: &ChangeEvent) -> Result<ChangeDetails, Error> {
    let (database, collection) = match &change_event.ns {
        Some(ns) => (Some(ns.db.as_str()), ns.coll.clone()),
        None => (None, None),
    };
    let meta = match serde_json::to_value(ChangeMeta {
        database,
        operation_type: &change_event.operation_type,
        document_key: change_event.document_key.as_ref(),
    }) {
        Ok(serde_json::Value::Object(meta)) => meta,
        Ok(_) => serde_json::Map::new(),
        Err(source) => return Err(Error::SerdeJson { source }),
    };
    let token =
        serde_json::to_value(&change_event.id).map_err(|source| Error::SerdeJson { source })?;
    let id = match token.get(RESUME_TOKEN_DATA_KEY) {
        Some(serde_json::Value::String(data)) => data.clone(),
        _ => token.to_string(),
    };
    Ok(ChangeDetails {
        collection,
        id,
        meta,
    })
}

/// The change's payload: the document when the change carries one, the
/// document key for a delete, and nothing otherwise.
fn change_document(change_event: &ChangeEvent) -> Option<&mongodb::bson::Document> {
    match (
        &change_event.full_document,
        &change_event.operation_type,
        &change_event.document_key,
    ) {
        (Some(document), _, _) => Some(document),
        (None, mongodb::change_stream::event::OperationType::Delete, Some(key)) => Some(key),
        _ => None,
    }
}

/// Event handler that watches a MongoDB change stream and forwards events downstream.
#[derive(Debug)]
pub struct EventHandler {
    /// Change stream configuration.
    config: Arc<super::config::ChangeStream>,
    client: Arc<mongodb::Client>,
    /// Same key `init()` used to cache the MongoDB client, so `run()` can
    /// evict it if the cached client turns out to be permanently broken.
    client_key: flowgen_core::client_registry::ClientKey,
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
    #[tracing::instrument(skip(self), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self) -> Result<(), Error> {
        let db = self.client.database(&self.config.db_name);

        let mut change_stream = db
            .watch()
            .full_document(FullDocumentType::UpdateLookup)
            .await
            .map_err(|source| Error::MongoDB { source })?;

        loop {
            let result = tokio::select! {
                _ = self.task_context.cancellation_token.cancelled() => return Ok(()),
                result = change_stream.next() => result,
            };

            let Some(result) = result else {
                return Err(Error::StreamEnded);
            };

            let change_event = result.map_err(|source| Error::MongoDB { source })?;

            let (completion_state, _completion_rx) =
                new_completion_channel(self.task_context.leaf_count);

            let mut e = match change_document(&change_event) {
                Some(document) => document
                    .to_event(self.task_type, self.task_id)
                    .map_err(|source| Error::MessageConversion { source })?,
                None => EventBuilder::new()
                    .subject(self.config.db_name.clone())
                    .data(EventData::Json(serde_json::Value::Null))
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|source| Error::Event { source })?,
            };

            e.completion_tx = Some(completion_state);
            let details = change_details(&change_event)?;
            if let Some(collection) = details.collection {
                e.subject = collection;
            }
            e.id = Some(details.id);
            e.meta
                .get_or_insert_with(serde_json::Map::new)
                .extend(details.meta);

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;
        }
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
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let credentials_path = init_config.credentials_path.clone();
        let client_key =
            flowgen_core::client_registry::ClientKey::new(self.task_type, &credentials_path);
        let client = self
            .task_context
            .client_registry
            .get_or_init(client_key.clone(), || async {
                let mut builder = MongoClientBuilder::new();
                if let Some(path) = credentials_path {
                    builder = builder.credentials_path(path);
                }
                builder
                    .build()
                    .map_err(|e| Error::Auth { source: e })?
                    .connect()
                    .await
                    .map_err(|e| Error::Auth { source: e })
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        Ok(EventHandler {
            client,
            client_key,
            config: Arc::new(init_config),
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

        let mut reconnect_backoff = retry_config.reconnect_strategy();

        // No inner spawn: run() must stay pending for the task's real
        // lifetime so the caller's JoinHandle/abort actually reaches this loop.
        loop {
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
                    if self.task_context.cancellation_token.is_cancelled() {
                        return Ok(());
                    }
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
                        return Ok(());
                    }
                    warn!("Change stream ended unexpectedly, reconnecting");
                }
                Err(e) => {
                    if self.task_context.cancellation_token.is_cancelled() {
                        return Ok(());
                    }
                    error!(error = %e, "Change stream error, reconnecting");
                }
            }

            self.task_context
                .client_registry
                .remove(&event_handler.client_key)
                .await;

            let delay = match reconnect_backoff.next() {
                Some(d) => d,
                None => retry_config.initial_backoff,
            };
            tokio::time::sleep(delay).await;
        }
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
    use mongodb::bson::doc;
    use std::path::PathBuf;
    use tokio::sync::mpsc;

    #[test]
    fn test_change_details() {
        let change_event: ChangeEvent = mongodb::bson::from_document(doc! {
            "_id": { "_data": "token" },
            "operationType": "insert",
            "ns": { "db": "d", "coll": "c" },
            "documentKey": { "_id": 1 },
            "fullDocument": { "_id": 1 },
        })
        .unwrap();

        let details = change_details(&change_event).unwrap();

        assert_eq!(details.collection.as_deref(), Some("c"));
        assert_eq!(details.id, "token");
        assert_eq!(details.meta["database"], "d");
        assert_eq!(details.meta["operation_type"], "insert");
        assert_eq!(details.meta["document_key"], serde_json::json!({"_id": 1}));
    }

    #[test]
    fn test_change_document() {
        let change = |fields: mongodb::bson::Document| -> ChangeEvent {
            let mut event = doc! { "_id": { "_data": "token" }, "documentKey": { "_id": 1 } };
            event.extend(fields);
            mongodb::bson::from_document(event).unwrap()
        };
        let delete = change(doc! { "operationType": "delete" });
        let update_without_document = change(doc! { "operationType": "update" });
        let update =
            change(doc! { "operationType": "update", "fullDocument": { "_id": 1, "n": 2 } });

        assert_eq!(change_document(&delete), Some(&doc! { "_id": 1 }));
        assert_eq!(change_document(&update_without_document), None);
        assert_eq!(change_document(&update), Some(&doc! { "_id": 1, "n": 2 }));
    }

    fn create_mock_config() -> super::super::config::ChangeStream {
        super::super::config::ChangeStream {
            name: "test_change_stream".to_string(),
            db_name: "test_database".to_string(),
            credentials_path: Some(PathBuf::from("/mock/path/credentials.json")),
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

    #[test]
    fn test_error_display_auth() {
        let err = Error::Auth {
            source: crate::client::Error::CredentialsFileRead {
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
            },
        };
        assert!(matches!(err, Error::Auth { .. }));
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
    fn test_error_display_stream_ended() {
        let err = Error::StreamEnded;
        assert_eq!(err.to_string(), "Stream ended unexpectedly");
    }

    #[test]
    fn test_error_from_mongodb() {
        let source = mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        let err = Error::MongoDB { source };
        assert!(err.to_string().contains("MongoDB error:"));
    }

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

    #[tokio::test]
    async fn test_event_handler_struct_can_be_constructed() {
        let config = Arc::new(create_mock_config());
        let client = Arc::new(
            mongodb::Client::with_uri_str("mongodb://localhost:27017")
                .await
                .expect("Should create client with URI"),
        );
        let (_tx, _rx) = mpsc::channel(10);
        let handler = EventHandler {
            config,
            client,
            client_key: flowgen_core::client_registry::ClientKey::new("test", &"test"),
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

    #[tokio::test]
    async fn test_init_auth_failure() {
        use flowgen_core::task::runner::Runner;

        let config = Arc::new(super::super::config::ChangeStream {
            name: "auth_fail_test".to_string(),
            db_name: "test_db".to_string(),
            credentials_path: Some(PathBuf::from("/invalid/credentials/path.json")),
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
        let client = Arc::new(
            mongodb::Client::with_uri_str("mongodb://localhost:27017")
                .await
                .expect("Should create client with URI"),
        );
        let handler = EventHandler {
            config,
            client,
            client_key: flowgen_core::client_registry::ClientKey::new("test", &"test"),
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
    async fn test_run_stays_alive_until_cancelled() {
        use flowgen_core::task::runner::Runner;

        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        let task_context = Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .cancellation_token(cancellation_token.clone())
                .build()
                .unwrap(),
        );

        let config = Arc::new(super::super::config::ChangeStream {
            name: "run_test".to_string(),
            db_name: "test_db".to_string(),
            credentials_path: Some(PathBuf::from("/mock/path/creds.json")),
            depends_on: Some(Vec::new()),
            retry: Some(flowgen_core::retry::RetryConfig {
                max_attempts: Some(1),
                initial_backoff: std::time::Duration::from_millis(20),
            }),
        });
        let reader = ChangeStreamReaderBuilder::new()
            .config(config)
            .task_context(task_context)
            .task_type("run_test")
            .build()
            .await
            .unwrap();

        let handle = tokio::spawn(reader.run());

        // run() must still be alive after the init retries would have
        // exhausted — it reconnects forever until cancelled, unlike the
        // old detached-inner-spawn version which returned instantly.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!handle.is_finished());

        cancellation_token.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("run() must return promptly after cancellation")
            .unwrap();
        assert!(result.is_ok());
    }
}
