use crate::client::MongoClientBuilder;
use flowgen_core::event::{Event, EventData, EventExt};
use futures_util::future;
use mongodb::bson::{oid::ObjectId, Bson, Document};
use mongodb::Collection;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during MongoDB write operations.
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
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Unsupported event data")]
    UnsupportedEventData,
    #[error("Invalid Mongo Document")]
    InvalidDocument,
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Mongo Writer failed: {source}")]
    MongoWriter {
        #[source]
        source: mongodb::error::Error,
    },
    #[error("Writer event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
}

impl From<mongodb::error::Error> for Error {
    fn from(source: mongodb::error::Error) -> Self {
        Error::MongoWriter { source }
    }
}

/// Event handler for processing and writing events to MongoDB.
pub struct EventHandler {
    /// Writer configuration.
    config: Arc<super::config::Writer>,
    client: mongodb::Client,
    /// Current task identifier.
    task_id: usize,
    /// Optional channel sender for response events.
    tx: Option<Sender<Event>>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event by writing it to the configured MongoDB collection.
    #[tracing::instrument(skip(self, event), name = "task.handle")]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        let json = match &event.data {
            EventData::Json(value) => value.clone(),
            _ => return Err(Error::UnsupportedEventData),
        };

        // Extract or generate an ObjectId from the incoming document.
        let oid = json["_id"]
            .as_object()
            .and_then(|obj| obj.get("$oid"))
            .and_then(|v| v.as_str())
            .and_then(|s| ObjectId::parse_str(s).ok())
            .unwrap_or_else(ObjectId::new);

        // Convert entire JSON to BSON Document.
        let mut doc = match json_to_bson(&json) {
            Bson::Document(d) => d,
            _ => return Err(Error::InvalidDocument),
        };

        // Override _id with the native ObjectId.
        doc.insert("_id", Bson::ObjectId(oid));

        let subject = format!("{}.{}", self.config.db_name, self.config.collection_name);

        let db_collection: Collection<Document> = self
            .client
            .database(&self.config.db_name)
            .collection(&self.config.collection_name);

        let resp = db_collection.insert_one(&doc).await?;

        let resp_json = serde_json::to_value(&resp).map_err(|e| Error::SerdeJson { source: e })?;

        let mut e = flowgen_core::event::EventBuilder::new()
            .data(EventData::Json(resp_json))
            .subject(subject)
            .id(resp.inserted_id.to_string())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()?;

        // Signal completion or pass through to the next task.
        match self.tx {
            None => {
                // Leaf task: signal completion directly.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
            Some(_) => {
                // Pass completion_tx through to the next task.
                e.completion_tx = completion_tx_arc.clone();
            }
        }

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|source| Error::SendMessage { source })?;

        Ok(())
    }
}

/// MongoDB writer that receives events and writes them to a configured collection.
#[derive(Debug)]
pub struct Writer {
    /// Writer configuration including collection settings and credentials.
    config: Arc<super::config::Writer>,
    /// Receiver for incoming events to write.
    rx: Receiver<Event>,
    /// Optional channel sender for response events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;
    type EventHandler = EventHandler;

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
            config: Arc::clone(&self.config),
            client,
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize MongoDB writer");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => return Err(e),
        };

        let mut handlers = Vec::new();

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                future::join_all(handlers).await;
                return Ok(());
            }

            match self.rx.recv().await {
                Some(event) => {
                    if Some(event.task_id) == event_handler.task_id.checked_sub(1) {
                        let event_handler = Arc::clone(&event_handler);
                        let retry_strategy = retry_config.strategy();

                        let handle = tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!(error = %e, "Failed to write MongoDB document");
                                            Err(tokio_retry::RetryError::transient(e))
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(
                                        error = %err,
                                        "MongoDB writer failed after all retry attempts"
                                    );
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );

                        handlers.push(handle);
                    }
                }
                None => {
                    // Channel closed, wait for all in-flight writes to complete.
                    future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

/// Builder for constructing Writer instances.
#[derive(Default)]
pub struct WriterBuilder {
    config: Option<Arc<super::config::Writer>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl WriterBuilder {
    pub fn new() -> WriterBuilder {
        WriterBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Writer>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets an optional downstream sender. Omit for terminal (leaf) writers.
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

    pub fn build(self) -> Result<Writer, Error> {
        Ok(Writer {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}

fn json_to_bson(value: &Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Bson::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                Bson::Null
            }
        }
        Value::String(s) => Bson::String(s.clone()),
        Value::Array(arr) => Bson::Array(arr.iter().map(json_to_bson).collect()),
        Value::Object(map) => {
            let doc: Document = map
                .iter()
                .map(|(k, v)| (k.clone(), json_to_bson(v)))
                .collect();
            Bson::Document(doc)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::Bson;
    use serde_json::json;
    use std::path::PathBuf;
    use tokio::sync::mpsc;

    /// Mock helper to generate a valid writer configuration module block
    fn create_mock_config() -> super::super::config::Writer {
        super::super::config::Writer {
            name: "test_mongo_writer".to_string(),
            db_name: "test_database".to_string(),
            collection_name: "test_collection".to_string(),
            credentials_path: PathBuf::from("/mock/path/credentials.json"),
            depends_on: Some(Vec::new()),
            retry: Default::default(),
        }
    }

    // ==========================================
    // 1. DATA CONVERSION & BSON PURE TESTS
    // ==========================================

    #[test]
    fn test_json_to_bson_primitives() {
        assert_eq!(json_to_bson(&json!(null)), Bson::Null);
        assert_eq!(json_to_bson(&json!(true)), Bson::Boolean(true));
        assert_eq!(json_to_bson(&json!(42)), Bson::Int64(42));
        assert_eq!(
            json_to_bson(&json!("flowgen")),
            Bson::String("flowgen".to_string())
        );
    }

    #[test]
    fn test_json_to_bson_complex_structures() {
        let json_data = json!({
            "meta": "data",
            "tags": ["rust", "async"],
            "nested": {
                "active": true
            }
        });

        let bson_doc = json_to_bson(&json_data);

        if let Bson::Document(doc) = bson_doc {
            assert_eq!(doc.get_str("meta").unwrap(), "data");

            let tags = doc.get_array("tags").unwrap();
            assert_eq!(tags.len(), 2);
            assert_eq!(tags[0].as_str().unwrap(), "rust");

            let nested = doc.get_document("nested").unwrap();
            assert!(nested.get_bool("active").unwrap());
        } else {
            panic!("Expected JSON Object serialization to yield a Bson::Document variant.");
        }
    }

    // ==========================================
    // 2. WRITER BUILDER VALIDATION TESTS
    // ==========================================

    #[test]
    fn test_writer_builder_missing_all_attributes() {
        let builder = WriterBuilder::new();
        let result = builder.build();

        assert!(result.is_err());
        if let Err(Error::MissingRequiredAttribute(attr)) = result {
            assert_eq!(attr, "config");
        } else {
            panic!("Expected MissingRequiredAttribute error variant for property 'config'");
        }
    }

    #[test]
    fn test_writer_builder_missing_receiver() {
        let config = Arc::new(create_mock_config());
        let builder = WriterBuilder::new().config(config);
        let result = builder.build();

        assert!(result.is_err());
        if let Err(Error::MissingRequiredAttribute(attr)) = result {
            assert_eq!(attr, "receiver");
        } else {
            panic!("Expected MissingRequiredAttribute error variant for property 'receiver'");
        }
    }

    #[test]
    fn test_writer_builder_missing_task_context() {
        let (_tx, rx) = mpsc::channel(1);
        let config = Arc::new(create_mock_config());

        let builder = WriterBuilder::new()
            .config(config)
            .receiver(rx)
            .task_type("mongo_write_task");

        let result = builder.build();
        assert!(result.is_err());
        if let Err(Error::MissingRequiredAttribute(attr)) = result {
            assert_eq!(attr, "task_context");
        } else {
            panic!("Expected MissingRequiredAttribute error variant for property 'task_context'");
        }
    }

    #[test]
    fn test_writer_builder_missing_task_type() {
        let (_tx, rx) = mpsc::channel(1);
        let config = Arc::new(create_mock_config());

        let builder = WriterBuilder::new().config(config).receiver(rx);

        let result = builder.build();
        assert!(result.is_err());
        if let Err(Error::MissingRequiredAttribute(attr)) = result {
            assert_eq!(attr, "task_context");
        } else {
            panic!("Expected MissingRequiredAttribute error variant for property 'task_context'");
        }
    }

    // ==========================================
    // 3. ERROR STRUCT TRAIT IMPLEMENTATION TESTS
    // ==========================================

    #[test]
    fn test_error_conversion_from_mongodb() {
        // Build a dummy structural Mongo Error to test out standard automated conversion traits
        let inner_mongo_err = mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "Target Mock Database Offline",
        ));

        let writer_error: Error = Error::from(inner_mongo_err);

        assert!(
            matches!(writer_error, Error::MongoWriter { .. }),
            "The error variant must map systematically down to Error::MongoWriter through From implementation overrides."
        );

        let error_message = format!("{}", writer_error);
        assert!(error_message.contains("Mongo Writer failed"));
    }
}
