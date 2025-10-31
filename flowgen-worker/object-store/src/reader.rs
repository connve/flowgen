use super::config::{DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION};
use bytes::{Bytes, BytesMut};
use flowgen_core::buffer::{ContentType, FromReader};
use flowgen_core::event::{Event, EventBuilder, SenderExt};
use flowgen_core::{client::Client, event::EventData};
use futures::StreamExt;
use object_store::GetResultPayload;
use std::io::{BufReader, Cursor};
use std::sync::Arc;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    Mutex,
};
use tracing::{error, info, warn, Instrument};

/// Default batch size for files.
const DEFAULT_BATCH_SIZE: usize = 10000;
/// Default files have headers.
const DEFAULT_HAS_HEADER: bool = true;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Input/output operation failed.
    #[error("IO operation failed: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    /// Apache Arrow error.
    #[error("Arrow operation failed: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    /// Apache Avro error.
    #[error("Avro operation failed: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    /// JSON serialization/deserialization error.
    #[error("JSON serialization/deserialization failed: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    /// Object store operation error.
    #[error("Object store operation failed: {source}")]
    ObjectStore {
        #[source]
        source: object_store::Error,
    },
    /// Object store client error.
    #[error(transparent)]
    ObjectStoreClient(#[from] super::client::Error),
    /// Event building or processing error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Failed to send event message.
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    /// Configuration template rendering error.
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    /// Missing required attribute.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Could not initialize object store context.
    #[error("Could not initialize object store context")]
    NoObjectStoreContext(),
    /// Could not retrieve file extension.
    #[error("Could not retrieve file extension")]
    NoFileExtension(),
    /// Cache error.
    #[error("Cache error")]
    Cache(),
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
}

/// Handles processing of individual events by writing them to object storage.
pub struct EventHandler {
    /// Writer configuration settings.
    config: Arc<super::config::Reader>,
    /// Object store client for writing data.
    client: Arc<Mutex<super::client::Client>>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event and writes it to the configured object store.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        // Get cache from task context if available (not available for reader).
        let cache: Option<&Arc<dyn flowgen_core::cache::Cache>> = None;
        let mut client_guard = self.client.lock().await;
        let context = client_guard
            .context
            .as_mut()
            .ok_or_else(Error::NoObjectStoreContext)?;

        let result = context
            .object_store
            .get(&context.path)
            .await
            .map_err(|e| Error::ObjectStore { source: e })?;

        let extension = result
            .meta
            .location
            .extension()
            .ok_or_else(Error::NoFileExtension)?;

        // Determine content type from file extension.
        let content_type = match extension {
            DEFAULT_JSON_EXTENSION => ContentType::Json,
            DEFAULT_CSV_EXTENSION => {
                let batch_size = self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
                let has_header = self.config.has_header.unwrap_or(DEFAULT_HAS_HEADER);
                let delimiter = self
                    .config
                    .delimiter
                    .as_ref()
                    .and_then(|d| d.as_bytes().first().copied());
                ContentType::Csv {
                    batch_size,
                    has_header,
                    delimiter,
                }
            }
            DEFAULT_AVRO_EXTENSION => ContentType::Avro,
            _ => {
                warn!("Unsupported file extension: {}", extension);
                return Ok(());
            }
        };

        // Parse payload into event data based on type.
        let event_data_list = match result.payload {
            GetResultPayload::File(file, _) => {
                let reader = BufReader::new(file);
                EventData::from_reader(reader, content_type.clone())?
            }
            GetResultPayload::Stream(mut stream) => {
                // Collect stream into bytes.
                let mut buffer = BytesMut::new();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|e| Error::ObjectStore { source: e })?;
                    buffer.extend_from_slice(&chunk);
                }
                let bytes = buffer.freeze();

                // Parse bytes using Cursor for Seek support.
                let cursor = Cursor::new(bytes);
                let reader = BufReader::new(cursor);
                EventData::from_reader(reader, content_type.clone())?
            }
        };

        // Cache schema for CSV if configured.
        if let ContentType::Csv { .. } = content_type {
            if let Some(cache_options) = &self.config.cache_options {
                if let Some(insert_key) = &cache_options.insert_key {
                    if let Some(EventData::ArrowRecordBatch(batch)) = event_data_list.first() {
                        let schema_bytes = Bytes::from(batch.schema().to_string());
                        if let Some(cache) = cache {
                            cache
                                .put(insert_key.as_str(), schema_bytes)
                                .await
                                .map_err(|_| Error::Cache())?;
                        }
                    }
                }
            }
        }

        // Send events.
        for event_data in event_data_list {
            let e = EventBuilder::new()
                .subject(self.config.name.to_owned())
                .data(event_data)
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()?;

            self.tx
                .send_with_logging(e)
                .map_err(|source| Error::SendMessage { source })?;
        }

        // Delete file from object store if configured.
        if self.config.delete_after_read.unwrap_or(false) {
            context
                .object_store
                .delete(&context.path)
                .await
                .map_err(|e| Error::ObjectStore { source: e })?;
            info!("Successfully deleted file: {}", context.path.as_ref());
        }

        Ok(())
    }
}

/// Object store reader that processes events from a broadcast receiver.
#[derive(Debug)]
pub struct Reader {
    /// Reader configuration settings.
    config: Arc<super::config::Reader>,
    /// Broadcast receiver for incoming events.
    rx: Receiver<Event>,
    /// Channel sender for processed events
    tx: Sender<Event>,
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

    /// Initializes the reader by establishing object store client connection.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Building and connecting the object store client with credentials
    async fn init(&self) -> Result<EventHandler, Error> {
        // Build object store client with conditional configuration.
        let mut client_builder = super::client::ClientBuilder::new().path(self.config.path.clone());

        if let Some(options) = &self.config.client_options {
            client_builder = client_builder.options(options.clone());
        }
        if let Some(credentials_path) = &self.config.credentials_path {
            client_builder = client_builder.credentials_path(credentials_path.clone());
        }

        let client = Arc::new(Mutex::new(client_builder.build()?.connect().await?));

        let event_handler = EventHandler {
            client,
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        // Initialize runner task.
        let event_handler = match self.init().await {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!("{}", e);
                return Ok(());
            }
        };

        // Process incoming events, filtering by task ID.
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

/// Builder pattern for constructing Writer instances.
#[derive(Default)]
pub struct ReaderBuilder {
    /// Writer configuration settings.
    config: Option<Arc<super::config::Reader>>,
    /// Broadcast receiver for incoming events.
    rx: Option<Receiver<Event>>,
    /// Event channel sender
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl ReaderBuilder {
    pub fn new() -> ReaderBuilder {
        ReaderBuilder {
            ..Default::default()
        }
    }

    /// Sets the writer configuration.
    pub fn config(mut self, config: Arc<super::config::Reader>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the current task identifier.
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

    /// Builds the Writer instance, validating required fields.
    pub async fn build(self) -> Result<Reader, Error> {
        Ok(Reader {
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
    use serde_json::{Map, Value};
    use std::path::PathBuf;
    use tokio::sync::broadcast;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(flowgen_core::task::manager::TaskManagerBuilder::new().build());
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_reader_builder_new() {
        let builder = ReaderBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.rx.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.task_context.is_none());
        assert_eq!(builder.task_id, 0);
    }

    #[test]
    fn test_reader_builder_config() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("s3://bucket/input/"),
            credentials_path: None,
            client_options: None,
            batch_size: Some(500),
            has_header: Some(true),
            cache_options: None,
            delete_after_read: None,
            delimiter: None,
        });

        let builder = ReaderBuilder::new().config(config.clone());
        assert!(builder.config.is_some());
        assert_eq!(
            builder.config.unwrap().path,
            PathBuf::from("s3://bucket/input/")
        );
    }

    #[test]
    fn test_reader_builder_receiver() {
        let (_, rx) = broadcast::channel::<Event>(10);
        let builder = ReaderBuilder::new().receiver(rx);
        assert!(builder.rx.is_some());
    }

    #[test]
    fn test_reader_builder_sender() {
        let (tx, _) = broadcast::channel::<Event>(10);
        let builder = ReaderBuilder::new().sender(tx);
        assert!(builder.tx.is_some());
    }

    #[test]
    fn test_reader_builder_task_id() {
        let builder = ReaderBuilder::new().task_id(123);
        assert_eq!(builder.task_id, 123);
    }

    #[tokio::test]
    async fn test_reader_builder_missing_config() {
        let (tx, rx) = broadcast::channel::<Event>(10);

        let result = ReaderBuilder::new()
            .receiver(rx)
            .sender(tx)
            .task_context(create_mock_task_context())
            .task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[tokio::test]
    async fn test_reader_builder_missing_receiver() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("/tmp/input/"),
            credentials_path: None,
            client_options: None,
            batch_size: None,
            has_header: None,
            cache_options: None,
            delete_after_read: None,
            delimiter: None,
        });

        let (tx, _) = broadcast::channel::<Event>(10);

        let result = ReaderBuilder::new()
            .config(config)
            .sender(tx)
            .task_context(create_mock_task_context())
            .task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "receiver")
        );
    }

    #[tokio::test]
    async fn test_reader_builder_missing_sender() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("gs://bucket/data/"),
            credentials_path: Some(PathBuf::from("/creds.json")),
            client_options: None,
            batch_size: Some(1000),
            has_header: Some(false),
            cache_options: None,
            delete_after_read: None,
            delimiter: None,
        });

        let (_, rx) = broadcast::channel::<Event>(10);

        let result = ReaderBuilder::new()
            .config(config)
            .receiver(rx)
            .task_context(create_mock_task_context())
            .task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "sender")
        );
    }

    #[tokio::test]
    async fn test_reader_builder_build_success() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("s3://my-bucket/files/"),
            credentials_path: Some(PathBuf::from("/aws-creds.json")),
            client_options: Some({
                let mut opts = std::collections::HashMap::new();
                opts.insert("region".to_string(), "us-west-2".to_string());
                opts
            }),
            batch_size: Some(2000),
            has_header: Some(true),
            cache_options: None,
            delete_after_read: None,
            delimiter: None,
        });

        let (tx, rx) = broadcast::channel::<Event>(50);

        let result = ReaderBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx)
            .task_id(777)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_ok());
        let reader = result.unwrap();
        assert_eq!(reader.task_id, 777);
        assert_eq!(reader.config.path, PathBuf::from("s3://my-bucket/files/"));
    }

    #[test]
    fn test_reader_builder_chain() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("file:///data/input/"),
            credentials_path: None,
            client_options: None,
            batch_size: Some(100),
            has_header: Some(false),
            cache_options: None,
            delete_after_read: None,
            delimiter: None,
        });

        let (tx, rx) = broadcast::channel::<Event>(5);

        let builder = ReaderBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx)
            .task_id(20);

        assert!(builder.config.is_some());
        assert!(builder.rx.is_some());
        assert!(builder.tx.is_some());
        assert_eq!(builder.task_id, 20);
    }

    #[tokio::test]
    async fn test_reader_builder_build_missing_task_context() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("s3://bucket/input/"),
            credentials_path: None,
            client_options: None,
            batch_size: None,
            has_header: None,
            cache_options: None,
            delete_after_read: None,
            delimiter: None,
        });
        let (tx, rx) = broadcast::channel::<Event>(10);

        let result = ReaderBuilder::new()
            .config(config)
            .receiver(rx)
            .sender(tx)
            .task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "task_context")
        );
    }
}
