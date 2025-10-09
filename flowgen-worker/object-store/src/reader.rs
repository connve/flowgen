use super::config::{DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION};
use bytes::{Bytes, BytesMut};
use flowgen_core::buffer::{ContentType, FromReader};
use flowgen_core::cache::Cache;
use flowgen_core::event::{
    generate_subject, Event, EventBuilder, SubjectSuffix, DEFAULT_LOG_MESSAGE,
};
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

/// Default subject prefix for logging messages.
const DEFAULT_MESSAGE_SUBJECT: &str = "object_store_reader";
/// Default batch size for files.
const DEFAULT_BATCH_SIZE: usize = 1000;
/// Default files have headers.
const DEFAULT_HAS_HEADER: bool = true;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Input/output operation failed.
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    ObjectStoreClient(#[from] super::client::Error),
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Could not initialize object store context")]
    NoObjectStoreContext(),
    #[error("Could not retrieve file extension")]
    NoFileExtension(),
    #[error("Cache errors")]
    Cache(),
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
}

/// Handles processing of individual events by writing them to object storage.
struct EventHandler<T: Cache> {
    /// Writer configuration settings.
    config: Arc<super::config::Reader>,
    /// Object store client for writing data.
    client: Arc<Mutex<super::client::Client>>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Current task identifier for event filtering.
    current_task_id: usize,
    /// Cache object for storing / retriving data.
    cache: Arc<T>,
}

impl<T: Cache> EventHandler<T> {
    /// Processes an event and writes it to the configured object store.
    async fn handle(&self, _: Event) -> Result<(), Error> {
        let mut client_guard = self.client.lock().await;
        let context = client_guard
            .context
            .as_mut()
            .ok_or_else(Error::NoObjectStoreContext)?;

        let result = context.object_store.get(&context.path).await?;

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
                ContentType::Csv {
                    batch_size,
                    has_header,
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
                    let chunk = chunk?;
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
                        self.cache
                            .put(insert_key.as_str(), schema_bytes)
                            .await
                            .map_err(|_| Error::Cache())?;
                    }
                }
            }
        }

        // Send events.
        for event_data in event_data_list {
            let subject = generate_subject(
                Some(&self.config.name),
                DEFAULT_MESSAGE_SUBJECT,
                SubjectSuffix::Timestamp,
            );

            let e = EventBuilder::new()
                .subject(subject)
                .data(event_data)
                .current_task_id(self.current_task_id)
                .build()?;

            info!("{}: {}", DEFAULT_LOG_MESSAGE, e.subject);
            self.tx.send(e)?;
        }

        // Delete file from object store if configured.
        if self.config.delete_after_read.unwrap_or(false) {
            context.object_store.delete(&context.path).await?;
            info!("Successfully deleted file: {}", context.path.as_ref());
        }

        Ok(())
    }
}

/// Object store reader that processes events from a broadcast receiver.
#[derive(Debug)]
pub struct Reader<T: Cache> {
    /// Reader configuration settings.
    config: Arc<super::config::Reader>,
    /// Broadcast receiver for incoming events.
    rx: Receiver<Event>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Current task identifier for event filtering.
    current_task_id: usize,
    /// Cache object for storing / retriving data.
    cache: Arc<T>,
}

impl<T: Cache> flowgen_core::task::runner::Runner for Reader<T> {
    type Error = Error;

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.current_task_id))]
    async fn run(mut self) -> Result<(), Self::Error> {
        // Build object store client with conditional configuration
        let mut client_builder = super::client::ClientBuilder::new().path(self.config.path.clone());

        if let Some(options) = &self.config.client_options {
            client_builder = client_builder.options(options.clone());
        }
        if let Some(credentials) = &self.config.credentials {
            client_builder = client_builder.credentials(credentials.to_path_buf());
        }

        let client = Arc::new(Mutex::new(client_builder.build()?.connect().await?));

        let event_handler = Arc::new(EventHandler {
            client,
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            current_task_id: self.current_task_id,
            cache: Arc::clone(&self.cache),
        });

        // Process incoming events, filtering by task ID.
        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    if event.current_task_id == Some(self.current_task_id - 1) {
                        let handler = Arc::clone(&event_handler);
                        tokio::spawn(
                            async move {
                                if let Err(err) = handler.handle(event).await {
                                    error!("{}", err);
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );
                    }
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

/// Builder pattern for constructing Writer instances.
#[derive(Default)]
pub struct ReaderBuilder<T: Cache> {
    /// Writer configuration settings.
    config: Option<Arc<super::config::Reader>>,
    /// Broadcast receiver for incoming events.
    rx: Option<Receiver<Event>>,
    /// Event channel sender
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    current_task_id: usize,
    /// Cache object for storing / retriving data.
    cache: Option<Arc<T>>,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
}

impl<T: Cache> ReaderBuilder<T>
where
    T: Default,
{
    pub fn new() -> ReaderBuilder<T> {
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

    /// Sets the cache object.
    pub fn cache(mut self, cache: Arc<T>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Sets the current task identifier.
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// Builds the Writer instance, validating required fields.
    pub async fn build(self) -> Result<Reader<T>, Error> {
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
            cache: self
                .cache
                .ok_or_else(|| Error::MissingRequiredAttribute("cache".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowgen_core::cache::Cache;
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

    // Simple mock cache implementation for tests
    #[derive(Debug, Default)]
    struct TestCache {}

    impl TestCache {
        fn new() -> Self {
            Self {}
        }
    }

    impl Cache for TestCache {
        type Error = String;

        async fn init(self, _bucket: &str) -> Result<Self, Self::Error> {
            Ok(self)
        }

        async fn put(&self, _key: &str, _value: bytes::Bytes) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn get(&self, _key: &str) -> Result<bytes::Bytes, Self::Error> {
            Ok(bytes::Bytes::new())
        }
    }

    #[test]
    fn test_reader_builder_new() {
        let builder: ReaderBuilder<TestCache> = ReaderBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.rx.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.cache.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[test]
    fn test_reader_builder_config() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("s3://bucket/input/"),
            credentials: None,
            client_options: None,
            batch_size: Some(500),
            has_header: Some(true),
            cache_options: None,
            delete_after_read: None,
        });

        let builder: ReaderBuilder<TestCache> = ReaderBuilder::new().config(config.clone());
        assert!(builder.config.is_some());
        assert_eq!(
            builder.config.unwrap().path,
            PathBuf::from("s3://bucket/input/")
        );
    }

    #[test]
    fn test_reader_builder_receiver() {
        let (_, rx) = broadcast::channel::<Event>(10);
        let builder: ReaderBuilder<TestCache> = ReaderBuilder::new().receiver(rx);
        assert!(builder.rx.is_some());
    }

    #[test]
    fn test_reader_builder_sender() {
        let (tx, _) = broadcast::channel::<Event>(10);
        let builder: ReaderBuilder<TestCache> = ReaderBuilder::new().sender(tx);
        assert!(builder.tx.is_some());
    }

    #[test]
    fn test_reader_builder_cache() {
        let cache = std::sync::Arc::new(TestCache::new());
        let builder = ReaderBuilder::new().cache(cache);
        assert!(builder.cache.is_some());
    }

    #[test]
    fn test_reader_builder_current_task_id() {
        let builder: ReaderBuilder<TestCache> = ReaderBuilder::new().current_task_id(123);
        assert_eq!(builder.current_task_id, 123);
    }

    #[tokio::test]
    async fn test_reader_builder_missing_config() {
        let (tx, rx) = broadcast::channel::<Event>(10);
        let cache = std::sync::Arc::new(TestCache::new());

        let result = ReaderBuilder::<TestCache>::new()
            .receiver(rx)
            .sender(tx)
            .cache(cache)
            .current_task_id(1)
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
            credentials: None,
            client_options: None,
            batch_size: None,
            has_header: None,
            cache_options: None,
            delete_after_read: None,
        });

        let (tx, _) = broadcast::channel::<Event>(10);
        let cache = std::sync::Arc::new(TestCache::new());

        let result = ReaderBuilder::<TestCache>::new()
            .config(config)
            .sender(tx)
            .cache(cache)
            .current_task_id(1)
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
            credentials: Some(PathBuf::from("/creds.json")),
            client_options: None,
            batch_size: Some(1000),
            has_header: Some(false),
            cache_options: None,
            delete_after_read: None,
        });

        let (_, rx) = broadcast::channel::<Event>(10);
        let cache = std::sync::Arc::new(TestCache::new());

        let result = ReaderBuilder::<TestCache>::new()
            .config(config)
            .receiver(rx)
            .cache(cache)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "sender")
        );
    }

    #[tokio::test]
    async fn test_reader_builder_missing_cache() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("file:///local/files/"),
            credentials: None,
            client_options: None,
            batch_size: Some(250),
            has_header: Some(true),
            cache_options: None,
            delete_after_read: None,
        });

        let (tx, rx) = broadcast::channel::<Event>(10);

        let result = ReaderBuilder::<TestCache>::new()
            .config(config)
            .receiver(rx)
            .sender(tx)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "cache")
        );
    }

    #[tokio::test]
    async fn test_reader_builder_build_success() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("s3://my-bucket/files/"),
            credentials: Some(PathBuf::from("/aws-creds.json")),
            client_options: Some({
                let mut opts = std::collections::HashMap::new();
                opts.insert("region".to_string(), "us-west-2".to_string());
                opts
            }),
            batch_size: Some(2000),
            has_header: Some(true),
            cache_options: None,
            delete_after_read: None,
        });

        let (tx, rx) = broadcast::channel::<Event>(50);
        let cache = std::sync::Arc::new(TestCache::new());

        let result = ReaderBuilder::<TestCache>::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx)
            .cache(cache)
            .current_task_id(777)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_ok());
        let reader = result.unwrap();
        assert_eq!(reader.current_task_id, 777);
        assert_eq!(reader.config.path, PathBuf::from("s3://my-bucket/files/"));
    }

    #[test]
    fn test_reader_builder_chain() {
        let config = Arc::new(crate::config::Reader {
            name: "test_reader".to_string(),
            path: PathBuf::from("file:///data/input/"),
            credentials: None,
            client_options: None,
            batch_size: Some(100),
            has_header: Some(false),
            cache_options: None,
            delete_after_read: None,
        });

        let (tx, rx) = broadcast::channel::<Event>(5);
        let cache = std::sync::Arc::new(TestCache::new());

        let builder = ReaderBuilder::<TestCache>::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx)
            .cache(cache)
            .current_task_id(20);

        assert!(builder.config.is_some());
        assert!(builder.rx.is_some());
        assert!(builder.tx.is_some());
        assert!(builder.cache.is_some());
        assert_eq!(builder.current_task_id, 20);
    }
}
