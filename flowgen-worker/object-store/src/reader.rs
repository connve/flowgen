use super::config::{DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION};
use bytes::{Bytes, BytesMut};
use flowgen_core::buffer::{ContentType, FromReader};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, SenderExt};
use flowgen_core::{client::Client, event::EventData};
use futures::StreamExt;
use object_store::GetResultPayload;
use std::io::{BufReader, Cursor};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{Receiver, Sender},
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
    #[error("Sending event to channel failed: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Reader event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("IO operation failed with error: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    #[error("Arrow operation failed with error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Avro operation failed with error: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    #[error("JSON serialization/deserialization failed with error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Object store operation failed with error: {source}")]
    ObjectStore {
        #[source]
        source: object_store::Error,
    },
    #[error("Object store client failed with error: {source}")]
    ObjectStoreClient {
        #[source]
        source: super::client::Error,
    },
    #[error("Configuration template rendering failed with error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Could not initialize object store context")]
    NoObjectStoreContext,
    #[error("Could not retrieve file extension")]
    NoFileExtension,
    #[error("Cache error")]
    Cache,
    #[error("Host coordination failed with error: {source}")]
    Host {
        #[source]
        source: flowgen_core::host::Error,
    },
    #[error("Invalid URL format with error: {source}")]
    ParseUrl {
        #[source]
        source: url::ParseError,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Handles processing of individual events by writing them to object storage.
pub struct EventHandler {
    /// Writer configuration settings.
    config: Arc<super::config::Reader>,
    /// Object store client for writing data.
    client: Arc<Mutex<super::client::Client>>,
    /// Channel sender for processed events
    tx: Option<Sender<Event>>,
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
            .ok_or_else(|| Error::NoObjectStoreContext)?;

        // Render config with to support templates inside configuration.
        let event_value = serde_json::value::Value::try_from(&event)
            .map_err(|source| Error::EventBuilder { source })?;
        let config = self
            .config
            .render(&event_value)
            .map_err(|source| Error::ConfigRender { source })?;

        // Parse the rendered path to extract just the path part (not the URL scheme/bucket)
        let config_path_str = config.path.to_string_lossy();
        let url = url::Url::parse(&config_path_str).map_err(|source| Error::ParseUrl { source })?;
        let path = object_store::path::Path::from(url.path());

        let result = context
            .object_store
            .get(&path)
            .await
            .map_err(|e| Error::ObjectStore { source: e })?;

        let extension = result
            .meta
            .location
            .extension()
            .ok_or_else(|| Error::NoFileExtension)?;

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
                EventData::from_reader(reader, content_type.clone())
                    .map_err(|source| Error::EventBuilder { source })?
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
                EventData::from_reader(reader, content_type.clone())
                    .map_err(|source| Error::EventBuilder { source })?
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
                                .map_err(|_| Error::Cache)?;
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
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            if let Some(ref tx) = self.tx {
                tx.send_with_logging(e)
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }
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

        let client = Arc::new(Mutex::new(
            client_builder
                .build()
                .map_err(|source| Error::ObjectStoreClient { source })?
                .connect()
                .await
                .map_err(|source| Error::ObjectStoreClient { source })?,
        ));

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
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

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

        // Process incoming events, filtering by task ID.
        loop {
            match self.rx.recv().await {
                Some(event) => {
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
                None => return Ok(()),
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
            tx: self.tx,
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
    use tokio::sync::mpsc;

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

    #[tokio::test]
    async fn test_reader_builder() {
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
            retry: None,
        });
        let (tx, rx) = mpsc::channel::<Event>(10);

        // Success case.
        let reader = ReaderBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(reader.is_ok());

        // Error case - missing config.
        let (tx2, rx2) = mpsc::channel::<Event>(10);
        let result = ReaderBuilder::new()
            .receiver(rx2)
            .sender(tx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingRequiredAttribute(_)
        ));
    }
}
