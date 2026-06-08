use super::config::{
    DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION,
    DEFAULT_PARQUET_EXTENSION,
};
use bytes::BytesMut;
use flowgen_core::buffer::{ContentType, FromReader};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventExt};
use flowgen_core::{client::Client, event::EventData};
use futures::StreamExt;
use object_store::{GetResultPayload, ObjectStoreExt};
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
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("IO error: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    #[error("Arrow error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Avro error: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Object store error: {source}")]
    ObjectStore {
        #[source]
        source: object_store::Error,
    },
    #[error("Object store client error: {source}")]
    ObjectStoreClient {
        #[source]
        source: super::client::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Could not initialize object store context")]
    NoObjectStoreContext,
    #[error("Could not retrieve file extension")]
    NoFileExtension,
    #[error("Invalid URL format: {source}")]
    ParseUrl {
        #[source]
        source: url::ParseError,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Object store read operation requires a path.")]
    MissingPath,
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// Handles processing of individual events by writing them to object storage.
pub struct EventHandler {
    /// Writer configuration settings.
    config: Arc<super::config::Processor>,
    /// Object store client for writing data.
    client: Arc<Mutex<super::client::Client>>,
    /// Channel sender for processed events
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    async fn flush_pending(
        &self,
        pending: Option<(Event, usize)>,
        had_data: bool,
        completion_tx_arc: &Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        if let Some((mut last, last_n)) = pending {
            match self.tx {
                None => {
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(last.data_as_json().ok());
                    }
                }
                Some(_) => {
                    last.completion_tx = completion_tx_arc.clone();
                }
            }
            last.send_with_logging(self.tx.as_ref())
                .context("num_records", last_n)
                .await
                .map_err(|source| Error::SendMessage { source })?;
        } else if !had_data {
            if let Some(arc) = completion_tx_arc.as_ref() {
                let upstream_leaf_share = self.task_context.leaf_count.max(1);
                for _ in 0..upstream_leaf_share {
                    arc.signal_completion(None);
                }
            }
        }
        Ok(())
    }

    /// Processes an event and writes it to the configured object store.
    #[tracing::instrument(skip(self, event), name = "task.handle")]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();
        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config to support templates inside configuration.
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Parse the rendered path to extract just the path part (not the URL scheme/bucket).
            let config_path = config.path.as_ref().ok_or(Error::MissingPath)?;
            let config_path_str = config_path.to_string_lossy();
            let url =
                url::Url::parse(&config_path_str).map_err(|source| Error::ParseUrl { source })?;
            let path = object_store::path::Path::from(url.path());

            // Automatically reconnects on auth failure to refresh expired credentials.
            let mut client_guard = self.client.lock().await;
            let mut result = {
                let context = client_guard
                    .context
                    .as_ref()
                    .ok_or_else(|| Error::NoObjectStoreContext)?;
                context.object_store.get(&path).await
            };

            // Retries once on authentication failure after reconnecting.
            if let Err(ref e) = result {
                if super::client::Client::is_auth_error(e) {
                    client_guard
                        .reconnect()
                        .await
                        .map_err(|source| Error::ObjectStoreClient { source })?;

                    let context = client_guard
                        .context
                        .as_ref()
                        .ok_or_else(|| Error::NoObjectStoreContext)?;
                    result = context.object_store.get(&path).await;
                }
            }

            let result = result.map_err(|e| Error::ObjectStore { source: e })?;

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
                        infer_schema_max_records: None,
                    }
                }
                DEFAULT_AVRO_EXTENSION => ContentType::Avro,
                DEFAULT_PARQUET_EXTENSION => {
                    let batch_size = self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
                    ContentType::Parquet { batch_size }
                }
                _ => {
                    warn!("Unsupported file extension: {}", extension);
                    return Ok(());
                }
            };

            // Materialise a seekable reader from the payload.
            // Both File and Stream paths need seek for CSV schema
            // inference and Parquet random access.
            let seekable_reader: BufReader<Cursor<bytes::Bytes>> = match result.payload {
                GetResultPayload::File(mut file, _) => {
                    let mut buf = Vec::new();
                    std::io::Read::read_to_end(&mut file, &mut buf)
                        .map_err(|source| Error::IO { source })?;
                    BufReader::new(Cursor::new(bytes::Bytes::from(buf)))
                }
                GetResultPayload::Stream(mut stream) => {
                    let mut buffer = BytesMut::new();
                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.map_err(|e| Error::ObjectStore { source: e })?;
                        buffer.extend_from_slice(&chunk);
                    }
                    BufReader::new(Cursor::new(buffer.freeze()))
                }
            };

            // Parse the file using the centralized format reader. The iterator
            // yields one item at a time so only one parsed item lives in memory
            // alongside the raw bytes.
            let iter = EventData::from_reader(seekable_reader, content_type)
                .map_err(|source| Error::EventBuilder { source })?;

            let mut pending: Option<(Event, usize)> = None;
            let mut had_data = false;

            for item_result in iter {
                let event_data = item_result.map_err(|source| Error::EventBuilder { source })?;
                let num_records = match &event_data {
                    EventData::ArrowRecordBatch(batch) => batch.num_rows(),
                    _ => 1,
                };
                had_data = true;

                if let Some((prev, prev_n)) = pending.take() {
                    prev.send_with_logging(self.tx.as_ref())
                        .context("num_records", prev_n)
                        .await
                        .map_err(|source| Error::SendMessage { source })?;
                }

                let e = EventBuilder::new()
                    .subject(self.config.name.to_owned())
                    .data(event_data)
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|source| Error::EventBuilder { source })?;

                pending = Some((e, num_records));
            }

            self.flush_pending(pending, had_data, &completion_tx_arc)
                .await?;

            // Delete file from object store if configured.
            if self.config.delete_after_read.unwrap_or(false) {
                let mut delete_result = {
                    let context = client_guard
                        .context
                        .as_ref()
                        .ok_or_else(|| Error::NoObjectStoreContext)?;
                    context.object_store.delete(&path).await
                };

                // Retries once on authentication failure after reconnecting.
                if let Err(ref e) = delete_result {
                    if super::client::Client::is_auth_error(e) {
                        client_guard
                            .reconnect()
                            .await
                            .map_err(|source| Error::ObjectStoreClient { source })?;

                        let context = client_guard
                            .context
                            .as_ref()
                            .ok_or_else(|| Error::NoObjectStoreContext)?;
                        delete_result = context.object_store.delete(&path).await;
                    }
                }

                delete_result.map_err(|e| Error::ObjectStore { source: e })?;
                info!("Successfully deleted file: {}", path.as_ref());
            }

            Ok(())
        })
        .await
    }
}

/// Object store read processor that processes events from a broadcast receiver.
#[derive(Debug)]
pub struct ReadProcessor {
    /// Read configuration settings.
    config: Arc<super::config::Processor>,
    /// Broadcast receiver for incoming events.
    rx: Receiver<Event>,
    /// Channel sender for processed events
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for ReadProcessor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the reader by establishing object store client connection.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Building and connecting the object store client with credentials
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let path = init_config.path.ok_or(Error::MissingPath)?;
        let credentials_path = self.config.credentials_path.clone();
        let client_options = self.config.client_options.clone();
        let client = self
            .task_context
            .client_registry
            .get_or_init(
                flowgen_core::client_registry::ClientKey::new(&(&path, &credentials_path)),
                || async {
                    let mut client_builder = super::client::ClientBuilder::new().path(path);
                    if let Some(options) = client_options {
                        client_builder = client_builder.options(options);
                    }
                    if let Some(credentials_path) = credentials_path {
                        client_builder = client_builder.credentials_path(credentials_path);
                    }
                    let client = client_builder
                        .build()
                        .map_err(|source| Error::ObjectStoreClient { source })?
                        .connect()
                        .await
                        .map_err(|source| Error::ObjectStoreClient { source })?;
                    Ok(Mutex::new(client))
                },
            )
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        let event_handler = EventHandler {
            client,
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize reader");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        // Process incoming events, filtering by task ID.
        let mut handlers = Vec::new();
        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    let handle = tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        error!(error = %e, "Failed to read object");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Read failed after all retry attempts");
                                // Emit error event downstream for error handling.
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                    handlers.push(handle);
                }
                None => {
                    futures_util::future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

/// Builder pattern for constructing ReadProcessor instances.
#[derive(Default)]
pub struct ReadProcessorBuilder {
    /// Read configuration settings.
    config: Option<Arc<super::config::Processor>>,
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

impl ReadProcessorBuilder {
    pub fn new() -> ReadProcessorBuilder {
        ReadProcessorBuilder {
            ..Default::default()
        }
    }

    /// Sets the writer configuration.
    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
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

    /// Builds the ReadProcessor instance, validating required fields.
    pub async fn build(self) -> Result<ReadProcessor, Error> {
        Ok(ReadProcessor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
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
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_reader_builder() {
        let config = Arc::new(crate::config::Processor {
            name: "test_reader".to_string(),
            operation: crate::config::Operation::Read,
            path: Some(PathBuf::from("s3://bucket/input/")),
            batch_size: Some(500),
            has_header: Some(true),
            ..Default::default()
        });
        let (tx, rx) = mpsc::channel::<Event>(10);

        // Success case.
        let reader = ReadProcessorBuilder::new()
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
        let result = ReadProcessorBuilder::new()
            .receiver(rx2)
            .sender(tx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }
}
