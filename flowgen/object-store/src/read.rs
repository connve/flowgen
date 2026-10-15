use super::config::{
    DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION,
    DEFAULT_PARQUET_EXTENSION,
};
use flowgen_core::buffer::{ContentType, FromReader};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventExt};
use flowgen_core::{client::Client, event::EventData};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{GetOptions, GetRange, ObjectMeta, ObjectStoreExt};
use std::io::{BufReader, Cursor};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};
use tracing::{error, info, warn};

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
    /// Sends the previously held event and holds this one, so the last event can carry
    /// the completion handle.
    async fn emit(
        &self,
        pending: &mut Option<(Event, usize)>,
        data: EventData,
    ) -> Result<(), Error> {
        let records = match &data {
            EventData::ArrowRecordBatch(batch) => batch.num_rows(),
            _ => 1,
        };
        if let Some((previous, previous_records)) = pending.take() {
            previous
                .send_with_logging(self.tx.as_ref())
                .context("num_records", previous_records)
                .await
                .map_err(|source| Error::SendMessage { source })?;
        }
        let event = EventBuilder::new()
            .subject(self.config.name.to_owned())
            .data(data)
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;
        *pending = Some((event, records));
        Ok(())
    }

    /// Emits the CSV batch by batch while it downloads, so only one batch is in memory.
    async fn read_csv(
        &self,
        mut stream: BoxStream<'static, Result<bytes::Bytes, Error>>,
        mut decoder: arrow::csv::reader::Decoder,
        pending: &mut Option<(Event, usize)>,
    ) -> Result<(), Error> {
        let mut chunk = bytes::Bytes::new();
        let mut finished = false;
        loop {
            loop {
                if chunk.is_empty() && !finished {
                    match stream.next().await {
                        Some(next) => {
                            chunk = next?;
                            continue;
                        }
                        None => finished = true,
                    }
                }
                let decoded = decoder
                    .decode(&chunk)
                    .map_err(|source| Error::Arrow { source })?;
                bytes::Buf::advance(&mut chunk, decoded);
                if decoded == 0 || decoder.capacity() == 0 {
                    break;
                }
            }
            match decoder.flush().map_err(|source| Error::Arrow { source })? {
                Some(batch) => {
                    self.emit(pending, EventData::ArrowRecordBatch(batch))
                        .await?
                }
                None => return Ok(()),
            }
        }
    }

    async fn flush_pending(
        &self,
        pending: Option<(Event, usize)>,
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
        } else if let Some(arc) = completion_tx_arc.as_ref() {
            let upstream_leaf_share = self.task_context.leaf_count.max(1);
            for _ in 0..upstream_leaf_share {
                arc.signal_completion(None);
            }
        }
        Ok(())
    }
}

fn store(client: &super::client::Client) -> Result<&dyn object_store::ObjectStore, Error> {
    match client.context.as_ref() {
        Some(context) => Ok(context.object_store.as_ref()),
        None => Err(Error::NoObjectStoreContext),
    }
}

/// Runs `op` against the store, reconnecting once if the credentials expired.
async fn call<T>(
    client: &Mutex<super::client::Client>,
    op: impl for<'a> Fn(&'a dyn object_store::ObjectStore) -> BoxFuture<'a, object_store::Result<T>>,
) -> Result<T, Error> {
    let mut client = client.lock().await;
    match op(store(&client)?).await {
        Err(error) if super::client::Client::is_auth_error(&error) => {
            client
                .reconnect()
                .await
                .map_err(|source| Error::ObjectStoreClient { source })?;
            op(store(&client)?)
                .await
                .map_err(|source| Error::ObjectStore { source })
        }
        result => result.map_err(|source| Error::ObjectStore { source }),
    }
}

/// Bytes per ranged GET while reading a CSV, so a slow consumer never keeps a
/// request open long enough to hit the client timeout.
const CSV_RANGE_BYTES: u64 = 8 * 1024 * 1024;

/// The object as consecutive ranged GETs, each fetched once the previous one
/// is used up and pinned to the version in `meta`, so both CSV passes read the
/// same object.
fn ranges(
    client: Arc<Mutex<super::client::Client>>,
    meta: ObjectMeta,
) -> BoxStream<'static, Result<bytes::Bytes, Error>> {
    futures::stream::try_unfold(0, move |start| {
        let client = Arc::clone(&client);
        let meta = meta.clone();
        async move {
            if start >= meta.size {
                return Ok(None);
            }
            let end = (start + CSV_RANGE_BYTES).min(meta.size);
            let options = GetOptions {
                range: Some(GetRange::Bounded(start..end)),
                if_match: meta.e_tag.clone(),
                version: meta.version.clone(),
                ..Default::default()
            };
            let result = call(&client, |store| {
                let location = meta.location.clone();
                let options = options.clone();
                Box::pin(async move { store.get_opts(&location, options).await })
            })
            .await?;
            let bytes = result
                .bytes()
                .await
                .map_err(|source| Error::ObjectStore { source })?;
            Ok(Some((bytes, end)))
        }
    })
    .boxed()
}

/// Infers the CSV schema from every row while the file downloads, without
/// keeping the file in memory.
async fn infer_csv_schema(
    stream: BoxStream<'static, Result<bytes::Bytes, Error>>,
    format: arrow::csv::reader::Format,
) -> Result<arrow::datatypes::Schema, Error> {
    let stream = futures::TryStreamExt::map_err(stream, std::io::Error::other);
    let reader = tokio_util::io::SyncIoBridge::new(tokio_util::io::StreamReader::new(stream));
    match tokio::task::spawn_blocking(move || format.infer_schema(reader, None)).await {
        Ok(Ok((schema, _))) => Ok(schema),
        Ok(Err(source)) => Err(Error::Arrow { source }),
        Err(source) => Err(Error::IO {
            source: std::io::Error::other(source),
        }),
    }
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();
        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config to support templates inside configuration.
            let config = self
                .config
                .render_for_event(event.as_ref())
                .map_err(|source| Error::ConfigRender { source })?;

            // Parse the rendered path to extract just the path part (not the URL scheme/bucket).
            let config_path = config.path.as_ref().ok_or(Error::MissingPath)?;
            let config_path_str = config_path.to_string_lossy();
            let url =
                url::Url::parse(&config_path_str).map_err(|source| Error::ParseUrl { source })?;
            let path = object_store::path::Path::from(url.path());

            let extension = path.extension().ok_or(Error::NoFileExtension)?;

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

            let mut pending = None;
            match content_type {
                ContentType::Csv {
                    batch_size,
                    has_header,
                    delimiter,
                    ..
                } => {
                    let format = arrow::csv::reader::Format::default()
                        .with_header(has_header)
                        .with_delimiter(delimiter.unwrap_or(b','));
                    let meta = call(&self.client, |store| {
                        let path = path.clone();
                        Box::pin(async move { store.head(&path).await })
                    })
                    .await?;
                    let schema =
                        infer_csv_schema(ranges(Arc::clone(&self.client), meta.clone()), format)
                            .await?;
                    let decoder = arrow::csv::ReaderBuilder::new(Arc::new(schema))
                        .with_header(has_header)
                        .with_delimiter(delimiter.unwrap_or(b','))
                        .with_batch_size(batch_size)
                        .build_decoder();
                    self.read_csv(
                        ranges(Arc::clone(&self.client), meta),
                        decoder,
                        &mut pending,
                    )
                    .await?;
                }
                content_type => {
                    let bytes = call(&self.client, |store| {
                        let path = path.clone();
                        Box::pin(async move { store.get(&path).await })
                    })
                    .await?
                    .bytes()
                    .await
                    .map_err(|source| Error::ObjectStore { source })?;
                    let items =
                        EventData::from_reader(BufReader::new(Cursor::new(bytes)), content_type)
                            .map_err(|source| Error::EventBuilder { source })?;
                    for item in items {
                        let data = item.map_err(|source| Error::EventBuilder { source })?;
                        self.emit(&mut pending, data).await?;
                    }
                }
            }

            self.flush_pending(pending, &completion_tx_arc).await?;

            if self.config.delete_after_read.unwrap_or(false) {
                call(&self.client, |store| {
                    let path = path.clone();
                    Box::pin(async move { store.delete(&path).await })
                })
                .await?;
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
                flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
                    .field("path", &path)
                    .field("credentials_path", &credentials_path)
                    .build(),
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
    async fn run(self) -> Result<(), Self::Error> {
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

        flowgen_core::task::runner::process_events(
            self.rx,
            event_handler,
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
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

    #[tokio::test]
    async fn a_csv_file_streams_in_batches_typed_by_every_record() {
        let batches = read_csv_file("id,value\n1,1\n2,2\n3,2.5", 2).await;

        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.num_rows())
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(
            batches[0]
                .schema()
                .field_with_name("value")
                .unwrap()
                .data_type(),
            &arrow::datatypes::DataType::Float64
        );
    }

    #[tokio::test]
    async fn rows_cut_by_a_range_boundary_are_read_whole() {
        let row = format!("{}\n", "a".repeat(99));
        let rows = (CSV_RANGE_BYTES as usize / row.len()) + 100;
        let csv = format!("value\n{}", row.repeat(rows));

        let batches = read_csv_file(&csv, 10_000).await;

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            rows
        );
    }

    async fn read_csv_file(content: &str, batch_size: usize) -> Vec<arrow::array::RecordBatch> {
        use flowgen_core::task::runner::Handler;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.csv");
        std::fs::write(&file, content).unwrap();
        let path = PathBuf::from(format!("file://{}", file.display()));
        let client = crate::client::ClientBuilder::new()
            .path(path.clone())
            .build()
            .unwrap()
            .connect()
            .await
            .unwrap();
        let (tx, mut rx) = mpsc::channel(10);
        let handler = EventHandler {
            config: Arc::new(crate::config::Processor {
                name: "read".to_string(),
                operation: crate::config::Operation::Read,
                path: Some(path),
                batch_size: Some(batch_size),
                ..Default::default()
            }),
            client: Arc::new(Mutex::new(client)),
            tx: Some(tx),
            task_id: 0,
            task_type: "test",
            task_context: create_mock_task_context(),
        };
        let event = EventBuilder::new()
            .data(EventData::Json(Value::Null))
            .subject("a".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();
        let collected = tokio::spawn(async move {
            let mut batches = Vec::new();
            while let Some(event) = rx.recv().await {
                if let EventData::ArrowRecordBatch(batch) = event.data {
                    batches.push(batch);
                }
            }
            batches
        });

        handler.handle(event).await.unwrap();
        drop(handler);
        collected.await.unwrap()
    }
}
