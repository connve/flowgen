use arrow::{array::RecordBatch, csv::reader::Format, ipc::writer::StreamWriter};
use bytes::Bytes;
use chrono::Utc;
use flowgen_core::{
    cache::Cache,
    stream::event::{Event, EventBuilder},
};
use std::{fs::File, io::Seek, sync::Arc};
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{event, Level};

/// Default subject prefix for file reader events.
const DEFAULT_MESSAGE_SUBJECT: &str = "file.reader";
/// Default batch size for processing records.
const DEFAULT_BATCH_SIZE: usize = 1000;
/// Default CSV header setting.
const DEFAULT_HAS_HEADER: bool = true;

/// File reader error types.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error(transparent)]
    Event(#[from] flowgen_core::stream::event::Error),
    #[error("missing required event attribute")]
    MissingRequiredAttribute(String),
    #[error("cache errors")]
    Cache(),
}

/// Converts RecordBatch to bytes.
pub trait RecordBatchConverter {
    type Error;
    /// Converts RecordBatch to bytes using Arrow IPC format.
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

impl RecordBatchConverter for RecordBatch {
    type Error = Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let buffer: Vec<u8> = Vec::new();

        // Create Arrow IPC stream writer.
        let mut stream_writer =
            StreamWriter::try_new(buffer, &self.schema()).map_err(Error::Arrow)?;
        stream_writer.write(self).map_err(Error::Arrow)?;
        stream_writer.finish().map_err(Error::Arrow)?;

        Ok(stream_writer.get_mut().to_vec())
    }
}

/// Handles file reading and event processing.
struct EventHandler<T: Cache> {
    /// Cache for replay IDs and schemas.
    cache: Arc<T>,
    /// Channel sender for processed events.
    tx: Sender<Event>,
    /// Subscriber configuration.
    config: Arc<super::config::Reader>,
    /// Task identifier for event tracking.
    current_task_id: usize,
}

impl<T: Cache> flowgen_core::task::runner::Runner for EventHandler<T> {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        // Open file and infer schema.
        let mut file = File::open(&self.config.path).map_err(Error::IO)?;
        let (schema, _) = Format::default()
            .with_header(true)
            .infer_schema(&mut file, Some(100))
            .map_err(Error::Arrow)?;
        file.rewind().map_err(Error::IO)?;

        if let Some(cache_options) = &self.config.cache_options {
            if let Some(insert_key) = &cache_options.insert_key {
                let schema_string = serde_json::to_string(&schema).map_err(Error::Serde)?;
                let schema_bytes = Bytes::from(schema_string);
                self.cache
                    .put(insert_key.as_str(), schema_bytes)
                    .await
                    .map_err(|_| Error::Cache())?;
            }
        };

        // Configure batch size and header settings.
        let batch_size = match self.config.batch_size {
            Some(batch_size) => batch_size,
            None => DEFAULT_BATCH_SIZE,
        };

        let has_header = match self.config.has_header {
            Some(has_header) => has_header,
            None => DEFAULT_HAS_HEADER,
        };

        // Create CSV reader.
        let csv = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_header(has_header)
            .with_batch_size(batch_size)
            .build(file)
            .map_err(Error::Arrow)?;

        // Process each batch.
        for batch in csv {
            let recordbatch = batch.map_err(Error::Arrow)?;
            let timestamp = Utc::now().timestamp_micros();
            // Generate event subject from filename.
            let subject = match &self.config.path.split("/").last() {
                Some(filename) => {
                    format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, filename, timestamp)
                }
                None => format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, timestamp),
            };
            let event_message = format!("event processed: {}", subject);

            // Create and send event.
            let e = EventBuilder::new()
                .data(recordbatch)
                .subject(subject)
                .current_task_id(self.current_task_id)
                .build()
                .map_err(Error::Event)?;

            self.tx.send(e).map_err(Error::SendMessage)?;
            event!(Level::INFO, "{}", event_message);
        }
        Ok(())
    }
}
/// File reader that processes events and spawns reading tasks.
pub struct Reader<T: Cache> {
    /// Reader configuration settings.
    config: Arc<super::config::Reader>,
    /// Channel sender for processed events.
    tx: Sender<Event>,
    /// Channel receiver for incoming events.
    rx: Receiver<Event>,
    /// Cache instance for storing schemas and replay data.
    cache: Arc<T>,
    /// Current task identifier.
    current_task_id: usize,
}

impl<T: Cache> flowgen_core::task::runner::Runner for Reader<T> {
    type Error = Error;
    async fn run(mut self) -> Result<(), Error> {
        // Process incoming events.
        while let Ok(event) = self.rx.recv().await {
            // Only process events from previous task.
            if event.current_task_id == Some(self.current_task_id - 1) {
                let config = Arc::clone(&self.config);
                let cache = Arc::clone(&self.cache);
                let tx = self.tx.clone();
                let event_handler = EventHandler {
                    cache,
                    config,
                    tx,
                    current_task_id: self.current_task_id,
                };

                // Spawn a new asynchronous task to handle the event processing.
                // This allows the main loop to continue receiving new events
                // while existing events are being written concurrently.
                tokio::spawn(async move {
                    // Process the events and in case of error log it.
                    if let Err(err) = event_handler.run().await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
    }
}

/// Builder for constructing Reader instances.
#[derive(Default)]
pub struct ReaderBuilder<T> {
    /// Optional reader configuration.
    config: Option<Arc<super::config::Reader>>,
    /// Optional event sender.
    tx: Option<Sender<Event>>,
    /// Optional event receiver.
    rx: Option<Receiver<Event>>,
    /// Optional cache instance.
    cache: Option<Arc<T>>,
    /// Task identifier for event processing.
    current_task_id: usize,
}

impl<T: Cache> ReaderBuilder<T>
where
    T: Default,
{
    /// Creates new ReaderBuilder.
    pub fn new() -> ReaderBuilder<T> {
        ReaderBuilder {
            ..Default::default()
        }
    }

    /// Sets reader configuration.
    pub fn config(mut self, config: Arc<super::config::Reader>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets event sender.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets event receiver.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets current task ID.
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    /// Sets cache instance.
    ///
    /// This method allows injecting a shared, thread-safe cache implementation
    /// that conforms to the `Cache` trait. The object will use this
    /// cache for its caching needs (e.g., storing or retrieving data).
    pub fn cache(mut self, cache: Arc<T>) -> Self {
        self.cache = Some(cache);
        self
    }
    /// Builds Reader instance.
    pub async fn build(self) -> Result<Reader<T>, Error> {
        Ok(Reader {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            cache: self
                .cache
                .ok_or_else(|| Error::MissingRequiredAttribute("cache".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
