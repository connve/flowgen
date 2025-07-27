//! Asynchronous CSV file reading with event-driven batch processing.
//!
//! Reads CSV files in configurable batches and emits events containing Arrow RecordBatch data.
//! Supports schema caching and concurrent processing of multiple files.

use arrow::{array::RecordBatch, csv::reader::Format, ipc::writer::StreamWriter};
use bytes::Bytes;
use chrono::Utc;
use flowgen_core::{
    cache::Cache,
    event::{Event, EventBuilder, EventData},
};
use std::{fs::File, io::Seek, sync::Arc};
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "file.reader";
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_HAS_HEADER: bool = true;

/// File reading errors.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
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
    Event(#[from] flowgen_core::event::Error),
    #[error("missing required event attribute")]
    MissingRequiredAttribute(String),
    #[error("cache errors")]
    Cache(),
}

/// Converts RecordBatch to bytes using Arrow IPC format.
pub trait RecordBatchConverter {
    type Error;
    /// Serializes RecordBatch to Arrow IPC bytes.
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

impl RecordBatchConverter for RecordBatch {
    type Error = Error;

    /// Serializes using Arrow IPC StreamWriter.
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let buffer: Vec<u8> = Vec::new();

        let mut stream_writer =
            StreamWriter::try_new(buffer, &self.schema()).map_err(Error::Arrow)?;
        stream_writer.write(self).map_err(Error::Arrow)?;
        stream_writer.finish().map_err(Error::Arrow)?;
        Ok(stream_writer.get_mut().to_vec())
    }
}

/// Processes file reading for individual events.
struct EventHandler<T: Cache> {
    cache: Arc<T>,
    tx: Sender<Event>,
    config: Arc<super::config::Reader>,
    current_task_id: usize,
}

impl<T: Cache> flowgen_core::task::runner::Runner for EventHandler<T> {
    type Error = Error;
    /// Reads CSV file in batches and emits events.
    async fn run(self) -> Result<(), Error> {
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

        let batch_size = self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let has_header = self.config.has_header.unwrap_or(DEFAULT_HAS_HEADER);
        let csv = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_header(has_header)
            .with_batch_size(batch_size)
            .build(file)
            .map_err(Error::Arrow)?;

        for batch in csv {
            let recordbatch = batch.map_err(Error::Arrow)?;
            let timestamp = Utc::now().timestamp_micros();
            let subject = match self.config.path.file_name().and_then(|f| f.to_str()) {
                Some(filename) => {
                    format!("{DEFAULT_MESSAGE_SUBJECT}.{filename}.{timestamp}")
                }
                None => format!("{DEFAULT_MESSAGE_SUBJECT}.{timestamp}"),
            };
            let event_message = format!("event processed: {subject}");

            let e = EventBuilder::new()
                .data(EventData::ArrowRecordBatch(recordbatch))
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
/// Event-driven file reader.
pub struct Reader<T: Cache> {
    config: Arc<super::config::Reader>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    cache: Arc<T>,
    current_task_id: usize,
}

impl<T: Cache> flowgen_core::task::runner::Runner for Reader<T> {
    type Error = Error;
    async fn run(mut self) -> Result<(), Error> {
        while let Ok(event) = self.rx.recv().await {
            // Process events from previous task only.
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

                tokio::spawn(async move {
                    if let Err(err) = event_handler.run().await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
    }
}

/// Builder for Reader instances.
#[derive(Default)]
pub struct ReaderBuilder<T> {
    config: Option<Arc<super::config::Reader>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    cache: Option<Arc<T>>,
    current_task_id: usize,
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

    pub fn config(mut self, config: Arc<super::config::Reader>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub fn cache(mut self, cache: Arc<T>) -> Self {
        self.cache = Some(cache);
        self
    }
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
