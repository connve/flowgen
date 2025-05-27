use arrow::{array::RecordBatch, csv::reader::Format, ipc::writer::StreamWriter};
use chrono::Utc;
use flowgen_core::{
    cache::Cache,
    stream::event::{Event, EventBuilder},
};
use futures::future::try_join_all;
use std::{fs::File, io::Seek, sync::Arc};
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "file.reader";
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_HAS_HEADER: bool = true;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error reading file")]
    IO(#[source] std::io::Error),
    #[error("error deserializing data into binary format")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("error with sending message over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("error constructing Flowgen Event")]
    Event(#[source] flowgen_core::stream::event::Error),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
}

pub trait RecordBatchConverter {
    type Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

impl RecordBatchConverter for RecordBatch {
    type Error = Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let buffer: Vec<u8> = Vec::new();

        let mut stream_writer =
            StreamWriter::try_new(buffer, &self.schema()).map_err(Error::Arrow)?;
        stream_writer.write(self).map_err(Error::Arrow)?;
        stream_writer.finish().map_err(Error::Arrow)?;

        Ok(stream_writer.get_mut().to_vec())
    }
}

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
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let config = Arc::clone(&self.config);
                let cache = Arc::clone(&self.cache);
                let tx = self.tx.clone();
                let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    let mut file = File::open(&config.path).unwrap();
                    let (schema, _) = Format::default()
                        .with_header(true)
                        .infer_schema(&mut file, Some(100))
                        .map_err(Error::Arrow)?;
                    file.rewind().map_err(Error::IO)?;

                    if config.cache_schema.is_some_and(|x| x) {
                        // cache.get();
                    }

                    let batch_size = match config.batch_size {
                        Some(batch_size) => batch_size,
                        None => DEFAULT_BATCH_SIZE,
                    };

                    let has_header = match config.has_header {
                        Some(has_header) => has_header,
                        None => DEFAULT_HAS_HEADER,
                    };

                    let csv = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
                        .with_header(has_header)
                        .with_batch_size(batch_size)
                        .build(file)
                        .map_err(Error::Arrow)?;

                    for batch in csv {
                        let recordbatch = batch.map_err(Error::Arrow)?;
                        let timestamp = Utc::now().timestamp_micros();
                        let subject = match &config.path.split("/").last() {
                            Some(filename) => {
                                format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, filename, timestamp)
                            }
                            None => format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, timestamp),
                        };
                        let event_message = format!("event processed: {}", subject);

                        let e = EventBuilder::new()
                            .data(recordbatch)
                            .subject(subject)
                            .current_task_id(self.current_task_id)
                            .build()
                            .map_err(Error::Event)?;

                        tx.send(e).map_err(Error::SendMessage)?;
                        event!(Level::INFO, "{}", event_message);
                    }
                    Ok(())
                });
                handle_list.push(handle);
            }
        }
        let _ = try_join_all(handle_list.iter_mut()).await;
        Ok(())
    }
}

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

    /// Sets the cache instance for this object.
    ///
    /// This method allows injecting a shared, thread-safe cache implementation
    /// that conforms to the `Cache` trait. The object will use this
    /// cache for its caching needs (e.g., storing or retrieving data).
    ///
    /// # Arguments
    /// * `cache` - An `Arc<dyn Cache<Error = Error>>` representing the shared cache instance.
    ///             The `Error` type must match the expected error type for cache operations
    ///             within this object.
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
