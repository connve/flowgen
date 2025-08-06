use arrow::csv::reader::Format;
use bytes::Bytes;
use chrono::{Utc};
use flowgen_core::cache::Cache;
use flowgen_core::{connect::client::Client, event::EventData};
use flowgen_core::event::{Event, EventBuilder};
use object_store::{GetResultPayload};
use serde_json::Value;
use std::io::{BufReader, Seek};
use std::{sync::Arc};
use tokio::sync::{broadcast::{Receiver, Sender}, Mutex};
use tracing::{event, Level};
use super::config::{DEFAULT_AVRO_EXTENSION, DEFAULT_JSON_EXTENSION, DEFAULT_CSV_EXTENSION};

/// Default subject prefix for logging messages.
const DEFAULT_MESSAGE_SUBJECT: &'static str = "object_store.reader";
/// Default batch size for files.
const DEFAULT_BATCH_SIZE: usize = 1000;
/// Default files have headers.
const DEFAULT_HAS_HEADER: bool = true;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
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
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("could not initialize object store context")]
    NoObjectStoreContext(),
    #[error("could not retrieve file extension")]
    NoFileExtension(),
    #[error("cache errors")]
    Cache(),
    
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
    async fn handle(self, _: Event) -> Result<(), Error> {
        let mut client_guard = self.client.lock().await;
        let context = client_guard
            .context
            .as_mut()
            .ok_or_else(Error::NoObjectStoreContext)?;

        let result = context
            .object_store
            .get(&context.path)
            .await?;

        let extension = result.meta.location.extension().ok_or_else(Error::NoFileExtension)?;

        let mut e = EventBuilder::new();
        match result.payload {
            GetResultPayload::File(mut file, _) => {
                  match extension {
                      DEFAULT_JSON_EXTENSION => {
                        let reader = BufReader::new(file);
                        let data: Value = serde_json::from_reader(reader)?;
                        e = e.data(EventData::Json(data));
                      }, 
                      DEFAULT_CSV_EXTENSION => {
                        let (schema, _) = Format::default()
                                .with_header(true)
                                .infer_schema(&mut file, Some(100))
                                .map_err(Error::Arrow)?;
                            file.rewind().map_err(Error::IO)?;

                        if let Some(cache_options) = &self.config.cache_options {
                            if let Some(insert_key) = &cache_options.insert_key {
                            let schema_string = serde_json::to_string(&schema)?;
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
                            
                            for data in csv {
                                e = e.data(EventData::ArrowRecordBatch(data?))
                            }
                      }
                      _ => {
                        todo!()
                      }
                  }

            },
            GetResultPayload::Stream(pin) => todo!(),
        }
        let timestamp = Utc::now().timestamp_micros();
        let subject = match &self.config.label {
                Some(label) => format!(
                    "{}.{}.{}",
                    DEFAULT_MESSAGE_SUBJECT,
                    label.to_lowercase(),
                    timestamp
                ),
                None => format!("{DEFAULT_MESSAGE_SUBJECT}.{timestamp}"),
            };
        let e = e.subject(subject.clone()).current_task_id(self.current_task_id).build()?;
        self.tx.send(e)?;
        event!(Level::INFO, "event processed: {}", DEFAULT_MESSAGE_SUBJECT);
        Ok(())
    }

}

/// Object store writer that processes events from a broadcast receiver.
pub struct Reader<T: Cache> {
    /// Writer configuration settings.
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

impl<T: Cache> flowgen_core::task::runner::Runner for Reader<T>{
    type Error = Error;

    async fn run(mut self) -> Result<(), Self::Error> {
        // Build object store client with conditional configuration
        let mut client_builder = super::client::ClientBuilder::new().path(self.config.path.clone());

        if let Some(options) = &self.config.client_options {
            client_builder = client_builder.options(options.clone());
        }
        if let Some(credentials) = &self.config.credentials {
            client_builder = client_builder.credentials(credentials.to_path_buf());
        }

        let client = Arc::new(Mutex::new(
            client_builder
                .build()?
                .connect()
                .await?,
        ));

        // Process incoming events, filtering by task ID.
        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let client = Arc::clone(&client);
                let config = Arc::clone(&self.config);
                let cache = Arc::clone(&self.cache);
                let tx = self.tx.clone();
                let current_task_id = self.current_task_id;
                let event_handler = EventHandler { client, config, tx, current_task_id, cache};
                tokio::spawn(async move {
                    if let Err(err) = event_handler.handle(event).await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
    
            }
        }
        Ok(())
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
}

impl<T: Cache> ReaderBuilder<T> 
where
    T: Default
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
