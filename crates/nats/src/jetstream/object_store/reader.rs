use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use async_nats::jetstream::{context::ObjectStoreErrorKind, object_store::GetErrorKind};
use flowgen_core::{connect::client::Client, stream::event::Event, stream::event::EventBuilder};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use chrono::Utc;
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "nats.object.store.in";
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_HAS_HEADER: bool = true;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error authorizating to NATS client")]
    NatsClient(#[source] crate::client::Error),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("failed to get nats bucket")]
    NatsObjectStoreBucketError(#[source] async_nats::error::Error<ObjectStoreErrorKind>),
    #[error("error deserializing data into binary format")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("error reading file")]
    IO(#[source] std::io::Error),
    #[error("failed to get key value from nats storage")]
    NatsObjectStoreWatchError(#[source] async_nats::jetstream::object_store::WatchError),
    #[error("error constructing Flowgen Event")]
    Event(#[source] flowgen_core::stream::event::Error),
    #[error("failed to get nats bucket")]
    NatsObjectStoreFileError(#[source] async_nats::error::Error<GetErrorKind>),
    #[error("error with sending message over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
}

pub struct NatsObjectReader {
    config: Arc<super::config::Source>,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl NatsObjectReader {
    pub async fn subscribe(self) -> Result<(), Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials.clone().into())
            .build()
            .map_err(Error::NatsClient)?
            .connect()
            .await
            .map_err(Error::NatsClient)?;

        if let Some(jetstream) = client.jetstream {
            let bucket = jetstream.get_object_store(&self.config.bucket).await.map_err(Error::NatsObjectStoreBucketError)?;
            let mut objects_stream = bucket
                .list()
                .await
                .map_err(Error::NatsObjectStoreWatchError)?;

            while let Some(Ok(object)) = objects_stream.next().await {
                let file_name = object.name;

                print!("Object Name:; {}", file_name);

                // Fetch file from the bucket
                let mut nats_obj_file = bucket
                    .get(file_name.clone())
                    .await
                    .map_err(Error::NatsObjectStoreFileError)?;

                let mut buffer = vec![];
                nats_obj_file
                    .read_to_end(&mut buffer)
                    .await
                    .map_err(Error::IO)?;

                let (schema, _) = Format::default()
                    .with_header(true)
                    .infer_schema(&mut buffer.as_slice(), Some(100))
                    .map_err(Error::Arrow)?;

                let batch_size = match self.config.batch_size {
                        Some(batch_size) => batch_size,
                        None => DEFAULT_BATCH_SIZE,
                };
        
                let has_header = match self.config.has_header {
                        Some(has_header) => has_header,
                        None => DEFAULT_HAS_HEADER,
                };

                let csv = ReaderBuilder::new(Arc::new(schema.clone()))
                    .with_header(has_header)
                    .with_batch_size(batch_size)
                    .build(buffer.as_slice())
                    .map_err(Error::Arrow)?;

                for batch in csv {
                    let recordbatch = batch.map_err(Error::Arrow)?;
                    let timestamp = Utc::now().timestamp_micros();
                    let subject = format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, file_name, timestamp);
                    let e = EventBuilder::new()
                        .data(recordbatch)
                        .subject(subject)
                        .current_task_id(self.current_task_id)
                        .build()
                        .map_err(Error::Event)?;

                    self.tx.send(e.clone()).map_err(Error::SendMessage)?;
                    event!(Level::INFO, "event received: {}", e.subject);
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct NatsObjectReaderBuilder {
    config: Option<Arc<super::config::Source>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
}

impl NatsObjectReaderBuilder {
    pub fn new() -> NatsObjectReaderBuilder {
        NatsObjectReaderBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Source>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<NatsObjectReader, Error> {
        Ok(NatsObjectReader {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
