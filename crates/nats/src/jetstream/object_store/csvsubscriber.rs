use arrow::{
    array::RecordBatch,
    ipc::writer::StreamWriter,
};
use chrono::Utc;
use async_nats::jetstream::object_store::Config;
use std::sync::Arc;
use tokio::{sync::broadcast::Sender, task::JoinHandle};
use flowgen_core::{connect::client::Client, stream::event::Event};

const DEFAULT_FILE_NAME: &str = "no_file_name";

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
    #[error("error authorizating to NATS client")]
    NatsClient(#[source] crate::client::Error),
    #[error("failed to get nats bucket")]
    NatsObjectStoreBucketError(#[source] async_nats::jetstream::context::CreateKeyValueError),
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

pub struct Csvsubscriber {
    config: Arc<super::config::Source>,
}

impl Csvsubscriber {
    pub async fn subscribe(self) -> Result<(), Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {

            let timestamp = Utc::now().timestamp_micros();
            let path = self.config.path.clone();
            let mut file = tokio::fs::File::open(path.clone()).await.map_err(Error::IO)?;
            let filename = match self.config.path.split("/").last() {
                Some(filename) => {
                    format!("{}.{}.{}", filename.replace(".csv", ""), timestamp,"csv")
                }
                None => format!("{}.{}", DEFAULT_FILE_NAME, timestamp),
            };
            

            
            let client = crate::client::ClientBuilder::new()
                .credentials_path(self.config.credentials.clone().into())
                .build()
                .map_err(Error::NatsClient)?
                .connect()
                .await
                .map_err(Error::NatsClient)?;

            if let Some(jetstream) = client.jetstream {

                let bucket = jetstream.create_object_store(Config {
                    bucket: self.config.input_bucket.to_string(),
                   ..Default::default()
                }).await.map_err(Error::NatsObjectStoreBucketError)?;
                bucket.put( filename.as_str(), &mut file).await.unwrap();
        }
            Ok(())
        });
        handle_list.push(handle);


        Ok(())
    }
}

#[derive(Default)]
pub struct CSVSubscriberBuilder {
    config: Option<Arc<super::config::Source>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
}

impl CSVSubscriberBuilder {
    pub fn new() -> CSVSubscriberBuilder {
        CSVSubscriberBuilder {
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

    pub async fn build(self) -> Result<Csvsubscriber, Error> {
        Ok(Csvsubscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
        })
    }
}
