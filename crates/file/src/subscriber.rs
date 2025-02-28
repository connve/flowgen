use arrow::{
    array::RecordBatch,
    csv::{reader::Format, ReaderBuilder},
    ipc::writer::StreamWriter,
};
use chrono::Utc;
use flowgen_core::event::{Event, EventBuilder};
use futures::future::TryJoinAll;
use std::{fs::File, io::Seek, sync::Arc};
use tokio::{sync::broadcast::Sender, task::JoinHandle};

#[derive(thiserror::Error, Debug)]
pub enum SubscriberError {
    #[error("There was an error reading/writing/seeking file.")]
    IOError(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    JoinError(#[source] tokio::task::JoinError),
    #[error("There was an error deserializing data into binary format.")]
    ArrowError(#[source] arrow::error::ArrowError),
    #[error("There was an error with sending message over channel.")]
    SendMessageError(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("There was an error constructing Flowgen Event.")]
    EventError(#[source] flowgen_core::event::EventError),
}

pub trait RecordBatchConverter {
    type Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

impl RecordBatchConverter for RecordBatch {
    type Error = SubscriberError;
    fn to_bytes(&self) -> Result<Vec<u8>, SubscriberError> {
        let buffer: Vec<u8> = Vec::new();

        let mut stream_writer =
            StreamWriter::try_new(buffer, &self.schema()).map_err(SubscriberError::ArrowError)?;
        stream_writer
            .write(self)
            .map_err(SubscriberError::ArrowError)?;
        stream_writer
            .finish()
            .map_err(SubscriberError::ArrowError)?;

        Ok(stream_writer.get_mut().to_vec())
    }
}

pub struct Subscriber {
    handle_list: Vec<JoinHandle<Result<(), SubscriberError>>>,
}

impl Subscriber {
    pub async fn subscribe(self) -> Result<(), SubscriberError> {
        tokio::spawn(async move {
            let _ = self
                .handle_list
                .into_iter()
                .collect::<TryJoinAll<_>>()
                .await
                .map_err(SubscriberError::JoinError);
        });
        Ok(())
    }
}

/// A builder of the file reader.
pub struct Builder {
    config: super::config::Source,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl Builder {
    /// Creates a new instance of a Builder.
    pub fn new(
        config: super::config::Source,
        tx: &Sender<Event>,
        current_task_id: usize,
    ) -> Builder {
        Builder {
            config,
            tx: tx.clone(),
            current_task_id,
        }
    }

    pub async fn build(self) -> Result<Subscriber, SubscriberError> {
        let mut handle_list: Vec<JoinHandle<Result<(), SubscriberError>>> = Vec::new();

        let handle: JoinHandle<Result<(), SubscriberError>> = tokio::spawn(async move {
            let mut file =
                File::open(self.config.path.clone()).map_err(SubscriberError::IOError)?;
            let (schema, _) = Format::default()
                .with_header(true)
                .infer_schema(&mut file, Some(100))
                .map_err(SubscriberError::ArrowError)?;
            file.rewind().map_err(SubscriberError::IOError)?;

            let csv = ReaderBuilder::new(Arc::new(schema.clone()))
                .with_header(true)
                .with_batch_size(1000)
                .build(file)
                .map_err(SubscriberError::ArrowError)?;

            for batch in csv {
                let recordbatch = batch.map_err(SubscriberError::ArrowError)?;
                let timestamp = Utc::now().timestamp_micros();
                let filename = match self.config.path.split("/").last() {
                    Some(filename) => filename,
                    None => break,
                };
                let subject = format!("{}.{}", filename, timestamp);

                let e = EventBuilder::new()
                    .data(recordbatch)
                    .subject(subject)
                    .current_task_id(self.current_task_id)
                    .build()
                    .map_err(SubscriberError::EventError)?;

                self.tx.send(e).map_err(SubscriberError::SendMessageError)?;
            }
            Ok(())
        });
        handle_list.push(handle);

        Ok(Subscriber { handle_list })
    }
}
