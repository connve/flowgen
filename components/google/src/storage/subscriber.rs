use flowgen_core::client::Client;
use futures_util::future::try_join_all;
use std::sync::Arc;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;

use super::v2::ReadObjectRequest;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error reading/writing/seeking file.")]
    InputOutput(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::mpsc::error::SendError<Vec<u8>>),
}

pub struct Subscriber {
    pub object: String,
    pub bucket: String,
    pub context: Arc<Mutex<super::context::Context>>,
    pub async_task_list: Option<Vec<JoinHandle<Result<(), Error>>>>,
    pub rx: Receiver<Vec<u8>>,
    tx: Sender<Vec<u8>>,
}

impl Subscriber {
    pub fn init(mut self) -> Result<Self, Error> {
        let mut async_task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let tx = self.tx.clone();
        let context = Arc::clone(&self.context);
        let object = self.object.clone();
        let bucket = self.bucket.clone();

        let subscribe_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            let mut stream = context
                .lock()
                .await
                .read_object(ReadObjectRequest {
                    object,
                    bucket,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();

            while let Some(received) = stream.next().await {
                match received {
                    Ok(resp) => {
                        let m: Vec<u8> = bincode::serialize(&resp).unwrap();
                        tx.send(m).await.unwrap();
                    }
                    Err(e) => {}
                }
            }
            Ok(())
        });

        async_task_list.push(subscribe_task);
        self.async_task_list = Some(async_task_list);

        Ok(self)
    }

    pub async fn subscribe(&mut self) -> Result<(), Error> {
        if let Some(async_task_list) = self.async_task_list.take() {
            try_join_all(async_task_list)
                .await
                .map_err(Error::TokioJoin)?
                .into_iter()
                .try_for_each(|result| result)?;
        }
        Ok(())
    }
}

/// A builder of the storage object reader.
pub struct Builder {
    service: flowgen_core::service::Service,
    config: super::config::Source,
}

impl Builder {
    /// Creates a new instance of a Builder.
    pub fn new(config: super::config::Source, service: flowgen_core::service::Service) -> Builder {
        Builder { config, service }
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        // Connect to GCP Cloud.
        let gcp_client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .unwrap()
            .connect()
            .await
            .unwrap();

        // Get PubSub context.
        let context = super::context::Builder::new(self.service)
            .with_client(gcp_client)
            .build()
            .unwrap();

        let context = Arc::new(Mutex::new(context));
        let (tx, rx) = tokio::sync::mpsc::channel(200);

        Ok(Subscriber {
            context,
            bucket: self.config.bucket,
            object: self.config.object,
            async_task_list: None,
            tx,
            rx,
        })
    }
}
