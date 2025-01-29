use super::message::NatsMessageExt;
use flowgen_core::{client::Client, event::Event};
use futures_util::future::TryJoinAll;
use tokio::{sync::broadcast::Sender, task::JoinHandle};
use tokio_stream::StreamExt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error authorizating to Nats client.")]
    NatsClientAuth(#[source] crate::client::Error),
    #[error("There was an error with Nats JetStream Message.")]
    NatsJetStreamMessage(#[source] crate::jetstream::message::Error),
    #[error("There was an error subscriging to Nats subject.")]
    NatsSubscribe(#[source] async_nats::SubscribeError),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
}

pub struct Subscriber {
    handle_list: Vec<JoinHandle<Result<(), Error>>>,
}

impl Subscriber {
    pub async fn subscribe(self) -> Result<(), Error> {
        tokio::spawn(async move {
            let _ = self
                .handle_list
                .into_iter()
                .collect::<TryJoinAll<_>>()
                .await
                .map_err(Error::TokioJoin);
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

    pub async fn build(self) -> Result<Subscriber, Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

        let client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::NatsClientAuth)?
            .connect()
            .await
            .map_err(Error::NatsClientAuth)?;

        if let Some(client) = client.nats_client {
            let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                let mut subscriber = client
                    .subscribe(self.config.subject)
                    .await
                    .map_err(Error::NatsSubscribe)?;
                while let Some(message) = subscriber.next().await {
                    let mut e = message.to_event().map_err(Error::NatsJetStreamMessage)?;
                    e.current_task_id = Some(self.current_task_id);
                    self.tx.send(e).map_err(Error::TokioSendMessage)?;
                }
                Ok(())
            });
            handle_list.push(handle);
        }

        Ok(Subscriber { handle_list })
    }
}
