use super::message::NatsMessageExt;
use async_nats::jetstream::{self};
use flowgen_core::{connect::client::Client, stream::event::Event};
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast::Sender, time};
use tokio_stream::StreamExt;
use tracing::{event, Level};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    NatsClient(#[from] crate::client::Error),
    #[error(transparent)]
    NatsJetStreamMessage(#[from] crate::jetstream::message::Error),
    #[error(transparent)]
    NatsJetStreamConsumer(#[from] async_nats::jetstream::stream::ConsumerError),
    #[error(transparent)]
    NatsJetStream(#[from] async_nats::jetstream::consumer::StreamError),
    #[error(transparent)]
    NatsJetStreamGetStream(#[from] async_nats::jetstream::context::GetStreamError),
    #[error(transparent)]
    NatsSubscribe(#[from] async_nats::SubscribeError),
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("other error with subscriber")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub struct Subscriber {
    config: Arc<super::config::Subscriber>,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Subscriber {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials.clone())
            .build()
            .map_err(Error::NatsClient)?
            .connect()
            .await
            .map_err(Error::NatsClient)?;

        if let Some(jetstream) = client.jetstream {
            let consumer = jetstream
                .get_stream(self.config.stream.clone())
                .await
                .map_err(Error::NatsJetStreamGetStream)?
                .get_or_create_consumer(
                    &self.config.durable_name,
                    jetstream::consumer::pull::Config {
                        durable_name: Some(self.config.durable_name.clone()),
                        filter_subject: self.config.subject.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(Error::NatsJetStreamConsumer)?;

            loop {
                if let Some(delay_secs) = self.config.delay_secs {
                    time::sleep(Duration::from_secs(delay_secs)).await
                }

                let mut stream = consumer
                    .messages()
                    .await
                    .map_err(Error::NatsJetStream)?
                    .take(self.config.batch_size);

                while let Some(message) = stream.next().await {
                    if let Ok(message) = message {
                        let mut e = message.to_event().map_err(Error::NatsJetStreamMessage)?;
                        message.ack().await.map_err(Error::Other)?;
                        e.current_task_id = Some(self.current_task_id);

                        event!(Level::INFO, "event processed: {}", e.subject);
                        self.tx.send(e).map_err(Error::SendMessage)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct SubscriberBuilder {
    config: Option<Arc<super::config::Subscriber>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Subscriber>) -> Self {
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

    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
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
