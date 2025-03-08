use super::message::NatsMessageExt;
use async_nats::jetstream::{self};
use flowgen_core::{client::Client, event::Event};
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast::Sender, time};
use tokio_stream::StreamExt;
use tracing::{event, Level};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error authorizating to NATS client")]
    NatsClient(#[source] crate::client::Error),
    #[error("error with NATS JetStream Message")]
    NatsJetStreamMessage(#[source] crate::jetstream::message::Error),
    #[error("error with NATS JetStream durable consumer")]
    NatsJetStreamConsumer(#[source] async_nats::jetstream::stream::ConsumerError),
    #[error("error with NATS JetStream")]
    NatsJetStream(#[source] async_nats::jetstream::consumer::StreamError),
    #[error("error getting NATS JetStream")]
    NatsJetStreamGetStream(#[source] async_nats::jetstream::context::GetStreamError),
    #[error("error subscriging to NATS subject")]
    NatsSubscribe(#[source] async_nats::SubscribeError),
    #[error("error executing async task")]
    TaskJoin(#[source] tokio::task::JoinError),
    #[error("error with sending message over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("other error with subscriber")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub struct Subscriber {
    config: Arc<super::config::Source>,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl Subscriber {
    pub async fn subscribe(self) -> Result<(), Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials.clone().into())
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
    config: Option<Arc<super::config::Source>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
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
