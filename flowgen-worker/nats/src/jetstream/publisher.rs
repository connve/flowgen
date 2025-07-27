use super::message::FlowgenMessageExt;
use async_nats::jetstream::stream::{Config, DiscardPolicy, RetentionPolicy};
use flowgen_core::connect::client::Client;
use flowgen_core::event::Event;
use std::{sync::Arc, time::Duration};
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{event, Level};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    NatsClientAuth(#[from] crate::client::Error),
    #[error(transparent)]
    NatsPublish(#[from] async_nats::jetstream::context::PublishError),
    #[error(transparent)]
    NatsCreateStream(#[from] async_nats::jetstream::context::CreateStreamError),
    #[error(transparent)]
    NatsGetStream(#[from] async_nats::jetstream::context::GetStreamError),
    #[error(transparent)]
    NatsRequest(#[from] async_nats::jetstream::context::RequestError),
    #[error(transparent)]
    NatsJetStreamEvent(#[from] super::message::Error),
    #[error("missing required event attribute")]
    MissingRequiredAttribute(String),
    #[error("Nats client is missing / not initialized properly")]
    MissingNatsClient(),
}

struct EventHandler {
    jetstream: Arc<Mutex<async_nats::jetstream::Context>>,
}

impl EventHandler {
    async fn handle(self, event: Event) -> Result<(), Error> {
        let e = event.to_publish().map_err(Error::NatsJetStreamEvent)?;

        self.jetstream
            .lock()
            .await
            .send_publish(event.subject.clone(), e)
            .await
            .map_err(Error::NatsPublish)?;

        event!(Level::INFO, "event processed: {}", event.subject);
        Ok(())
    }
}

pub struct Publisher {
    config: Arc<super::config::Publisher>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Publisher {
    type Error = Error;
    async fn run(mut self) -> Result<(), Self::Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials.clone())
            .build()
            .map_err(Error::NatsClientAuth)?
            .connect()
            .await
            .map_err(Error::NatsClientAuth)?;

        if let Some(jetstream) = client.jetstream {
            let mut max_age = 86400;
            if let Some(config_max_age) = self.config.max_age {
                max_age = config_max_age
            }

            let mut stream_config = Config {
                name: self.config.stream.clone(),
                description: self.config.stream_description.clone(),
                max_messages_per_subject: 1,
                subjects: self.config.subjects.clone(),
                discard: DiscardPolicy::Old,
                retention: RetentionPolicy::Limits,
                max_age: Duration::new(max_age, 0),
                ..Default::default()
            };

            let stream = jetstream.get_stream(self.config.stream.clone()).await;

            match stream {
                Ok(_) => {
                    let mut subjects = stream
                        .map_err(Error::NatsGetStream)?
                        .info()
                        .await
                        .map_err(Error::NatsRequest)?
                        .config
                        .subjects
                        .clone();

                    subjects.extend(self.config.subjects.clone());
                    subjects.sort();
                    subjects.dedup();
                    stream_config.subjects = subjects;

                    jetstream
                        .update_stream(stream_config)
                        .await
                        .map_err(Error::NatsCreateStream)?;
                }
                Err(_) => {
                    jetstream
                        .create_stream(stream_config)
                        .await
                        .map_err(Error::NatsCreateStream)?;
                }
            }

            let jetstream = Arc::new(Mutex::new(jetstream));

            while let Ok(event) = self.rx.recv().await {
                if event.current_task_id == Some(self.current_task_id - 1) {
                    let jetstream = Arc::clone(&jetstream);
                    let event_handler = EventHandler { jetstream };
                    // Spawn a new asynchronous task to handle event processing.
                    tokio::spawn(async move {
                        if let Err(err) = event_handler.handle(event).await {
                            event!(Level::ERROR, "{}", err);
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    config: Option<Arc<super::config::Publisher>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl PublisherBuilder {
    pub fn new() -> PublisherBuilder {
        PublisherBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Publisher>) -> Self {
        self.config = Some(config);
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

    pub async fn build(self) -> Result<Publisher, Error> {
        Ok(Publisher {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
