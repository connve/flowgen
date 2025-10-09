use super::message::FlowgenMessageExt;
use async_nats::jetstream::stream::{Config, DiscardPolicy, RetentionPolicy};
use flowgen_core::client::Client;
use flowgen_core::event::{Event, DEFAULT_LOG_MESSAGE};
use std::{sync::Arc, time::Duration};
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{event, Level};

/// Errors that can occur during NATS JetStream publishing operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Client authentication or connection error.
    #[error(transparent)]
    ClientAuth(#[from] crate::client::Error),
    /// Failed to publish message to JetStream.
    #[error(transparent)]
    Publish(#[from] async_nats::jetstream::context::PublishError),
    /// Failed to create JetStream stream.
    #[error(transparent)]
    CreateStream(#[from] async_nats::jetstream::context::CreateStreamError),
    /// Failed to get existing JetStream stream.
    #[error(transparent)]
    GetStream(#[from] async_nats::jetstream::context::GetStreamError),
    /// Failed to make request to JetStream.
    #[error(transparent)]
    Request(#[from] async_nats::jetstream::context::RequestError),
    /// Error converting event to message format.
    #[error(transparent)]
    MessageConversion(#[from] super::message::Error),
    /// Required event attribute is missing.
    #[error("Missing required event attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Client was not properly initialized or is missing.
    #[error("Client is missing or not initialized properly")]
    MissingClient(),
}

struct EventHandler {
    jetstream: Arc<Mutex<async_nats::jetstream::Context>>,
}

impl EventHandler {
    async fn handle(self, event: Event) -> Result<(), Error> {
        let e = event.to_publish()?;

        event!(Level::INFO, "{}: {}", DEFAULT_LOG_MESSAGE, event.subject);

        self.jetstream
            .lock()
            .await
            .send_publish(event.subject.clone(), e)
            .await?;
        Ok(())
    }
}

/// NATS JetStream publisher that receives events and publishes them to configured streams.
#[derive(Debug)]
pub struct Publisher {
    /// Publisher configuration including stream settings.
    config: Arc<super::config::Publisher>,
    /// Receiver for incoming events to publish.
    rx: Receiver<Event>,
    /// Current task identifier for event filtering.
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Publisher {
    type Error = Error;
    async fn run(mut self) -> Result<(), Self::Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials.clone())
            .build()?
            .connect()
            .await?;

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
                    let mut subjects = stream?.info().await?.config.subjects.clone();

                    subjects.extend(self.config.subjects.clone());
                    subjects.sort();
                    subjects.dedup();
                    stream_config.subjects = subjects;

                    jetstream.update_stream(stream_config).await?;
                }
                Err(_) => {
                    jetstream.create_stream(stream_config).await?;
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

/// Builder for configuring and creating NATS JetStream publishers.
#[derive(Default)]
pub struct PublisherBuilder {
    /// Optional publisher configuration.
    config: Option<Arc<super::config::Publisher>>,
    /// Optional event receiver.
    rx: Option<Receiver<Event>>,
    /// Current task identifier for event processing.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::sync::broadcast;


    #[test]
    fn test_publisher_builder_new() {
        let builder = PublisherBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.rx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[test]
    fn test_publisher_builder_default() {
        let builder = PublisherBuilder::default();
        assert!(builder.config.is_none());
        assert!(builder.rx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[test]
    fn test_publisher_builder_config() {
        let config = Arc::new(super::super::config::Publisher {
            credentials: PathBuf::from("/test/creds.jwt"),
            stream: "test_stream".to_string(),
            stream_description: Some("Test stream".to_string()),
            subjects: vec!["test.subject".to_string()],
            max_age: Some(3600),
        });

        let builder = PublisherBuilder::new().config(config.clone());
        assert_eq!(builder.config, Some(config));
    }

    #[test]
    fn test_publisher_builder_receiver() {
        let (_tx, rx) = broadcast::channel(100);
        let builder = PublisherBuilder::new().receiver(rx);
        assert!(builder.rx.is_some());
    }

    #[test]
    fn test_publisher_builder_current_task_id() {
        let builder = PublisherBuilder::new().current_task_id(42);
        assert_eq!(builder.current_task_id, 42);
    }

    #[tokio::test]
    async fn test_publisher_builder_build_missing_config() {
        let (_tx, rx) = broadcast::channel(100);
        let result = PublisherBuilder::new()
            .receiver(rx)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[tokio::test]
    async fn test_publisher_builder_build_missing_receiver() {
        let config = Arc::new(super::super::config::Publisher {
            credentials: PathBuf::from("/test/creds.jwt"),
            stream: "test_stream".to_string(),
            stream_description: None,
            subjects: vec!["test.subject".to_string()],
            max_age: None,
        });

        let result = PublisherBuilder::new()
            .config(config)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "receiver")
        );
    }

    #[tokio::test]
    async fn test_publisher_builder_build_success() {
        let config = Arc::new(super::super::config::Publisher {
            credentials: PathBuf::from("/test/creds.jwt"),
            stream: "test_stream".to_string(),
            stream_description: Some("Test description".to_string()),
            subjects: vec!["test.subject.1".to_string(), "test.subject.2".to_string()],
            max_age: Some(86400),
        });
        let (_tx, rx) = broadcast::channel(100);

        let result = PublisherBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .current_task_id(5)
            .build()
            .await;

        assert!(result.is_ok());
        let publisher = result.unwrap();
        assert_eq!(publisher.config, config);
        assert_eq!(publisher.current_task_id, 5);
    }

    #[tokio::test]
    async fn test_publisher_builder_chain() {
        let config = Arc::new(super::super::config::Publisher {
            credentials: PathBuf::from("/chain/test.creds"),
            stream: "chain_stream".to_string(),
            stream_description: None,
            subjects: vec!["chain.subject".to_string()],
            max_age: Some(1800),
        });
        let (_tx, rx) = broadcast::channel(50);

        let publisher = PublisherBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .current_task_id(10)
            .build()
            .await
            .unwrap();

        assert_eq!(publisher.config, config);
        assert_eq!(publisher.current_task_id, 10);
    }
}
