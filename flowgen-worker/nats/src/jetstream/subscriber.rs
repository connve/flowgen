use super::message::NatsMessageExt;
use async_nats::jetstream::{self};
use flowgen_core::{
    client::Client,
    event::{Event, DEFAULT_LOG_MESSAGE},
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast::Sender, time};
use tokio_stream::StreamExt;
use tracing::{event, Level};

/// Errors that can occur during NATS JetStream subscription operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Client authentication or connection error.
    #[error(transparent)]
    Client(#[from] crate::client::Error),
    /// Error converting message to flowgen event format.
    #[error(transparent)]
    MessageConversion(#[from] crate::jetstream::message::Error),
    /// JetStream consumer operation error.
    #[error(transparent)]
    Consumer(#[from] async_nats::jetstream::stream::ConsumerError),
    /// JetStream consumer stream error.
    #[error(transparent)]
    ConsumerStream(#[from] async_nats::jetstream::consumer::StreamError),
    /// Failed to get JetStream stream.
    #[error(transparent)]
    GetStream(#[from] async_nats::jetstream::context::GetStreamError),
    /// Failed to get JetStream stream information.
    #[error(transparent)]
    StreamInfo(#[from] async_nats::jetstream::stream::InfoError),
    /// Failed to retrieve consumer configuration information.
    #[error("Consumer configuration check failed")]
    ConsumerInfoFailed,
    /// Consumer exists with conflicting filter subject configuration.
    #[error("Consumer '{consumer}' exists with different filter subject '{existing}', expected '{expected}'. Please delete the existing consumer or use a different durable name")]
    ConsumerFilterMismatch {
        consumer: String,
        existing: String,
        expected: String,
    },
    /// Failed to subscribe to NATS subject.
    #[error(transparent)]
    Subscribe(#[from] async_nats::SubscribeError),
    /// Async task join error.
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    /// Failed to send event through broadcast channel.
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    /// Required configuration attribute is missing.
    #[error("Missing required attribute: {}.", _0)]
    MissingRequiredAttribute(String),
    /// General subscriber error for wrapped external errors.
    #[error("Other error with subscriber")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// NATS JetStream subscriber that consumes messages and converts them to flowgen events.
#[derive(Debug)]
pub struct Subscriber {
    /// Subscriber configuration including stream and consumer settings.
    config: Arc<super::config::Subscriber>,
    /// Sender for forwarding converted events.
    tx: Sender<Event>,
    /// Current task identifier for event tagging.
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Subscriber {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials.clone())
            .build()?
            .connect()
            .await?;

        if let Some(jetstream) = client.jetstream {
            let stream = jetstream.get_stream(self.config.stream.clone()).await?;

            let consumer_config = jetstream::consumer::pull::Config {
                durable_name: Some(self.config.durable_name.clone()),
                filter_subject: self.config.subject.clone(),
                ..Default::default()
            };

            let consumer = match stream.get_consumer(&self.config.durable_name).await {
                Ok(mut existing_consumer) => {
                    let consumer_info = existing_consumer.info().await?;
                    let current_filter = consumer_info.config.filter_subject.clone();

                    if current_filter != self.config.subject {
                        return Err(Error::ConsumerFilterMismatch {
                            consumer: self.config.durable_name.clone(),
                            existing: current_filter,
                            expected: self.config.subject.clone(),
                        });
                    } else {
                        existing_consumer
                    }
                }
                Err(_) => stream.create_consumer(consumer_config).await?,
            };

            loop {
                if let Some(delay_secs) = self.config.delay_secs {
                    time::sleep(Duration::from_secs(delay_secs)).await
                }

                let mut stream = consumer.messages().await?.take(self.config.batch_size);

                while let Some(message) = stream.next().await {
                    if let Ok(message) = message {
                        let mut e = message.to_event()?;
                        message.ack().await.map_err(Error::Other)?;
                        e.current_task_id = Some(self.current_task_id);

                        event!(Level::INFO, "{}: {}", DEFAULT_LOG_MESSAGE, e.subject);
                        self.tx.send(e)?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Builder for configuring and creating NATS JetStream subscribers.
#[derive(Default)]
pub struct SubscriberBuilder {
    /// Optional subscriber configuration.
    config: Option<Arc<super::config::Subscriber>>,
    /// Optional event sender.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event processing.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::sync::broadcast;

    #[test]
    fn test_error_display() {
        let err = Error::MissingRequiredAttribute("test_field".to_string());
        assert!(err
            .to_string()
            .contains("Missing required attribute: test_field"));

        let err = Error::Other(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "test error",
        )));
        assert!(err.to_string().contains("Other error with subscriber"));
    }

    #[test]
    fn test_subscriber_builder_new() {
        let builder = SubscriberBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[test]
    fn test_subscriber_builder_default() {
        let builder = SubscriberBuilder::default();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[test]
    fn test_subscriber_builder_config() {
        let config = Arc::new(super::super::config::Subscriber {
            credentials: PathBuf::from("/test/creds.jwt"),
            stream: "test_stream".to_string(),
            subject: "test.subject".to_string(),
            durable_name: "test_consumer".to_string(),
            batch_size: 100,
            delay_secs: Some(5),
        });

        let builder = SubscriberBuilder::new().config(config.clone());
        assert_eq!(builder.config, Some(config));
    }

    #[test]
    fn test_subscriber_builder_sender() {
        let (tx, _rx) = broadcast::channel(100);
        let builder = SubscriberBuilder::new().sender(tx);
        assert!(builder.tx.is_some());
    }

    #[test]
    fn test_subscriber_builder_current_task_id() {
        let builder = SubscriberBuilder::new().current_task_id(42);
        assert_eq!(builder.current_task_id, 42);
    }

    #[tokio::test]
    async fn test_subscriber_builder_build_missing_config() {
        let (tx, _rx) = broadcast::channel(100);
        let result = SubscriberBuilder::new()
            .sender(tx)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[tokio::test]
    async fn test_subscriber_builder_build_missing_sender() {
        let config = Arc::new(super::super::config::Subscriber {
            credentials: PathBuf::from("/test/creds.jwt"),
            stream: "test_stream".to_string(),
            subject: "test.subject".to_string(),
            durable_name: "test_consumer".to_string(),
            batch_size: 50,
            delay_secs: None,
        });

        let result = SubscriberBuilder::new()
            .config(config)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "sender")
        );
    }

    #[tokio::test]
    async fn test_subscriber_builder_build_success() {
        let config = Arc::new(super::super::config::Subscriber {
            credentials: PathBuf::from("/test/creds.jwt"),
            stream: "test_stream".to_string(),
            subject: "test.subject.*".to_string(),
            durable_name: "test_consumer".to_string(),
            batch_size: 25,
            delay_secs: Some(10),
        });
        let (tx, _rx) = broadcast::channel(100);

        let result = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx)
            .current_task_id(5)
            .build()
            .await;

        assert!(result.is_ok());
        let subscriber = result.unwrap();
        assert_eq!(subscriber.config, config);
        assert_eq!(subscriber.current_task_id, 5);
    }

    #[tokio::test]
    async fn test_subscriber_builder_chain() {
        let config = Arc::new(super::super::config::Subscriber {
            credentials: PathBuf::from("/chain/test.creds"),
            stream: "chain_stream".to_string(),
            subject: "chain.subject".to_string(),
            durable_name: "chain_consumer".to_string(),
            batch_size: 10,
            delay_secs: Some(1),
        });
        let (tx, _rx) = broadcast::channel(50);

        let subscriber = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx)
            .current_task_id(10)
            .build()
            .await
            .unwrap();

        assert_eq!(subscriber.config, config);
        assert_eq!(subscriber.current_task_id, 10);
    }

    #[test]
    fn test_subscriber_structure() {
        let config = Arc::new(super::super::config::Subscriber {
            credentials: PathBuf::from("/test/creds.jwt"),
            stream: "struct_test".to_string(),
            subject: "struct.test".to_string(),
            durable_name: "struct_consumer".to_string(),
            batch_size: 1,
            delay_secs: None,
        });
        let (tx, _rx) = broadcast::channel(1);

        let subscriber = Subscriber {
            config: config.clone(),
            tx,
            current_task_id: 0,
        };

        assert_eq!(subscriber.config, config);
        assert_eq!(subscriber.current_task_id, 0);
    }

    #[test]
    fn test_error_variants_added_for_consumer_management() {
        // Test that new error variants for consumer operations exist
        let consumer_err = Error::MissingRequiredAttribute("test".to_string());
        assert!(consumer_err
            .to_string()
            .contains("Missing required attribute"));

        // Test error display for comprehensive coverage
        let other_err = Error::Other(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "test",
        )));
        assert!(other_err
            .to_string()
            .contains("Other error with subscriber"));

        // Test consumer filter mismatch error
        let filter_err = Error::ConsumerFilterMismatch {
            consumer: "test_consumer".to_string(),
            existing: "old.subject".to_string(),
            expected: "new.subject".to_string(),
        };
        let err_msg = filter_err.to_string();
        assert!(err_msg.contains("test_consumer"));
        assert!(err_msg.contains("old.subject"));
        assert!(err_msg.contains("new.subject"));
        assert!(err_msg.contains("Please delete the existing consumer"));

        // Test consumer info failed error
        let info_err = Error::ConsumerInfoFailed;
        assert!(info_err
            .to_string()
            .contains("Consumer configuration check failed"));
    }
}
