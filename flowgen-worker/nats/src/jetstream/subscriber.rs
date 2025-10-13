use super::message::NatsMessageExt;
use async_nats::jetstream::{self};
use flowgen_core::{
    client::Client,
    event::{Event, DEFAULT_LOG_MESSAGE},
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast::Sender, time};
use tokio_stream::StreamExt;
use tracing::{error, info, Instrument};

/// Default subject prefix for NATS subscriber.
const DEFAULT_MESSAGE_SUBJECT: &str = "nats_jetstream_subscriber";

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
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
}

/// Event handler for processing NATS messages.
pub struct EventHandler {
    consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    tx: Sender<Event>,
    current_task_id: usize,
    config: Arc<super::config::Subscriber>,
}

impl EventHandler {
    /// Processes messages from the NATS JetStream consumer.
    async fn handle(self) -> Result<(), Error> {
        loop {
            if let Some(delay_secs) = self.config.delay_secs {
                time::sleep(Duration::from_secs(delay_secs)).await
            }

            let stream = self.consumer.messages().await?;
            let mut batch = stream.take(self.config.batch_size);

            while let Some(message) = batch.next().await {
                if let Ok(message) = message {
                    let mut e = message.to_event()?;
                    message.ack().await.ok();
                    e.current_task_id = Some(self.current_task_id);

                    info!("{}: {}", DEFAULT_LOG_MESSAGE, e.subject);
                    self.tx.send(e)?;
                }
            }
        }
    }
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
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Subscriber {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the subscriber by establishing connection and creating consumer.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Connecting to NATS with credentials
    /// - Getting or creating JetStream stream and consumer
    /// - Validating consumer configuration
    async fn init(&self) -> Result<EventHandler, Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials_path.clone())
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

            Ok(EventHandler {
                consumer,
                tx: self.tx.clone(),
                current_task_id: self.current_task_id,
                config: Arc::clone(&self.config),
            })
        } else {
            Err(Error::Other(
                "JetStream context not available".to_string().into(),
            ))
        }
    }

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.current_task_id))]
    async fn run(self) -> Result<(), Error> {
        // Initialize runner task.
        let event_handler = match self.init().await {
            Ok(handler) => handler,
            Err(e) => {
                error!("{}", e);
                return Ok(());
            }
        };

        // Spawn event handler task.
        tokio::spawn(
            async move {
                if let Err(e) = event_handler.handle().await {
                    error!("{}", e);
                }
            }
            .instrument(tracing::Span::current()),
        );

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
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
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

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
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
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value};
    use std::path::PathBuf;
    use tokio::sync::broadcast;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(flowgen_core::task::manager::TaskManagerBuilder::new().build());
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .build()
                .unwrap(),
        )
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
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
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
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
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
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
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
            .task_context(create_mock_task_context())
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
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("/chain/test.creds"),
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
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        assert_eq!(subscriber.config, config);
        assert_eq!(subscriber.current_task_id, 10);
    }

    #[test]
    fn test_subscriber_structure() {
        let config = Arc::new(super::super::config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
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
            _task_context: create_mock_task_context(),
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

    #[tokio::test]
    async fn test_subscriber_builder_build_missing_task_context() {
        let config = Arc::new(super::super::config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
            stream: "test_stream".to_string(),
            subject: "test.subject".to_string(),
            durable_name: "test_consumer".to_string(),
            batch_size: 50,
            delay_secs: None,
        });
        let (tx, _rx) = broadcast::channel(100);

        let result = SubscriberBuilder::new()
            .config(config)
            .sender(tx)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "task_context")
        );
    }
}
