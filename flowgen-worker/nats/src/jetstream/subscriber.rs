use super::message::NatsMessageExt;
use async_nats::jetstream::{self};
use flowgen_core::{
    client::Client,
    event::{Event, SenderExt},
};
use std::sync::Arc;
use tokio::pin;
use tokio::{sync::mpsc::Sender, time};
use tokio_stream::StreamExt;
use tracing::{error, Instrument};

/// Errors that can occur during NATS JetStream subscription operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Sending event to channel failed: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("NATS client failed with error: {source}")]
    Client {
        #[source]
        source: crate::client::Error,
    },
    #[error("Message conversion failed with error: {source}")]
    MessageConversion {
        #[source]
        source: crate::jetstream::message::Error,
    },
    #[error("JetStream consumer operation failed with error: {source}")]
    Consumer {
        #[source]
        source: async_nats::jetstream::stream::ConsumerError,
    },
    #[error("JetStream consumer stream failed with error: {source}")]
    ConsumerStream {
        #[source]
        source: async_nats::jetstream::consumer::StreamError,
    },
    #[error("JetStream consumer batch fetch failed with error: {source}")]
    ConsumerBatch {
        #[source]
        source: async_nats::jetstream::consumer::pull::BatchError,
    },
    #[error("JetStream stream management failed with error: {source}")]
    StreamManagement {
        #[source]
        source: super::stream::Error,
    },
    #[error("Failed to get JetStream stream with error: {source}")]
    GetStream {
        #[source]
        source: async_nats::jetstream::context::GetStreamError,
    },
    #[error("Failed to subscribe to NATS subject with error: {source}")]
    Subscribe {
        #[source]
        source: async_nats::SubscribeError,
    },
    #[error("Consumer configuration check failed")]
    ConsumerInfoFailed,
    #[error("Consumer '{consumer}' exists with different filter subject '{existing}', expected '{expected}'. Please delete the existing consumer or use a different durable name")]
    ConsumerFilterMismatch {
        consumer: String,
        existing: String,
        expected: String,
    },
    #[error("Missing stream configuration")]
    MissingStreamConfig,
    #[error("Other subscriber error")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Stream ended unexpectedly, connection may have been lost")]
    StreamEnded,
}

/// Event handler for processing NATS messages.
pub struct EventHandler {
    consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    config: Arc<super::config::Subscriber>,
    task_type: &'static str,
}

impl EventHandler {
    /// Processes a single message result.
    async fn process_message(
        &self,
        message_result: Result<
            async_nats::jetstream::Message,
            Box<dyn std::error::Error + Send + Sync>,
        >,
    ) -> Result<(), Error> {
        match message_result {
            Ok(message) => {
                let e = message
                    .to_event(self.task_type, self.task_id)
                    .map_err(|source| Error::MessageConversion { source })?;

                message.ack().await.ok();

                if let Some(ref tx) = self.tx {
                    tx.send_with_logging(e)
                        .await
                        .map_err(|source| Error::SendMessage { source })?;
                }
                Ok(())
            }
            Err(err) => Err(Error::Other(err)),
        }
    }

    /// Processes messages from the NATS JetStream consumer.
    async fn handle(self) -> Result<(), Error> {
        loop {
            // Apply delay between batches if configured
            if let Some(delay) = self.config.delay {
                time::sleep(delay).await
            }

            // Fetch messages with / without max_messages setting.
            let messages = match self.config.max_messages {
                Some(max_messages) => self
                    .consumer
                    .fetch()
                    .max_messages(max_messages)
                    .messages()
                    .await
                    .map_err(|e| Error::ConsumerBatch { source: e })?,
                None => self
                    .consumer
                    .fetch()
                    .messages()
                    .await
                    .map_err(|e| Error::ConsumerBatch { source: e })?,
            };

            // Process all fetched messages with optional throttling.
            match self.config.throttle {
                Some(throttle_duration) => {
                    let throttled = messages.throttle(throttle_duration);
                    pin!(throttled);
                    while let Some(message_result) = throttled.next().await {
                        self.process_message(message_result).await?;
                    }
                }
                None => {
                    let mut messages = messages;
                    while let Some(message_result) = messages.next().await {
                        self.process_message(message_result).await?;
                    }
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
    tx: Option<Sender<Event>>,
    /// Task identifier for event tagging.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
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
            .url(self.config.url.clone())
            .build()
            .map_err(|source| Error::Client { source })?
            .connect()
            .await
            .map_err(|source| Error::Client { source })?;

        if let Some(jetstream) = client.jetstream {
            let stream_opts = self
                .config
                .stream
                .as_ref()
                .ok_or_else(|| Error::MissingStreamConfig)?;

            let jetstream = match stream_opts.create_or_update {
                true => super::stream::create_or_update_stream(jetstream, stream_opts)
                    .await
                    .map_err(|source| Error::StreamManagement { source })?,
                false => jetstream,
            };

            let stream = jetstream
                .get_stream(&stream_opts.name)
                .await
                .map_err(|source| Error::GetStream { source })?;

            let durable_name = self
                .config
                .durable_name
                .as_ref()
                .ok_or_else(|| Error::MissingRequiredAttribute("durable_name".to_string()))?;

            let consumer_config = jetstream::consumer::pull::Config {
                durable_name: Some(durable_name.clone()),
                filter_subject: self.config.subject.clone(),
                ..Default::default()
            };

            let consumer = match stream.get_consumer(durable_name).await {
                Ok(mut existing_consumer) => {
                    let consumer_info = existing_consumer
                        .info()
                        .await
                        .map_err(|_| Error::ConsumerInfoFailed)?;
                    let current_filter = consumer_info.config.filter_subject.clone();

                    if current_filter != self.config.subject {
                        return Err(Error::ConsumerFilterMismatch {
                            consumer: durable_name.clone(),
                            existing: current_filter,
                            expected: self.config.subject.clone(),
                        });
                    } else {
                        existing_consumer
                    }
                }
                Err(_) => stream
                    .create_consumer(consumer_config)
                    .await
                    .map_err(|e| Error::Consumer { source: e })?,
            };

            Ok(EventHandler {
                consumer,
                tx: self.tx.clone(),
                task_id: self.task_id,
                config: Arc::clone(&self.config),
                task_type: self.task_type,
            })
        } else {
            Err(Error::Other(
                "JetStream context not available".to_string().into(),
            ))
        }
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        tokio::spawn(
            async move {
                let result = tokio_retry::Retry::spawn(retry_config.strategy(), || async {
                    let event_handler = match self.init().await {
                        Ok(handler) => handler,
                        Err(e) => {
                            error!("{}", e);
                            return Err(e);
                        }
                    };

                    match event_handler.handle().await {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            error!("{}", e);
                            Err(e)
                        }
                    }
                })
                .await;

                if let Err(e) = result {
                    error!(
                        "{}",
                        Error::RetryExhausted {
                            source: Box::new(e)
                        }
                    );
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
    /// Task identifier for event processing.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization.
    task_type: Option<&'static str>,
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

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value};
    use std::{path::PathBuf, time::Duration};
    use tokio::sync::mpsc;

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

    #[tokio::test]
    async fn test_subscriber_builder() {
        let config = Arc::new(super::super::config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
            subject: "test.subject".to_string(),
            stream: Some(super::super::config::StreamOptions {
                name: "test_stream".to_string(),
                subjects: vec!["test.>".to_string()],
                create_or_update: true,
                retention: Some(super::super::config::RetentionPolicy::Limits),
                discard: Some(super::super::config::DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("test_consumer".to_string()),
            max_messages: Some(100),
            delay: Some(Duration::from_secs(5)),
            throttle: None,
            ..Default::default()
        });
        let (tx, _rx) = mpsc::channel(100);

        // Success case.
        let subscriber = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .task_id(1)
            .task_type("test_task")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(subscriber.is_ok());

        // Error case - missing config.
        let (tx2, _rx2) = mpsc::channel(100);
        let result = SubscriberBuilder::new()
            .sender(tx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingRequiredAttribute(_)
        ));
    }

    #[test]
    fn test_error_variants_added_for_consumer_management() {
        // Test that new error variants for consumer operations exist
        let consumer_err = Error::MissingRequiredAttribute("test".to_string());
        assert!(consumer_err
            .to_string()
            .contains("Missing required builder attribute"));

        // Test error display for comprehensive coverage
        let other_err = Error::Other(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "test",
        )));
        assert!(other_err.to_string().contains("Other subscriber error"));

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
            subject: "test.subject".to_string(),
            stream: Some(super::super::config::StreamOptions {
                name: "test_stream".to_string(),
                subjects: vec!["test.>".to_string()],
                create_or_update: true,
                retention: Some(super::super::config::RetentionPolicy::Limits),
                discard: Some(super::super::config::DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("test_consumer".to_string()),
            max_messages: Some(50),
            delay: None,
            throttle: None,
            ..Default::default()
        });
        let (tx, _rx) = mpsc::channel(100);

        let result = SubscriberBuilder::new()
            .config(config)
            .sender(tx)
            .task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "task_context")
        );
    }
}
