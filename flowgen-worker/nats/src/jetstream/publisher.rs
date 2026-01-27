use super::message::FlowgenMessageExt;
use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, SenderExt};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};
use tracing::{error, Instrument};

/// Serializable representation of a NATS JetStream publish acknowledgment.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PublishAck {
    pub stream: String,
    pub sequence: u64,
    pub domain: Option<String>,
    pub duplicate: bool,
}

impl From<async_nats::jetstream::publish::PublishAck> for PublishAck {
    fn from(ack: async_nats::jetstream::publish::PublishAck) -> Self {
        Self {
            stream: ack.stream,
            sequence: ack.sequence,
            domain: Some(ack.domain),
            duplicate: ack.duplicate,
        }
    }
}

/// Errors that can occur during NATS JetStream publishing operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Sending event to channel failed: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Publisher event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("NATS client failed with error: {source}")]
    ClientAuth {
        #[source]
        source: crate::client::Error,
    },
    #[error("Failed to publish message to JetStream with error: {source}")]
    Publish {
        #[source]
        source: async_nats::jetstream::context::PublishError,
    },
    #[error("Stream management failed with error: {source}")]
    Stream {
        #[source]
        source: super::stream::Error,
    },
    #[error("Message conversion failed with error: {source}")]
    MessageConversion {
        #[source]
        source: super::message::Error,
    },
    #[error("JSON serialization failed with error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Configuration template rendering failed with error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Host coordination failed with error: {source}")]
    Host {
        #[source]
        source: flowgen_core::host::Error,
    },
    #[error("Stream configuration is missing")]
    NoStream,
    #[error("Client is missing or not initialized properly")]
    MissingClient,
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

pub struct EventHandler {
    jetstream: Arc<Mutex<async_nats::jetstream::Context>>,
    task_id: usize,
    tx: Sender<Event>,
    config: Arc<super::config::Publisher>,
    task_type: &'static str,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        // Render config with to support templates inside configuration.
        let event_value = serde_json::value::Value::try_from(&event)
            .map_err(|source| Error::EventBuilder { source })?;
        let config = self
            .config
            .render(&event_value)
            .map_err(|source| Error::ConfigRender { source })?;

        let e = event
            .to_publish()
            .map_err(|source| Error::MessageConversion { source })?;

        let ack_future = self
            .jetstream
            .lock()
            .await
            .send_publish(config.subject, e)
            .await
            .map_err(|e| Error::Publish { source: e })?;

        let ack = ack_future.await.map_err(|e| Error::Publish { source: e })?;
        let ack: PublishAck = ack.into();
        let ack_json = serde_json::to_value(&ack).map_err(|e| Error::SerdeJson { source: e })?;

        let e = EventBuilder::new()
            .subject(self.config.name.clone())
            .data(EventData::Json(ack_json))
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        self.tx
            .send_with_logging(e)
            .await
            .map_err(|source| Error::SendMessage { source })?;

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
    /// Channel sender for response events.
    tx: Sender<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Publisher {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the publisher by establishing connection and creating/updating stream.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Connecting to NATS with credentials
    /// - Creating or updating JetStream stream
    async fn init(&self) -> Result<EventHandler, Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials_path.clone())
            .url(self.config.url.clone())
            .build()
            .map_err(|source| Error::ClientAuth { source })?
            .connect()
            .await
            .map_err(|source| Error::ClientAuth { source })?;

        if let Some(jetstream) = client.jetstream {
            let stream_opts = self.config.stream.as_ref().ok_or_else(|| Error::NoStream)?;

            let jetstream = match stream_opts.create_or_update {
                true => super::stream::create_or_update_stream(jetstream, stream_opts)
                    .await
                    .map_err(|source| Error::Stream { source })?,
                false => jetstream,
            };

            let jetstream = Arc::new(Mutex::new(jetstream));
            let event_handler = EventHandler {
                jetstream,
                task_id: self.task_id,
                tx: self.tx.clone(),
                config: Arc::clone(&self.config),
                task_type: self.task_type,
            };

            Ok(event_handler)
        } else {
            Err(Error::MissingClient)
        }
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(
                    "{}",
                    Error::RetryExhausted {
                        source: Box::new(e)
                    }
                );
                return Ok(());
            }
        };

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        error!("{}", e);
                                        Err(e)
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(
                                    "{}",
                                    Error::RetryExhausted {
                                        source: Box::new(err)
                                    }
                                );
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                None => return Ok(()),
            }
        }
    }
}

/// Builder for configuring and creating NATS JetStream publishers.
#[derive(Default)]
pub struct PublisherBuilder {
    /// Optional publisher configuration.
    config: Option<Arc<super::config::Publisher>>,
    /// Optional event receiver.
    rx: Option<Receiver<Event>>,
    /// Optional event sender.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event processing.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
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

    pub async fn build(self) -> Result<Publisher, Error> {
        Ok(Publisher {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
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
    use std::path::PathBuf;
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
    async fn test_publisher_builder() {
        let config = Arc::new(super::super::config::Publisher {
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
            subject: "test.subject".to_string(),
            stream: Some(super::super::config::StreamOptions {
                name: "test_stream".to_string(),
                description: Some("Test stream".to_string()),
                subjects: vec!["test.subject".to_string()],
                max_age_secs: Some(3600),
                max_messages_per_subject: Some(1),
                create_or_update: true,
                retention: Some(super::super::config::RetentionPolicy::Limits),
                discard: Some(super::super::config::DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: None,
            max_messages: None,
            delay: None,
            throttle: None,
            ..Default::default()
        });
        let (tx, rx) = mpsc::channel(100);

        // Success case.
        let publisher = PublisherBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(publisher.is_ok());

        // Error case - missing config.
        let (_tx2, rx2) = mpsc::channel(100);
        let result = PublisherBuilder::new()
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingRequiredAttribute(_)
        ));
    }
}
