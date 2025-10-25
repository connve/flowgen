use super::message::FlowgenMessageExt;
use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{
    generate_subject, Event, EventBuilder, EventData, SenderExt, SubjectSuffix,
};
use std::sync::Arc;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    Mutex,
};
use tracing::{error, Instrument};

/// Default subject prefix for NATS publisher.
const DEFAULT_MESSAGE_SUBJECT: &str = "nats_jetstream_publisher";

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
    /// Client authentication or connection error.
    #[error(transparent)]
    ClientAuth(#[from] crate::client::Error),
    /// Failed to publish message to JetStream.
    #[error("Failed to publish message to JetStream: {source}")]
    Publish {
        #[source]
        source: async_nats::jetstream::context::PublishError,
    },
    /// Stream management error.
    #[error(transparent)]
    Stream(#[from] super::stream::Error),
    /// Error converting event to message format.
    #[error(transparent)]
    MessageConversion(#[from] super::message::Error),
    /// Required event attribute is missing.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Stream configuration is missing.
    #[error("Stream configuration is missing")]
    NoStream,
    /// Client was not properly initialized or is missing.
    #[error("Client is missing or not initialized properly")]
    MissingClient(),
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
    /// Failed to send event through broadcast channel.
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: tokio::sync::broadcast::error::SendError<Event>,
    },
    /// JSON serialization error.
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    /// Event building error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Configuration rendering error.
    #[error("Configuration rendering failed: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
}

pub struct EventHandler {
    jetstream: Arc<Mutex<async_nats::jetstream::Context>>,
    current_task_id: usize,
    tx: Sender<Event>,
    config: Arc<super::config::Publisher>,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if event.current_task_id != self.current_task_id.checked_sub(1) {
            return Ok(());
        }

        // Render config with event data to support templates like "pubsub.{{event.subject}}"
        let event_data = event.to_template_data().map_err(Error::Event)?;

        let render_data = serde_json::json!({
            "event": event_data
        });

        let rendered_config = self
            .config
            .render(&render_data)
            .map_err(|e| Error::ConfigRender { source: e })?;

        let e = event.to_publish()?;

        let ack_future = self
            .jetstream
            .lock()
            .await
            .send_publish(rendered_config.subject, e)
            .await
            .map_err(|e| Error::Publish { source: e })?;

        let ack = ack_future.await.map_err(|e| Error::Publish { source: e })?;
        let ack: PublishAck = ack.into();
        let ack_json = serde_json::to_value(&ack).map_err(|e| Error::SerdeJson { source: e })?;

        let subject = generate_subject(
            Some(&self.config.name),
            DEFAULT_MESSAGE_SUBJECT,
            SubjectSuffix::Timestamp,
        );

        let e = EventBuilder::new()
            .subject(subject)
            .data(EventData::Json(ack_json))
            .current_task_id(self.current_task_id)
            .build()?;

        self.tx
            .send_with_logging(e)
            .map_err(|e| Error::SendMessage { source: e })?;

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
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
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
            .build()?
            .connect()
            .await?;

        if let Some(jetstream) = client.jetstream {
            let stream_opts = self.config.stream.as_ref().ok_or_else(|| Error::NoStream)?;

            let jetstream = match stream_opts.create_or_update {
                true => super::stream::create_or_update_stream(jetstream, stream_opts).await?,
                false => jetstream,
            };

            let jetstream = Arc::new(Mutex::new(jetstream));
            let event_handler = EventHandler {
                jetstream,
                current_task_id: self.current_task_id,
                tx: self.tx.clone(),
                config: Arc::clone(&self.config),
            };

            Ok(event_handler)
        } else {
            Err(Error::MissingClient())
        }
    }

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.current_task_id))]
    async fn run(mut self) -> Result<(), Self::Error> {
        // Initialize runner task.
        let event_handler = match self.init().await {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!("{}", e);
                return Ok(());
            }
        };

        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    tokio::spawn(
                        async move {
                            if let Err(err) = event_handler.handle(event).await {
                                error!("{}", err);
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                Err(_) => return Ok(()),
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
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
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
            }),
            durable_name: None,
            batch_size: None,
            delay_secs: None,
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
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
            subject: "test.subject".to_string(),
            stream: None,
            durable_name: None,
            batch_size: None,
            delay_secs: None,
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
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
            subject: "test.subject.1".to_string(),
            stream: Some(super::super::config::StreamOptions {
                name: "test_stream".to_string(),
                description: Some("Test description".to_string()),
                subjects: vec!["test.subject.1".to_string(), "test.subject.2".to_string()],
                max_age_secs: Some(86400),
                max_messages_per_subject: Some(1),
                create_or_update: true,
                retention: Some(super::super::config::RetentionPolicy::Limits),
                discard: Some(super::super::config::DiscardPolicy::Old),
            }),
            durable_name: None,
            batch_size: None,
            delay_secs: None,
        });
        let (tx, rx) = broadcast::channel(100);

        let result = PublisherBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx)
            .current_task_id(5)
            .task_context(create_mock_task_context())
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
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("/chain/test.creds"),
            subject: "chain.subject".to_string(),
            stream: Some(super::super::config::StreamOptions {
                name: "chain_stream".to_string(),
                description: None,
                subjects: vec!["chain.subject".to_string()],
                max_age_secs: Some(1800),
                max_messages_per_subject: None,
                create_or_update: false,
                retention: Some(super::super::config::RetentionPolicy::WorkQueue),
                discard: Some(super::super::config::DiscardPolicy::Old),
            }),
            durable_name: None,
            batch_size: None,
            delay_secs: None,
        });
        let (tx, rx) = broadcast::channel(50);

        let publisher = PublisherBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx)
            .current_task_id(10)
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        assert_eq!(publisher.config, config);
        assert_eq!(publisher.current_task_id, 10);
    }

    #[tokio::test]
    async fn test_publisher_builder_build_missing_task_context() {
        let config = Arc::new(super::super::config::Publisher {
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("/test/creds.jwt"),
            subject: "test.subject".to_string(),
            stream: None,
            durable_name: None,
            batch_size: None,
            delay_secs: None,
        });
        let (tx, rx) = broadcast::channel(100);

        let result = PublisherBuilder::new()
            .config(config)
            .receiver(rx)
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
