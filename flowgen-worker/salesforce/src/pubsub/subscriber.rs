use flowgen_core::{
    client::Client,
    event::{generate_subject, AvroData, Event, EventBuilder, EventData, SenderExt, SubjectSuffix},
};
use salesforce_pubsub_v1::eventbus::v1::{FetchRequest, SchemaRequest, TopicRequest};
use std::sync::Arc;
use tokio::sync::{broadcast::Sender, Mutex};
use tokio_stream::StreamExt;
use tracing::{error, warn, Instrument};

const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce_pubsub_subscriber";
const DEFAULT_NUM_REQUESTED: i32 = 100;

/// Errors that can occur during Salesforce Pub/Sub subscription operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Pub/Sub context or gRPC communication error.
    #[error("Pub/Sub error: {source}")]
    PubSub {
        #[source]
        source: salesforce_core::pubsub::context::Error,
    },
    /// Client authentication error.
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: salesforce_core::client::Error,
    },
    /// Flowgen core event system error.
    #[error("Event error: {source}")]
    Event {
        #[source]
        source: flowgen_core::event::Error,
    },
    /// Async task join error.
    #[error("Async task join failed: {source}")]
    TaskJoin {
        #[source]
        source: tokio::task::JoinError,
    },
    /// Failed to send event through broadcast channel.
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: tokio::sync::broadcast::error::SendError<Event>,
    },
    /// Binary encoding or decoding error.
    #[error("Binary encoding/decoding failed: {source}")]
    Bincode {
        #[source]
        source: bincode::Error,
    },
    /// Flowgen core service error.
    #[error("Service error: {source}")]
    Service {
        #[source]
        source: flowgen_core::service::Error,
    },
    /// Required configuration attribute is missing.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Cache operation error with descriptive message.
    #[error("Cache error: {_0}")]
    Cache(String),
    /// JSON serialization or deserialization error.
    #[error("JSON serialization/deserialization failed: {source}")]
    Serde {
        #[source]
        source: serde_json::Error,
    },
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
}

/// Processes events from a single Salesforce Pub/Sub topic.
///
/// Subscribes to a topic, deserializes Avro payloads, and forwards events
/// to the event channel. Supports durable consumers with replay ID caching.
pub struct EventHandler {
    /// Salesforce Pub/Sub client context
    pubsub: Arc<Mutex<salesforce_core::pubsub::context::Context>>,
    /// Subscriber configuration
    config: Arc<super::config::Subscriber>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Task identifier for event tracking
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Runs the topic listener to process events from Salesforce Pub/Sub.
    ///
    /// Fetches topic and schema info, establishes subscription with optional
    /// replay ID, then processes incoming events in a loop.
    async fn handle(self) -> Result<(), Error> {
        // Get cache from task context if available.
        let cache = self.task_context.cache.as_ref();
        // Get topic metadata.
        let topic_info = self
            .pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.name.clone(),
            })
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        // Get schema for message deserialization.
        let schema_info = self
            .pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        // Set batch size for event fetching.
        let num_requested = match self.config.topic.num_requested {
            Some(num_requested) => num_requested,
            None => DEFAULT_NUM_REQUESTED,
        };

        // Build fetch request.
        let topic_name = topic_info.topic_name.as_str();
        let mut fetch_request = FetchRequest {
            topic_name: topic_name.to_string(),
            num_requested,
            ..Default::default()
        };

        // Set replay ID for durable consumers.
        if let Some(durable_consumer_opts) = self
            .config
            .topic
            .durable_consumer_options
            .as_ref()
            .filter(|opts| opts.enabled && !opts.managed_subscription)
        {
            if let Some(cache) = cache {
                match cache.get(&durable_consumer_opts.name).await {
                    Ok(reply_id) => {
                        fetch_request.replay_id = reply_id.into();
                        fetch_request.replay_preset = 2;
                    }
                    Err(_) => {
                        warn!(
                            "No cache entry found for key: {:?}",
                            &durable_consumer_opts.name
                        );
                    }
                }
            }
        }

        // Subscribe to topic stream.
        let mut stream = self
            .pubsub
            .lock()
            .await
            .subscribe(fetch_request)
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        while let Some(event) = stream.next().await {
            match event {
                Ok(fr) => {
                    for ce in fr.events {
                        // Cache replay ID for durable consumer recovery.
                        if let Some(durable_consumer_opts) = self
                            .config
                            .topic
                            .durable_consumer_options
                            .as_ref()
                            .filter(|opts| opts.enabled && !opts.managed_subscription)
                        {
                            if let Some(cache) = cache {
                                cache
                                    .put(&durable_consumer_opts.name, ce.replay_id.into())
                                    .await
                                    .map_err(|err| {
                                        Error::Cache(format!("Failed to cache replay ID: {err:?}"))
                                    })?;
                            }
                        }

                        if let Some(event) = ce.event {
                            // Setup event data payload.
                            let data = AvroData {
                                schema: schema_info.schema_json.clone(),
                                raw_bytes: event.payload[..].to_vec(),
                            };

                            // Normalize topic name.
                            let topic = topic_name.replace('/', ".").to_lowercase();

                            // Generate event subject.
                            let base_subject = if let Some(stripped) = topic.strip_prefix('.') {
                                format!("{DEFAULT_MESSAGE_SUBJECT}.{stripped}")
                            } else {
                                format!("{DEFAULT_MESSAGE_SUBJECT}.{topic}")
                            };
                            let subject =
                                generate_subject(None, &base_subject, SubjectSuffix::Id(&event.id));

                            // Build and send event.
                            let e = EventBuilder::new()
                                .data(EventData::Avro(data))
                                .subject(subject)
                                .id(event.id)
                                .current_task_id(self.current_task_id)
                                .task_type(self.task_context.task_type)
                                .build()
                                .map_err(|e| Error::Event { source: e })?;

                            self.tx
                                .send_with_logging(e)
                                .map_err(|e| Error::SendMessage { source: e })?;
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::PubSub {
                        source: salesforce_core::pubsub::context::Error::Tonic(Box::new(e)),
                    });
                }
            }
        }

        Ok(())
    }
}

/// Manages multiple Salesforce Pub/Sub topic subscriptions.
///
/// Creates TopicListener instances for each configured topic,
/// handling authentication and connection setup.
#[derive(Debug)]
pub struct Subscriber {
    /// Configuration for topics, credentials, and consumer options
    config: Arc<super::config::Subscriber>,
    /// Event channel sender
    tx: Sender<Event>,
    /// Task identifier for event tracking
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Subscriber {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the subscriber by establishing connections and authentication.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Creating gRPC service connection
    /// - Authenticating with Salesforce
    /// - Building Pub/Sub context
    async fn init(&self) -> Result<EventHandler, Error> {
        // Determine Pub/Sub endpoint
        let endpoint = match &self.config.endpoint {
            Some(endpoint) => endpoint,
            None => &format!(
                "{}:{}",
                super::config::DEFAULT_PUBSUB_URL,
                super::config::DEFAULT_PUBSUB_PORT
            ),
        };

        // Create gRPC service connection
        let service = flowgen_core::service::ServiceBuilder::new()
            .endpoint(endpoint.to_owned())
            .build()
            .map_err(|e| Error::Service { source: e })?
            .connect()
            .await
            .map_err(|e| Error::Service { source: e })?;

        let channel = service.channel.ok_or_else(|| Error::Service {
            source: flowgen_core::service::Error::MissingEndpoint(),
        })?;

        // Authenticate with Salesforce
        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(self.config.credentials_path.clone())
            .build()
            .map_err(|e| Error::Auth { source: e })?
            .connect()
            .await
            .map_err(|e| Error::Auth { source: e })?;

        // Create Pub/Sub context
        let pubsub = salesforce_core::pubsub::context::Context::new(channel, sfdc_client)
            .map_err(|e| Error::PubSub { source: e })?;
        let pubsub = Arc::new(Mutex::new(pubsub));

        // Create event handler
        Ok(EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.current_task_id,
            tx: self.tx.clone(),
            pubsub,
            task_context: Arc::clone(&self.task_context),
        })
    }

    /// Runs the subscriber by initializing and spawning the event handler task.
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

/// Builder for constructing Subscriber instances.
#[derive(Default)]
pub struct SubscriberBuilder {
    /// Subscriber configuration
    config: Option<Arc<super::config::Subscriber>>,
    /// Event channel sender
    tx: Option<Sender<Event>>,
    /// Task identifier
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
}

impl SubscriberBuilder {
    /// Creates a new builder instance.
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    /// Sets the subscriber configuration.
    pub fn config(mut self, config: Arc<super::config::Subscriber>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event channel sender.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the current task ID.
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    /// Sets the task execution context.
    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// Builds the Subscriber instance.
    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::config;
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
        assert!(builder.task_context.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[test]
    fn test_subscriber_builder_config() {
        let config = Arc::new(config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: config::Topic {
                name: "/event/Test__e".to_string(),
                durable_consumer_options: None,
                num_requested: Some(10),
            },
            endpoint: None,
        });

        let builder: SubscriberBuilder = SubscriberBuilder::new().config(config.clone());
        assert!(builder.config.is_some());
        assert_eq!(builder.config.unwrap().topic.name, "/event/Test__e");
    }

    #[test]
    fn test_subscriber_builder_sender() {
        let (tx, _) = broadcast::channel::<Event>(10);
        let builder: SubscriberBuilder = SubscriberBuilder::new().sender(tx);
        assert!(builder.tx.is_some());
    }

    #[test]
    fn test_subscriber_builder_current_task_id() {
        let builder: SubscriberBuilder = SubscriberBuilder::new().current_task_id(99);
        assert_eq!(builder.current_task_id, 99);
    }

    #[tokio::test]
    async fn test_subscriber_builder_missing_config() {
        let (tx, _) = broadcast::channel::<Event>(10);

        let result = SubscriberBuilder::new()
            .sender(tx)
            .task_context(create_mock_task_context())
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[tokio::test]
    async fn test_subscriber_builder_missing_sender() {
        let config = Arc::new(config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: config::Topic {
                name: "/event/Test__e".to_string(),
                durable_consumer_options: None,
                num_requested: Some(5),
            },
            endpoint: None,
        });

        let result = SubscriberBuilder::new()
            .config(config)
            .task_context(create_mock_task_context())
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
        let config = Arc::new(config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("complete_creds"),
            topic: config::Topic {
                name: "/data/AccountChangeEvent".to_string(),
                durable_consumer_options: Some(config::DurableConsumerOptions {
                    enabled: true,
                    managed_subscription: true,
                    name: "TestConsumer".to_string(),
                }),
                num_requested: Some(100),
            },
            endpoint: Some("api.pubsub.salesforce.com:7443".to_string()),
        });

        let (tx, _) = broadcast::channel::<Event>(10);

        let result = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx)
            .current_task_id(42)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_ok());
        let subscriber = result.unwrap();
        assert_eq!(subscriber.current_task_id, 42);
        assert_eq!(subscriber.config.topic.name, "/data/AccountChangeEvent");
        assert_eq!(
            subscriber.config.endpoint,
            Some("api.pubsub.salesforce.com:7443".to_string())
        );
    }

    #[tokio::test]
    async fn test_subscriber_builder_build_missing_task_context() {
        let config = Arc::new(config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: config::Topic {
                name: "/event/Test__e".to_string(),
                durable_consumer_options: None,
                num_requested: Some(10),
            },
            endpoint: None,
        });
        let (tx, _) = broadcast::channel::<Event>(10);

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
