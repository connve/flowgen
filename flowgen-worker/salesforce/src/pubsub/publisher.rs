use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use chrono::Utc;
use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{generate_subject, Event, SenderExt, SubjectSuffix};
use salesforce_pubsub_v1::eventbus::v1::{
    ProducerEvent, PublishRequest, SchemaRequest, TopicRequest,
};
use std::sync::Arc;
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{error, Instrument};

/// Errors that can occur during Salesforce Pub/Sub publishing operations.
#[derive(thiserror::Error, Debug)]
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
    /// Flowgen core serialization extension error.
    #[error("Serialization error: {source}")]
    SerdeExt {
        #[source]
        source: flowgen_core::serde::Error,
    },
    /// Apache Avro error.
    #[error("Avro operation failed: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    /// Template rendering error for dynamic configuration.
    #[error("Render error: {source}")]
    Render {
        #[source]
        source: flowgen_core::config::Error,
    },
    /// Failed to send event through broadcast channel.
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    /// Event building error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Configuration template rendering failed.
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    /// Flowgen core service error.
    #[error("Service error: {source}")]
    Service {
        #[source]
        source: flowgen_core::service::Error,
    },
    /// Required attribute is missing.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Event data object is empty when content is expected.
    #[error("Empty object")]
    EmptyObject(),
    /// Failed to parse Avro schema from JSON string.
    #[error("Error parsing Schema JSON string to Schema type")]
    SchemaParse(),
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
    /// JSON serialization error.
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
}

/// Event handler for processing and publishing events to Salesforce Pub/Sub.
pub struct EventHandler {
    /// Publisher configuration.
    config: Arc<super::config::Publisher>,
    /// Pub/Sub connection context.
    pubsub: Arc<Mutex<salesforce_core::pubsub::context::Context>>,
    /// Topic name for publishing.
    topic: String,
    /// Subject prefix for event generation (derived from topic name).
    subject_prefix: String,
    /// Schema ID for event serialization.
    schema_id: String,
    /// Avro serializer configuration.
    schema: Arc<AvroSchema>,
    /// Current task identifier.
    task_id: usize,
    /// Channel sender for response events.
    tx: tokio::sync::broadcast::Sender<Event>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event by publishing it to Salesforce Pub/Sub.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        // Render config with to support templates inside configuration.
        let event_value = serde_json::value::Value::try_from(&event)?;
        let config = self.config.render(&event_value)?;
        let mut publish_payload = config.payload;

        let now = Utc::now().timestamp_millis();
        publish_payload.insert(
            "CreatedDate".to_string(),
            serde_json::Value::Number(serde_json::Number::from(now)),
        );

        // Convert serde_json::Map to Avro Record using From<serde_json::Value> trait
        let json_value = serde_json::Value::Object(publish_payload);
        let record = AvroValue::from(json_value)
            .resolve(self.schema.as_ref())
            .map_err(|e| Error::Avro { source: e })?;

        // Serialize the record directly without schema wrapper (Salesforce expects just the data)
        let serialized_payload = apache_avro::to_avro_datum(self.schema.as_ref(), record)
            .map_err(|e| Error::Avro { source: e })?;

        let mut events = Vec::new();
        let pe = ProducerEvent {
            schema_id: self.schema_id.clone(),
            payload: serialized_payload,
            ..Default::default()
        };
        events.push(pe);

        let resp = self
            .pubsub
            .lock()
            .await
            .publish(PublishRequest {
                topic_name: self.topic.clone(),
                events,
                ..Default::default()
            })
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        let subject = generate_subject(&self.subject_prefix, Some(SubjectSuffix::Id(&resp.rpc_id)));

        let resp_json = serde_json::to_value(resp).map_err(|e| Error::SerdeJson { source: e })?;

        let e = flowgen_core::event::EventBuilder::new()
            .data(flowgen_core::event::EventData::Json(resp_json))
            .subject(subject)
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()?;

        self.tx
            .send_with_logging(e)
            .map_err(|source| Error::SendMessage { source })?;

        Ok(())
    }
}

/// Salesforce Pub/Sub publisher that receives events and publishes them to configured topics.
#[derive(Debug)]
pub struct Publisher {
    /// Publisher configuration including topic settings and credentials.
    config: Arc<super::config::Publisher>,
    /// Receiver for incoming events to publish.
    rx: Receiver<Event>,
    /// Channel sender for response events.
    tx: tokio::sync::broadcast::Sender<Event>,
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

    /// Initializes the publisher by establishing connection and retrieving schema.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Connecting to Salesforce Pub/Sub service
    /// - Authenticating with credentials
    /// - Retrieving topic information and schema
    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self.config.as_ref();

        let service = flowgen_core::service::ServiceBuilder::new()
            .endpoint(format!(
                "{}:{}",
                super::config::DEFAULT_PUBSUB_URL,
                super::config::DEFAULT_PUBSUB_PORT
            ))
            .build()
            .map_err(|e| Error::Service { source: e })?
            .connect()
            .await
            .map_err(|e| Error::Service { source: e })?;

        let channel = service.channel.ok_or_else(|| Error::Service {
            source: flowgen_core::service::Error::MissingEndpoint(),
        })?;

        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(config.credentials_path.clone())
            .build()
            .map_err(|e| Error::Auth { source: e })?
            .connect()
            .await
            .map_err(|e| Error::Auth { source: e })?;

        let pubsub = salesforce_core::pubsub::context::Context::new(channel, sfdc_client)
            .map_err(|e| Error::PubSub { source: e })?;

        let pubsub = Arc::new(Mutex::new(pubsub));

        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.clone(),
            })
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        let schema = AvroSchema::parse_str(&schema_info.schema_json)
            .map_err(|e| Error::Avro { source: e })?;

        // Generate subject prefix from topic name.
        let topic = topic_info.topic_name.replace('/', ".").to_lowercase();
        let subject_prefix = if let Some(stripped) = topic.strip_prefix('.') {
            stripped.to_string()
        } else {
            topic
        };

        let topic_name = self.config.topic.clone();

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            pubsub,
            topic: topic_name,
            subject_prefix,
            schema_id: schema_info.schema_id,
            schema: Arc::new(schema),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
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
                    if Some(event.task_id) == event_handler.task_id.checked_sub(1) {
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
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    config: Option<Arc<super::config::Publisher>>,
    rx: Option<Receiver<Event>>,
    tx: Option<tokio::sync::broadcast::Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
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

    pub fn sender(mut self, sender: tokio::sync::broadcast::Sender<Event>) -> Self {
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
    use crate::pubsub::config;
    use serde_json::{json, Map, Value};
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
        assert_eq!(builder.task_id, 0);
    }

    #[test]
    fn test_publisher_builder_config() {
        let config = Arc::new(config::Publisher {
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: "/event/Test__e".to_string(),
            payload: serde_json::Map::new(),
            endpoint: None,
        });

        let builder = PublisherBuilder::new().config(config.clone());
        assert!(builder.config.is_some());
        assert_eq!(builder.config.unwrap().topic, "/event/Test__e");
    }

    #[test]
    fn test_publisher_builder_receiver() {
        let (_, rx) = broadcast::channel::<Event>(10);
        let builder = PublisherBuilder::new().receiver(rx);
        assert!(builder.rx.is_some());
    }

    #[test]
    fn test_publisher_builder_task_id() {
        let builder = PublisherBuilder::new().task_id(42);
        assert_eq!(builder.task_id, 42);
    }

    #[tokio::test]
    async fn test_publisher_builder_missing_config() {
        let (_, rx) = broadcast::channel::<Event>(10);
        let result = PublisherBuilder::new()
            .receiver(rx)
            .task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[tokio::test]
    async fn test_publisher_builder_missing_receiver() {
        let config = Arc::new(config::Publisher {
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: "/event/Test__e".to_string(),
            payload: serde_json::Map::new(),
            endpoint: None,
        });

        let result = PublisherBuilder::new()
            .config(config)
            .task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "receiver")
        );
    }

    #[tokio::test]
    async fn test_publisher_builder_build_success() {
        let config = Arc::new(config::Publisher {
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: "/event/Test__e".to_string(),
            payload: {
                let mut payload = serde_json::Map::new();
                payload.insert("Test_Field__c".to_string(), json!("test_value"));
                payload
            },
            endpoint: None,
        });

        let (tx, rx) = broadcast::channel::<Event>(10);

        let result = PublisherBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx)
            .task_id(5)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_ok());
        let publisher = result.unwrap();
        assert_eq!(publisher.task_id, 5);
        assert_eq!(publisher.config.topic, "/event/Test__e");
    }

    #[tokio::test]
    async fn test_publisher_builder_build_missing_task_context() {
        let config = Arc::new(config::Publisher {
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: "/event/Test__e".to_string(),
            payload: serde_json::Map::new(),
            endpoint: None,
        });
        let (tx, rx) = broadcast::channel::<Event>(10);

        let result = PublisherBuilder::new()
            .config(config)
            .receiver(rx)
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
