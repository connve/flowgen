use chrono::Utc;
use flowgen_core::{
    client::Client,
    config::ConfigExt,
    event::{generate_subject, Event, SubjectSuffix},
    serde::{MapExt, StringExt},
};
use salesforce_pubsub::eventbus::v1::{ProducerEvent, PublishRequest, SchemaRequest, TopicRequest};
use serde_avro_fast::{ser, Schema};
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{error, info, Instrument};

const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce_pubsub_publisher";

/// Errors that can occur during Salesforce Pub/Sub publishing operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Pub/Sub context or gRPC communication error.
    #[error(transparent)]
    PubSub(#[from] super::context::Error),
    /// Client authentication error.
    #[error(transparent)]
    Auth(#[from] crate::client::Error),
    /// Flowgen core serialization extension error.
    #[error(transparent)]
    SerdeExt(#[from] flowgen_core::serde::Error),
    /// Apache Avro serialization error.
    #[error("Avro serialization failed: {source}")]
    SerdeAvro {
        #[source]
        source: serde_avro_fast::ser::SerError,
    },
    /// Avro schema parsing error.
    #[error("Avro schema parsing failed: {source}")]
    SerdeSchema {
        #[source]
        source: serde_avro_fast::schema::SchemaError,
    },
    /// Template rendering error for dynamic configuration.
    #[error(transparent)]
    Render(#[from] flowgen_core::config::Error),
    /// Failed to send event through broadcast channel.
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: tokio::sync::broadcast::error::SendError<Event>,
    },
    /// Flowgen core event system error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Flowgen core service error.
    #[error(transparent)]
    Service(#[from] flowgen_core::service::Error),
    /// Required attribute is missing.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Event data object is empty when content is expected.
    #[error("Empty object")]
    EmptyObject(),
    /// Failed to parse Avro schema from JSON string.
    #[error("Error parsing Schema Json string to Schema type")]
    SchemaParse(),
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
}

/// Event handler for processing and publishing events to Salesforce Pub/Sub.
pub struct EventHandler {
    /// Publisher configuration.
    config: Arc<super::config::Publisher>,
    /// Pub/Sub connection context.
    pubsub: Arc<Mutex<super::context::Context>>,
    /// Topic name for publishing.
    topic: String,
    /// Base subject for event generation.
    base_subject: String,
    /// Schema ID for event serialization.
    schema_id: String,
    /// Avro serializer configuration.
    serializer_config: Mutex<ser::SerializerConfig<'static>>,
    /// Current task identifier.
    current_task_id: usize,
}

impl EventHandler {
    /// Processes an event by publishing it to Salesforce Pub/Sub.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let config = self.config.render(&event.data)?;
        let payload = config.payload.to_string()?.to_value()?;

        let mut publish_payload: Map<String, Value> = Map::new();
        for (k, v) in payload.as_object().ok_or_else(Error::EmptyObject)? {
            publish_payload.insert(k.to_owned(), v.to_owned());
        }
        let now = Utc::now().timestamp_millis();
        publish_payload.insert("CreatedDate".to_string(), Value::Number(now.into()));

        let mut serializer_config = self.serializer_config.lock().await;
        let serialized_payload: Vec<u8> =
            serde_avro_fast::to_datum_vec(&publish_payload, &mut serializer_config)
                .map_err(|e| Error::SerdeAvro { source: e })?;

        let mut events = Vec::new();
        let pe = ProducerEvent {
            schema_id: self.schema_id.clone(),
            payload: serialized_payload,
            ..Default::default()
        };
        events.push(pe);

        let _ = self
            .pubsub
            .lock()
            .await
            .publish(PublishRequest {
                topic_name: self.topic.clone(),
                events,
                ..Default::default()
            })
            .await?;

        // Generate event subject.
        let subject = generate_subject(
            Some(&self.config.name),
            &self.base_subject,
            SubjectSuffix::Timestamp,
        );

        info!("Event processed: {}", subject);
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
    /// Current task identifier for event filtering.
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
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
            .build()?
            .connect()
            .await?;

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        let pubsub = super::context::Context::new(service, sfdc_client)?;

        let pubsub = Arc::new(Mutex::new(pubsub));

        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.clone(),
            })
            .await?
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await?
            .into_inner();

        let schema: Schema = schema_info
            .schema_json
            .parse()
            .map_err(|e| Error::SerdeSchema { source: e })?;

        // Leak the schema to get a 'static reference.
        // This is intentional and safe in this context since the schema
        // is effectively program-lifetime data.
        let leaked_schema: &'static Schema = Box::leak(Box::new(schema));
        let serializer_config = ser::SerializerConfig::new(leaked_schema);

        // Generate base subject.
        let topic = topic_info.topic_name.replace('/', ".").to_lowercase();
        let base_subject = format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, &topic[1..]);

        let topic_name = self.config.topic.clone();

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            pubsub,
            topic: topic_name,
            base_subject,
            schema_id: schema_info.schema_id,
            serializer_config: Mutex::new(serializer_config),
            current_task_id: self.current_task_id,
        };

        Ok(event_handler)
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
                    if event.current_task_id == event_handler.current_task_id.checked_sub(1) {
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
    current_task_id: usize,
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
        assert_eq!(builder.current_task_id, 0);
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
    fn test_publisher_builder_current_task_id() {
        let builder = PublisherBuilder::new().current_task_id(42);
        assert_eq!(builder.current_task_id, 42);
    }

    #[tokio::test]
    async fn test_publisher_builder_missing_config() {
        let (_, rx) = broadcast::channel::<Event>(10);
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

        let (_, rx) = broadcast::channel::<Event>(10);

        let result = PublisherBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .current_task_id(5)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_ok());
        let publisher = result.unwrap();
        assert_eq!(publisher.current_task_id, 5);
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
        let (_, rx) = broadcast::channel::<Event>(10);

        let result = PublisherBuilder::new()
            .config(config)
            .receiver(rx)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "task_context")
        );
    }
}
