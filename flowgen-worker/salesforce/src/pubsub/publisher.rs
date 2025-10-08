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
use std::{path::Path, sync::Arc};
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{debug, info};

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
    #[error(transparent)]
    SerdeAvro(#[from] serde_avro_fast::ser::SerError),
    /// Template rendering error for dynamic configuration.
    #[error(transparent)]
    Render(#[from] flowgen_core::config::Error),
    /// Failed to send event through broadcast channel.
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    /// Flowgen core event system error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Flowgen core service error.
    #[error(transparent)]
    Service(#[from] flowgen_core::service::Error),
    /// Required event attribute is missing.
    #[error("Missing required event attribute: {}", _0)]
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
    /// Task manager error.
    #[error(transparent)]
    TaskManager(#[from] flowgen_core::task::manager::Error),
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
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl flowgen_core::task::runner::Runner for Publisher {
    type Error = Error;

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.current_task_id))]
    async fn run(mut self) -> Result<(), Self::Error> {
        // Register task with task manager.
        let task_id = format!(
            "{}.{}.{}",
            self.task_context.flow.name, DEFAULT_MESSAGE_SUBJECT, self.config.name
        );
        let mut task_manager_rx = self
            .task_context
            .task_manager
            .register(
                task_id,
                Some(flowgen_core::task::manager::LeaderElectionOptions {}),
            )
            .await?;

        let config = self.config.as_ref();
        let a = Path::new(&config.credentials);

        let service = flowgen_core::service::ServiceBuilder::new()
            .endpoint(format!(
                "{}:{}",
                super::config::DEFAULT_PUBSUB_URL,
                super::config::DEFAULT_PUBSUB_PORT
            ))
            .build()
            .map_err(Error::Service)?
            .connect()
            .await
            .map_err(Error::Service)?;

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(a.to_path_buf())
            .build()
            .map_err(Error::Auth)?
            .connect()
            .await
            .map_err(Error::Auth)?;

        let pubsub = super::context::ContextBuilder::new(service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::PubSub)?;

        let pubsub = Arc::new(Mutex::new(pubsub));

        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.clone(),
            })
            .await
            .map_err(Error::PubSub)?
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(Error::PubSub)?
            .into_inner();

        let pubsub = pubsub.clone();

        let topic = &self.config.topic;
        let schema_id = &schema_info.schema_id;

        let schema: Schema = schema_info
            .schema_json
            .parse()
            .map_err(|_| Error::SchemaParse())?;

        let serializer_config = &mut ser::SerializerConfig::new(&schema);

        loop {
            tokio::select! {
                biased;

                // Check for leadership changes.
                Some(status) = task_manager_rx.recv() => {
                    if status == flowgen_core::task::manager::LeaderElectionResult::NotLeader {
                        debug!("Lost leadership for task: {}", self.config.name);
                        return Ok(());
                    }
                }

                // Process events.
                result = self.rx.recv() => {
                    match result {
                        Ok(event) => {
                            if event.current_task_id == Some(self.current_task_id - 1) {
                                let config = config.render(&event.data)?;
                                let payload = config.payload.to_string()?.to_value()?;

                                let mut publish_payload: Map<String, Value> = Map::new();
                                for (k, v) in payload.as_object().ok_or_else(Error::EmptyObject)? {
                                    publish_payload.insert(k.to_owned(), v.to_owned());
                                }
                                let now = Utc::now().timestamp_millis();
                                publish_payload.insert("CreatedDate".to_string(), Value::Number(now.into()));

                                let serialized_payload: Vec<u8> =
                                    serde_avro_fast::to_datum_vec(&publish_payload, serializer_config)
                                        .map_err(Error::SerdeAvro)?;

                                let mut events = Vec::new();
                                let pe = ProducerEvent {
                                    schema_id: schema_id.to_string(),
                                    payload: serialized_payload,
                                    ..Default::default()
                                };
                                events.push(pe);

                                let _ = pubsub
                                    .lock()
                                    .await
                                    .publish(PublishRequest {
                                        topic_name: topic.to_string(),
                                        events,
                                        ..Default::default()
                                    })
                                    .await
                                    .map_err(Error::PubSub)?;

                                // Generate event subject/
                                let topic = topic_info.topic_name.replace('/', ".").to_lowercase();
                                let base_subject = format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, &topic[1..]);
                                let subject =
                                    generate_subject(Some(&self.config.name), &base_subject, SubjectSuffix::Timestamp);

                                info!("Event processed: {}", subject);
                            }
                        }
                        Err(_) => return Ok(()),
                    }
                }
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
    use serde_json::{json, Map, Value};
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
            credentials: "test_creds".to_string(),
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
            credentials: "test_creds".to_string(),
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
            credentials: "test_creds".to_string(),
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
}
