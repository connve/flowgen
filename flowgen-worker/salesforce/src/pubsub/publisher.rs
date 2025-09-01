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
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce.pubsub.out";
const DEFAULT_PUBSUB_URI: &str = "https://api.pubsub.salesforce.com";
const DEFAULT_PUBSUB_PORT: &str = "443";

/// Errors that can occur during Salesforce Pub/Sub publishing operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Salesforce Pub/Sub context or gRPC communication error.
    #[error(transparent)]
    SalesforcePubSub(#[from] super::context::Error),
    /// Salesforce client authentication error.
    #[error(transparent)]
    SalesforceAuth(#[from] crate::client::Error),
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
    #[error("Missing required event attribute: {}.", _0)]
    MissingRequiredAttribute(String),
    /// Event data object is empty when content is expected.
    #[error("Empty object.")]
    EmptyObject(),
    /// Failed to parse Avro schema from JSON string.
    #[error("Error parsing Schema Json string to Schema type.")]
    SchemaParse(),
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
}

impl flowgen_core::task::runner::Runner for Publisher {
    type Error = Error;
    async fn run(mut self) -> Result<(), Self::Error> {
        let config = self.config.as_ref();
        let a = Path::new(&config.credentials);

        let service = flowgen_core::service::ServiceBuilder::new()
            .endpoint(format!("{DEFAULT_PUBSUB_URI}:{DEFAULT_PUBSUB_PORT}"))
            .build()
            .map_err(Error::Service)?
            .connect()
            .await
            .map_err(Error::Service)?;

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(a.to_path_buf())
            .build()
            .map_err(Error::SalesforceAuth)?
            .connect()
            .await
            .map_err(Error::SalesforceAuth)?;

        let pubsub = super::context::ContextBuilder::new(service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::SalesforcePubSub)?;

        let pubsub = Arc::new(Mutex::new(pubsub));

        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.clone(),
            })
            .await
            .map_err(Error::SalesforcePubSub)?
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(Error::SalesforcePubSub)?
            .into_inner();

        let pubsub = pubsub.clone();

        let topic = &self.config.topic;
        let schema_id = &schema_info.schema_id;

        let schema: Schema = schema_info
            .schema_json
            .parse()
            .map_err(|_| Error::SchemaParse())?;

        let serializer_config = &mut ser::SerializerConfig::new(&schema);

        while let Ok(event) = self.rx.recv().await {
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
                    .map_err(Error::SalesforcePubSub)?;

                // Generate event subject/
                let topic = topic_info.topic_name.replace('/', ".").to_lowercase();
                let base_subject = format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, &topic[1..]);
                let subject = generate_subject(
                    self.config.label.as_deref(),
                    &base_subject,
                    SubjectSuffix::Timestamp,
                );

                event!(Level::INFO, "Event processed: {}", subject);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    config: Option<Arc<super::config::Publisher>>,
    rx: Option<Receiver<Event>>,
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
    use crate::pubsub::config;
    use serde_json::json;
    use tokio::sync::broadcast;

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
            label: Some("test".to_string()),
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
            label: Some("test".to_string()),
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
            label: Some("test_publisher".to_string()),
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
            .build()
            .await;

        assert!(result.is_ok());
        let publisher = result.unwrap();
        assert_eq!(publisher.current_task_id, 5);
        assert_eq!(publisher.config.topic, "/event/Test__e");
    }

    #[test]
    fn test_error_display() {
        let err = Error::MissingRequiredAttribute("test_field".to_string());
        assert!(err
            .to_string()
            .contains("Missing required event attribute: test_field"));

        let err = Error::EmptyObject();
        assert!(err.to_string().contains("Empty object"));

        let err = Error::SchemaParse();
        assert!(err.to_string().contains("Error parsing Schema Json"));
    }
}
