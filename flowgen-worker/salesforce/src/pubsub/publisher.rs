use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use chrono::Utc;
use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventData, EventExt};
use futures_util::future;
use salesforce_core::pubsub::{
    ProducerEvent, PubSubError, PublishRequest, SchemaRequest, TopicRequest,
};
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, Mutex};
use tracing::{error, Instrument};

/// Checks if a gRPC error is due to invalid authentication.
fn is_auth_error(error: &PubSubError) -> bool {
    if let PubSubError::Tonic(status) = error {
        let message = status.message();
        return message.contains("does not have valid authentication credentials")
            || message.contains("authentication exception occurred");
    }
    false
}

/// Errors that can occur during Salesforce Pub/Sub publishing operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Pub/Sub error: {source}")]
    PubSub {
        #[source]
        source: PubSubError,
    },
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: salesforce_core::client::Error,
    },
    #[error("Serialization error: {source}")]
    SerdeExt {
        #[source]
        source: flowgen_core::serde::Error,
    },
    #[error("Avro operation error: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    #[error("Render error: {source}")]
    Render {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Send event message error: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error("Service error: {source}")]
    Service {
        #[source]
        source: flowgen_core::service::Error,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Empty object")]
    EmptyObject(),
    #[error("Error parsing Schema JSON string to Schema type")]
    SchemaParse(),
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Event handler for processing and publishing events to Salesforce Pub/Sub.
pub struct EventHandler {
    /// Publisher configuration.
    config: Arc<super::config::Publisher>,
    /// Pub/Sub connection context.
    pubsub: Arc<Mutex<salesforce_core::pubsub::Client>>,
    /// Topic name for publishing.
    topic: String,
    /// Schema ID for event serialization.
    schema_id: String,
    /// Avro serializer configuration.
    schema: Arc<AvroSchema>,
    /// Current task identifier.
    task_id: usize,
    /// Channel sender for response events.
    tx: Option<tokio::sync::mpsc::Sender<Event>>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event by publishing it to Salesforce Pub/Sub.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let event = Arc::new(event);

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config with to support templates inside configuration.
            let event_value = serde_json::value::Value::try_from(event.as_ref())?;
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

            // Generate subject prefix from topic name.
            let subject = if let Some(stripped) = self
                .topic
                .replace('/', ".")
                .to_lowercase()
                .strip_prefix('.')
            {
                stripped.to_string()
            } else {
                self.topic.to_owned()
            };

            let resp_json =
                serde_json::to_value(&resp).map_err(|e| Error::SerdeJson { source: e })?;

            let e = flowgen_core::event::EventBuilder::new()
                .data(EventData::Json(resp_json))
                .subject(subject)
                .id(resp.rpc_id)
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()?;

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
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
    tx: Option<tokio::sync::mpsc::Sender<Event>>,
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

        let pubsub = salesforce_core::pubsub::Client::new(channel, sfdc_client)
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

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            pubsub,
            topic: config.topic.to_owned(),
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
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize publisher");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(error = %e, "Publisher failed after all retry attempts");
                return Ok(());
            }
        };

        let mut handlers = Vec::new();

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    if Some(event.task_id) == event_handler.task_id.checked_sub(1) {
                        let event_handler = Arc::clone(&event_handler);
                        let retry_strategy = retry_config.strategy();
                        let handle = tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!(error = %e, "Failed to publish message");
                                            // Check if reconnect is needed (gRPC auth errors).
                                            if let Error::PubSub { ref source } = e {
                                                if is_auth_error(source) {
                                                    let mut pubsub =
                                                        event_handler.pubsub.lock().await;
                                                    if let Err(reconnect_err) =
                                                        pubsub.reconnect().await
                                                    {
                                                        return Err(tokio_retry::RetryError::transient(Error::PubSub {
                                                            source: reconnect_err,
                                                        }));
                                                    }
                                                }
                                            }
                                            Err(tokio_retry::RetryError::transient(e))
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(error = %err, "Failed to publish message after all retry attempts");
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );
                        handlers.push(handle);
                    }
                }
                None => {
                    // Channel closed, wait for all spawned handlers to complete.
                    future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    config: Option<Arc<super::config::Publisher>>,
    rx: Option<Receiver<Event>>,
    tx: Option<tokio::sync::mpsc::Sender<Event>>,
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

    pub fn sender(mut self, sender: tokio::sync::mpsc::Sender<Event>) -> Self {
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
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::config;
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
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_publisher_builder() {
        let config = Arc::new(config::Publisher {
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: "/event/Test__e".to_string(),
            payload: serde_json::Map::new(),
            endpoint: None,
            retry: None,
        });
        let (tx, rx) = mpsc::channel::<Event>(10);

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
        let (_tx2, rx2) = mpsc::channel::<Event>(10);
        let result = PublisherBuilder::new()
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }
}
