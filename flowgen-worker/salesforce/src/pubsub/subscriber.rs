use flowgen_core::{
    client::Client,
    event::{AvroData, Event, EventBuilder, EventData, SenderExt},
};
use salesforce_pubsub_v1::eventbus::v1::{FetchRequest, SchemaRequest, TopicRequest};
use std::sync::Arc;
use tokio::sync::{mpsc::Sender, Mutex};
use tokio_stream::StreamExt;
use tracing::{error, warn, Instrument};

const DEFAULT_NUM_REQUESTED: i32 = 100;
const DEFAULT_TOPIC_PREFIX_DATA: &str = "/data/";
const DEFAULT_TOPIC_PREFIX_EVENT: &str = "/event/";

/// Errors that can occur during Salesforce Pub/Sub subscription operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Pub/Sub error: {source}")]
    PubSub {
        #[source]
        source: salesforce_core::pubsub::context::Error,
    },
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: salesforce_core::client::Error,
    },
    #[error("Event error: {source}")]
    Event {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Async task join failed: {source}")]
    TaskJoin {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Binary encoding/decoding failed with error: {source}")]
    Bincode {
        #[source]
        source: bincode::Error,
    },
    #[error("Service error: {source}")]
    Service {
        #[source]
        source: flowgen_core::service::Error,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Cache error: {_0}")]
    Cache(String),
    #[error("JSON serialization/deserialization failed with error: {source}")]
    Serde {
        #[source]
        source: serde_json::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Stream ended unexpectedly, connection may have been lost")]
    StreamEnded,
    #[error("Failed to clear invalid replay_id from cache: {0}")]
    ReplayIdCacheClear(String),
    #[error("Managed subscription requires durable_consumer_options to be configured")]
    MissingManagedSubscriptionConfig,
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
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

/// Checks if a gRPC error is due to an invalid/corrupted replay ID.
fn is_replay_id_error(error: &salesforce_core::pubsub::context::Error) -> bool {
    if let salesforce_core::pubsub::context::Error::Tonic(status) = error {
        if let Some(error_code) = status.metadata().get("error-code") {
            if let Ok(code_str) = error_code.to_str() {
                return code_str
                    == "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.validation.failed"
                    || code_str
                        == "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted";
            }
        }
    }
    false
}

impl EventHandler {
    /// Processes a batch of events from Salesforce Pub/Sub.
    async fn process_events(
        &self,
        events: Vec<salesforce_pubsub_v1::eventbus::v1::ConsumerEvent>,
        schema_info: &salesforce_pubsub_v1::eventbus::v1::SchemaInfo,
        topic_name: &str,
    ) -> Result<(), Error> {
        let cache = self.task_context.cache.as_ref();

        for ce in events {
            // Cache replay ID for durable consumer recovery (non-managed only).
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

                // Normalize topic name by removing data/ or event/ prefix.
                let subject = topic_name
                    .strip_prefix(DEFAULT_TOPIC_PREFIX_DATA)
                    .or_else(|| topic_name.strip_prefix(DEFAULT_TOPIC_PREFIX_EVENT))
                    .unwrap_or(topic_name)
                    .to_lowercase();

                // Build and send event.
                let e = EventBuilder::new()
                    .data(EventData::Avro(data))
                    .subject(subject)
                    .id(event.id)
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|e| Error::Event { source: e })?;

                self.tx
                    .send_with_logging(e)
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }
        }

        Ok(())
    }

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

        // Get topic name for later use.
        let topic_name = topic_info.topic_name.as_str();

        // Determine subscription type and create appropriate stream.
        let use_managed_subscription = self
            .config
            .topic
            .durable_consumer_options
            .as_ref()
            .map(|opts| opts.enabled && opts.managed_subscription)
            .unwrap_or(false);

        if use_managed_subscription {
            // Use managed subscription.
            let durable_consumer_opts = self
                .config
                .topic
                .durable_consumer_options
                .as_ref()
                .ok_or(Error::MissingManagedSubscriptionConfig)?;

            let managed_request = salesforce_pubsub_v1::eventbus::v1::ManagedFetchRequest {
                developer_name: durable_consumer_opts.name.clone(),
                num_requested,
                ..Default::default()
            };

            let mut stream = self
                .pubsub
                .lock()
                .await
                .managed_subscribe(managed_request)
                .await
                .map_err(|e| Error::PubSub { source: e })?
                .into_inner();

            // Process managed subscription events.
            while let Some(event) = stream.next().await {
                let events = match event {
                    Ok(fr) => fr.events,
                    Err(e) => {
                        return Err(Error::PubSub {
                            source: salesforce_core::pubsub::context::Error::Tonic(Box::new(e)),
                        });
                    }
                };

                self.process_events(events, &schema_info, topic_name)
                    .await?;
            }

            return Err(Error::StreamEnded);
        }

        // Build fetch request with optional replay_id.
        let mut fetch_request = FetchRequest {
            topic_name: topic_name.to_string(),
            num_requested,
            ..Default::default()
        };

        // Set replay ID or preset for non-managed durable consumers.
        if let Some(durable_consumer_opts) = self
            .config
            .topic
            .durable_consumer_options
            .as_ref()
            .filter(|opts| opts.enabled)
        {
            // Try to load cached replay_id, or use configured preset.
            let cached_replay_id = match cache {
                Some(c) => c.get(&durable_consumer_opts.name).await.ok(),
                None => None,
            };

            match cached_replay_id {
                Some(reply_id) => {
                    // Found cached replay_id - use it to resume from last processed event.
                    fetch_request.replay_id = reply_id.into();
                    fetch_request.replay_preset = 2; // CUSTOM preset when using replay_id.
                }
                None => {
                    // No cached replay_id found - use replay_preset to determine start position.
                    // LATEST (0) starts from the tip of the stream.
                    // EARLIEST (1) starts from the earliest retained events in the retention window.
                    fetch_request.replay_preset = durable_consumer_opts.replay_preset.to_i32();
                    warn!(
                        "No cache entry found for key: {:?}, starting from {:?}",
                        &durable_consumer_opts.name, &durable_consumer_opts.replay_preset
                    );
                }
            }
        }

        let mut stream = match self
            .pubsub
            .lock()
            .await
            .subscribe(fetch_request.clone())
            .await
        {
            Ok(response) => response.into_inner(),
            Err(e) => {
                // If subscribe fails due to invalid replay_id, clear it from cache and let retry handle it.
                if !fetch_request.replay_id.is_empty() && is_replay_id_error(&e) {
                    warn!("Invalid replay_id detected, clearing from cache and retrying");

                    // Clear the invalid replay_id from cache so next retry starts fresh.
                    if let Some(durable_consumer_opts) = self
                        .config
                        .topic
                        .durable_consumer_options
                        .as_ref()
                        .filter(|opts| opts.enabled)
                    {
                        if let Some(cache) = cache {
                            cache
                                .delete(&durable_consumer_opts.name)
                                .await
                                .map_err(|e| Error::ReplayIdCacheClear(e.to_string()))?;
                        }
                    }
                }

                // Return error to trigger retry with fresh connection.
                return Err(Error::PubSub { source: e });
            }
        };

        while let Some(event) = stream.next().await {
            let events = match event {
                Ok(fr) => fr.events,
                Err(e) => {
                    return Err(Error::PubSub {
                        source: salesforce_core::pubsub::context::Error::Tonic(Box::new(e)),
                    });
                }
            };

            self.process_events(events, &schema_info, topic_name)
                .await?;
        }

        Err(Error::StreamEnded)
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

    /// Initializes the subscriber by establishing connections and authentication.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Creating gRPC service connection
    /// - Authenticating with Salesforce
    /// - Building Pub/Sub context
    async fn init(&self) -> Result<EventHandler, Error> {
        // Determine Pub/Sub endpoint.
        let endpoint = match &self.config.endpoint {
            Some(endpoint) => endpoint,
            None => &format!(
                "{}:{}",
                super::config::DEFAULT_PUBSUB_URL,
                super::config::DEFAULT_PUBSUB_PORT
            ),
        };

        // Create gRPC service connection.
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

        // Authenticate with Salesforce.
        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(self.config.credentials_path.clone())
            .build()
            .map_err(|e| Error::Auth { source: e })?
            .connect()
            .await
            .map_err(|e| Error::Auth { source: e })?;

        // Create Pub/Sub context.
        let pubsub = salesforce_core::pubsub::context::Context::new(channel, sfdc_client)
            .map_err(|e| Error::PubSub { source: e })?;
        let pubsub = Arc::new(Mutex::new(pubsub));

        // Create event handler.
        Ok(EventHandler {
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            pubsub,
            task_type: self.task_type,
            task_context: Arc::clone(&self._task_context),
        })
    }

    /// Runs the subscriber by initializing and spawning the event handler task.
    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        // Merge app-level and task-level retry config.
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        // Spawn event handler task.
        tokio::spawn(
            async move {
                // Retry loop with exponential backoff.
                let result = tokio_retry::Retry::spawn(retry_config.strategy(), || async {
                    // Initialize task.
                    let event_handler = match self.init().await {
                        Ok(handler) => handler,
                        Err(e) => {
                            error!("{}", e);
                            return Err(e);
                        }
                    };

                    // Run event handler.
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

/// Builder for constructing Subscriber instances.
#[derive(Default)]
pub struct SubscriberBuilder {
    /// Subscriber configuration
    config: Option<Arc<super::config::Subscriber>>,
    /// Event channel sender
    tx: Option<Sender<Event>>,
    /// Task identifier
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
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
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
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

    /// Sets the task type.
    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
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
    async fn test_subscriber_builder() {
        let config = Arc::new(config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: config::Topic {
                name: "/event/Test__e".to_string(),
                durable_consumer_options: None,
                num_requested: Some(10),
            },
            endpoint: None,
            retry: None,
        });
        let (tx, _) = mpsc::channel::<Event>(10);

        // Success case.
        let subscriber = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(subscriber.is_ok());

        // Error case - missing config.
        let (tx2, _rx2) = mpsc::channel::<Event>(10);
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
}
