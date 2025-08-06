use flowgen_core::{
    cache::Cache,
    connect::client::Client,
    event::{AvroData, Event, EventBuilder, EventData},
};
use salesforce_pubsub::eventbus::v1::{FetchRequest, SchemaRequest, TopicRequest};
use std::sync::Arc;
use tokio::sync::{broadcast::Sender, Mutex};
use tokio_stream::StreamExt;
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce.pubsub.in";
const DEFAULT_PUBSUB_URL: &str = "https://api.pubsub.salesforce.com";
const DEFAULT_PUBSUB_PORT: &str = "443";
const DEFAULT_NUM_REQUESTED: i32 = 1000;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    SalesforcePubSub(#[from] super::context::Error),
    #[error(transparent)]
    SalesforceAuth(#[from] crate::client::Error),
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    #[error(transparent)]
    SerdeAvroSchema(#[from] serde_avro_fast::schema::SchemaError),
    #[error(transparent)]
    SerdeAvroValue(#[from] serde_avro_fast::de::DeError),
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    RecordBatch(#[from] flowgen_core::convert::recordbatch::Error),
    #[error(transparent)]
    Service(#[from] flowgen_core::connect::service::Error),
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("cache error: {0}")]
    Cache(String),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

/// Processes events from a single Salesforce Pub/Sub topic.
///
/// Subscribes to a topic, deserializes Avro payloads, and forwards events
/// to the event channel. Supports durable consumers with replay ID caching.
struct EventHandler<T: Cache> {
    /// Cache for replay IDs and schemas
    cache: Arc<T>,
    /// Salesforce Pub/Sub client context
    pubsub: Arc<Mutex<super::context::Context>>,
    /// Subscriber configuration
    config: Arc<super::config::Subscriber>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Task identifier for event tracking
    current_task_id: usize,
}

impl<T: Cache> EventHandler<T> {
    /// Runs the topic listener to process events from Salesforce Pub/Sub.
    ///
    /// Fetches topic and schema info, establishes subscription with optional
    /// replay ID, then processes incoming events in a loop.
    async fn handle(self) -> Result<(), Error> {
        // Get topic metadata.
        let topic_info = self
            .pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.name.clone(),
            })
            .await
            .map_err(Error::SalesforcePubSub)?
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
            .map_err(Error::SalesforcePubSub)?
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
            match self.cache.get(&durable_consumer_opts.name).await {
                Ok(reply_id) => {
                    fetch_request.replay_id = reply_id.into();
                    fetch_request.replay_preset = 2;
                }
                Err(_) => {
                    event!(
                        Level::WARN,
                        "status: NoCacheKeyFound, message: 'The cache key was not found'"
                    );
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
            .map_err(Error::SalesforcePubSub)?
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
                            self.cache
                                .put(&durable_consumer_opts.name, ce.replay_id.into())
                                .await
                                .map_err(|err| {
                                    Error::Cache(format!("Failed to cache replay ID: {err:?}"))
                                })?;
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
                            let subject = if let Some(stripped) = topic.strip_prefix('.') {
                                format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, stripped, event.id)
                            } else {
                                format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, topic, event.id)
                            };

                            // Build and send event.
                            let e = EventBuilder::new()
                                .data(EventData::Avro(data))
                                .subject(subject)
                                .id(event.id)
                                .current_task_id(self.current_task_id)
                                .build()
                                .map_err(Error::Event)?;

                            event!(Level::INFO, "event processed: {}", e.subject);
                            self.tx.send(e).map_err(Error::SendMessage)?;
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::SalesforcePubSub(super::context::Error::Tonic(e)));
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
pub struct Subscriber<T: Cache> {
    /// Configuration for topics, credentials, and consumer options
    config: Arc<super::config::Subscriber>,
    /// Event channel sender
    tx: Sender<Event>,
    /// Task identifier for event tracking
    current_task_id: usize,
    /// Cache for replay IDs and schemas
    cache: Arc<T>,
}

impl<T: Cache> flowgen_core::task::runner::Runner for Subscriber<T> {
    type Error = Error;

    /// Runs the subscriber by spawning TopicListener tasks for each topic.
    ///
    /// Establishes Salesforce connection, authenticates, and spawns a
    /// TopicListener task for each configured topic.
    async fn run(self) -> Result<(), Error> {
        // Determine Pub/Sub endpoint
        let endpoint = match &self.config.endpoint {
            Some(endpoint) => endpoint,
            None => &format!("{DEFAULT_PUBSUB_URL}:{DEFAULT_PUBSUB_PORT}"),
        };

        // Create gRPC service connection
        let service = flowgen_core::connect::service::ServiceBuilder::new()
            .endpoint(endpoint.to_owned())
            .build()
            .map_err(Error::Service)?
            .connect()
            .await
            .map_err(Error::Service)?;

        // Authenticate with Salesforce
        let sfdc_client = crate::client::Builder::new()
            .credentials_path(self.config.credentials.clone().into())
            .build()
            .map_err(Error::SalesforceAuth)?
            .connect()
            .await
            .map_err(Error::SalesforceAuth)?;

        // Create Pub/Sub context
        let pubsub = super::context::Builder::new(service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::SalesforcePubSub)?;
        let pubsub = Arc::new(Mutex::new(pubsub));

        // Spawn TopicListener.
        let pubsub: Arc<Mutex<super::context::Context>> = Arc::clone(&pubsub);
        let tx = self.tx.clone();
        let config = Arc::clone(&self.config);
        let cache = Arc::clone(&self.cache);
        let current_task_id = self.current_task_id;
        let event_handler = EventHandler {
            cache,
            config,
            current_task_id,
            tx,
            pubsub,
        };

        // Spawn event handler.
        tokio::spawn(async move {
            if let Err(err) = event_handler.handle().await {
                event!(Level::ERROR, "{}", err);
            }
        });

        Ok(())
    }
}

/// Builder for constructing Subscriber instances.
#[derive(Default)]
pub struct SubscriberBuilder<T: Cache> {
    /// Subscriber configuration
    config: Option<Arc<super::config::Subscriber>>,
    /// Event channel sender
    tx: Option<Sender<Event>>,
    /// Task identifier
    current_task_id: usize,
    /// Cache instance
    cache: Option<Arc<T>>,
}

impl<T: Cache> SubscriberBuilder<T>
where
    T: Default,
{
    /// Creates a new builder instance.
    pub fn new() -> SubscriberBuilder<T> {
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

    /// Sets the cache instance.
    pub fn cache(mut self, cache: Arc<T>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Builds the Subscriber instance.
    pub async fn build(self) -> Result<Subscriber<T>, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            cache: self
                .cache
                .ok_or_else(|| Error::MissingRequiredAttribute("cache".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
