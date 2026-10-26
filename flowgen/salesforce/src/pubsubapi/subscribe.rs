//! The `salesforce_pubsubapi_subscribe` task: subscribes to a Pub/Sub API
//! topic and emits each event, reconnecting whenever the stream breaks.

use crate::processor::{Error, PubSub};
use flowgen_core::{
    config::ConfigExt,
    event::{new_completion_channel, AvroData, Event, EventBuilder, EventData, EventExt},
    task::TaskConfig,
};
use salesforce_core::pubsubapi::{
    eventbus::v1::{ConsumerEvent, ManagedFetchRequest, SchemaInfo},
    FetchRequest, PubSubError, SchemaRequest, TopicRequest,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tracing::{error, warn, Instrument};

const DEFAULT_NUM_REQUESTED: i32 = 200;
const DEFAULT_TOPIC_PREFIX_DATA: &str = "/data/";
const DEFAULT_TOPIC_PREFIX_EVENT: &str = "/event/";

/// Configuration for subscribing to a Salesforce Pub/Sub API topic.
///
/// # Examples
///
/// ```yaml
/// salesforce_pubsubapi_subscribe:
///   name: opportunity_changes
///   credentials_path: /path/to/salesforce_creds.json
///   topic:
///     name: /data/OpportunityChangeEvent
///     num_requested: 25
///     durable_consumer_options:
///       enabled: true
///       managed_subscription: false
///       name: OpportunityProcessor
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Subscribe {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to credentials file containing Salesforce authentication details.
    pub credentials_path: PathBuf,
    /// Topic configuration including name and subscription options.
    pub topic: Topic,
    /// Optional Salesforce Pub/Sub endpoint (e.g., "api.pubsub.salesforce.com:7443" or "api.deu.pubsub.salesforce.com:7443").
    pub endpoint: Option<String>,
    /// Timeout for waiting on flow completion before considering the event failed.
    /// If not specified, waits indefinitely for flow completion.
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<std::time::Duration>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Subscribe {}

/// A Pub/Sub topic and how it is read.
///
/// Topic names: `/event/CustomEvent__e` for platform events,
/// `/data/AccountChangeEvent` for change data capture.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Topic {
    /// Full topic name including namespace (e.g., "/event/Account_Change__e").
    pub name: String,
    /// Optional durable consumer configuration for reliable message processing.
    pub durable_consumer_options: Option<DurableConsumerOptions>,
    /// Number of messages to request per batch (server default if not specified).
    pub num_requested: Option<i32>,
}

/// Where to start reading events when no replay ID is available.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplayPreset {
    /// Start from the latest events (default).
    #[default]
    Latest,
    /// Start from the earliest available events in the retention window.
    Earliest,
}

impl ReplayPreset {
    /// The preset's value in the gRPC API.
    pub fn to_i32(&self) -> i32 {
        match self {
            ReplayPreset::Latest => 0,
            ReplayPreset::Earliest => 1,
        }
    }
}

/// Durable consumer options: the position in the topic is kept, by
/// Salesforce for a managed subscription, or as a cached replay ID.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableConsumerOptions {
    /// Whether to enable durable consumer functionality for reliable message processing.
    pub enabled: bool,
    /// Whether Salesforce should automatically manage the subscription lifecycle.
    pub managed_subscription: bool,
    /// Unique name for this durable consumer subscription (must be unique within the org).
    pub name: String,
    /// Where to start reading events when no replay ID is cached (defaults to Latest).
    #[serde(default)]
    pub replay_preset: ReplayPreset,
}

/// Sanitizes a topic name to create a valid cache key.
/// Removes leading/trailing slashes and replaces internal slashes with hyphens.
fn sanitize_topic_name(topic_name: &str) -> String {
    topic_name.trim_matches('/').replace('/', "-")
}

/// Checks if a gRPC error is due to an invalid/corrupted replay ID.
fn is_replay_id_error(error: &PubSubError) -> bool {
    if let PubSubError::Tonic(status) = error {
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

/// Processes events from a single Salesforce Pub/Sub topic.
///
/// Subscribes to a topic, deserializes Avro payloads, and forwards events
/// to the event channel. Supports durable consumers with replay ID caching.
pub struct EventHandler {
    /// Pub/Sub client and the registry keys to evict on reconnect: the cached
    /// channel or client may be what just died, and leaving them in place
    /// would hand every retry the same dead connection.
    pubsub: PubSub,
    /// Subscriber configuration
    config: Arc<Subscribe>,
    /// Channel sender for processed events
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes a batch of events from Salesforce Pub/Sub.
    /// Events are processed concurrently, but replay_id is only cached after ALL events succeed.
    async fn process_events(
        &self,
        events: Vec<ConsumerEvent>,
        schema_info: &SchemaInfo,
        topic_name: &str,
    ) -> Result<(), Error> {
        if events.is_empty() {
            return Ok(());
        }

        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let cache = Arc::clone(&self.task_context.cache);
        let flow_name = self.task_context.flow.identity().to_string();
        // `flow_key` keys the cache; `flow_name` stays human-readable for logs.
        let flow_key = self.task_context.flow.id();
        let mut handles = Vec::new();
        let last_replay_id = events.last().map(|ce| ce.replay_id.clone());

        for ce in events {
            if self.task_context.cancellation_token.is_cancelled() {
                break;
            }

            if let Some(event) = ce.event {
                let tx = self.tx.clone();
                let task_id = self.task_id;
                let task_type = self.task_type;
                let schema = schema_info.schema_json.clone();
                let event_id = event.id.clone();

                // Normalize topic name by removing data/ or event/ prefix.
                let subject = topic_name
                    .strip_prefix(DEFAULT_TOPIC_PREFIX_DATA)
                    .or_else(|| topic_name.strip_prefix(DEFAULT_TOPIC_PREFIX_EVENT))
                    .unwrap_or(topic_name)
                    .to_lowercase();

                let config = Arc::clone(&self.config);
                let cancellation_token = self.task_context.cancellation_token.clone();
                let leaf_count = self.task_context.leaf_count;

                // Spawn a task for each event to process them concurrently.
                let handle = tokio::spawn(
                    async move {
                        if cancellation_token.is_cancelled() {
                            return Ok(());
                        }

                        // Size the completion channel to the number of leaves
                        // in this flow. The Pub/Sub event is acked only after
                        // every leaf has signalled completion.
                        let (completion_state, completion_rx) = new_completion_channel(leaf_count);

                        let data = AvroData {
                            schema,
                            raw_bytes: event.payload[..].to_vec(),
                        };

                        let mut e = EventBuilder::new()
                            .data(EventData::Avro(data))
                            .subject(subject)
                            .id(event.id)
                            .task_id(task_id)
                            .task_type(task_type)
                            .build()?;

                        e.completion_tx = Some(completion_state);

                        // Check cancellation before sending to prevent "receiver dropped" errors.
                        if cancellation_token.is_cancelled() {
                            return Ok(());
                        }

                        e.send_with_logging(tx.as_ref())
                            .await
                            .map_err(|source| Error::SendMessage { source })?;

                        // Failed flows skip replay_id advancement, allowing next fetch to retry from same position.
                        let success = match config.ack_timeout {
                            Some(timeout) => matches!(
                                tokio::time::timeout(timeout, completion_rx).await,
                                Ok(Ok(Ok(_)))
                            ),
                            None => matches!(completion_rx.await, Ok(Ok(_))),
                        };

                        if !success {
                            warn!(
                                event_id = ?event_id,
                                "Flow completion failed or timed out"
                            );
                        }

                        Ok::<(), Error>(())
                    }
                    .instrument(tracing::info_span!(
                        parent: tracing::Span::current(),
                        "task.handle",
                        duration_ms = tracing::field::Empty,
                    )),
                );

                handles.push(handle);
            }
        }

        // Wait for all events in the batch to complete.
        let mut all_succeeded = true;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!("Event processing failed: {}", e);
                    all_succeeded = false;
                }
                Err(e) => {
                    error!("Event processing task panicked: {}", e);
                    all_succeeded = false;
                }
            }
        }

        if all_succeeded && !self.task_context.cancellation_token.is_cancelled() {
            if let Some(replay_id) = last_replay_id {
                if self
                    .config
                    .topic
                    .durable_consumer_options
                    .as_ref()
                    .is_some_and(|opts| opts.enabled && !opts.managed_subscription)
                {
                    let sanitized_topic = sanitize_topic_name(topic_name);
                    let cache_key = format!("flow.{flow_key}.replay_id.{sanitized_topic}");
                    if let Err(e) = cache.put(&cache_key, replay_id.into(), None).await {
                        error!(
                            "Failed to cache replay_id for flow {flow_name} topic {topic_name}: {e}"
                        );
                    }
                }
            }
        } else {
            warn!("Not all events succeeded in batch, replay_id not advanced");
        }

        Ok(())
    }

    /// Runs the topic listener to process events from Salesforce Pub/Sub.
    ///
    /// Fetches topic and schema info, establishes subscription with optional
    /// replay ID, then processes incoming events in a loop.
    async fn handle(self) -> Result<(), Error> {
        let cache = &self.task_context.cache;
        let flow_name = self.task_context.flow.identity();
        // `flow_key` keys the cache; `flow_name` stays human-readable for logs.
        let flow_key = self.task_context.flow.id();
        let pubsub = &self.pubsub.client;
        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.name.clone(),
            })
            .await
            .map_err(|source| Error::PubSub { source })?
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(|source| Error::PubSub { source })?
            .into_inner();

        let num_requested = match self.config.topic.num_requested {
            Some(num_requested) => num_requested,
            None => DEFAULT_NUM_REQUESTED,
        };

        let topic_name = topic_info.topic_name.as_str();

        let managed_subscription = self
            .config
            .topic
            .durable_consumer_options
            .as_ref()
            .filter(|opts| opts.enabled && opts.managed_subscription);

        if let Some(durable_consumer_opts) = managed_subscription {
            let managed_request = ManagedFetchRequest {
                developer_name: durable_consumer_opts.name.clone(),
                num_requested,
                ..Default::default()
            };

            let mut stream = pubsub
                .lock()
                .await
                .managed_subscribe(managed_request)
                .await
                .map_err(|source| Error::PubSub { source })?
                .into_inner();

            while let Some(event) = stream.next().await {
                if self.task_context.cancellation_token.is_cancelled() {
                    return Ok(());
                }

                let events = match event {
                    Ok(fr) => fr.events,
                    Err(e) => {
                        return Err(Error::PubSub {
                            source: PubSubError::Tonic(Box::new(e)),
                        });
                    }
                };

                self.process_events(events, &schema_info, topic_name)
                    .await?;

                if self.task_context.cancellation_token.is_cancelled() {
                    return Ok(());
                }
            }

            return Err(Error::StreamEnded);
        }

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
            .filter(|opts| opts.enabled && !opts.managed_subscription)
        {
            let sanitized_topic = sanitize_topic_name(topic_name);
            let cache_key = format!("flow.{flow_key}.replay_id.{sanitized_topic}");
            let cached_replay_id = cache.get(&cache_key).await.ok();

            match cached_replay_id.flatten() {
                Some(reply_id) => {
                    fetch_request.replay_id = reply_id.into();
                    fetch_request.replay_preset = 2;
                }
                None => {
                    fetch_request.replay_preset = durable_consumer_opts.replay_preset.to_i32();
                    warn!(
                        "No cached replay_id found for flow {flow_name} topic {topic_name}, starting from {:?}",
                        &durable_consumer_opts.replay_preset
                    );
                }
            }
        }

        let subscribed = pubsub.lock().await.subscribe(fetch_request.clone()).await;
        let mut stream = match subscribed {
            Ok(response) => response.into_inner(),
            Err(e) if crate::processor::is_pubsub_auth_error(&e) => {
                // Reconnect to get a fresh OAuth token, then subscribe once more.
                pubsub
                    .lock()
                    .await
                    .reconnect()
                    .await
                    .map_err(|source| Error::PubSub { source })?;
                pubsub
                    .lock()
                    .await
                    .subscribe(fetch_request.clone())
                    .await
                    .map_err(|source| Error::PubSub { source })?
                    .into_inner()
            }
            Err(e) if !fetch_request.replay_id.is_empty() && is_replay_id_error(&e) => {
                // Clear the invalid replay_id so the next attempt starts fresh.
                warn!("Invalid replay_id detected, clearing from cache and retrying");
                let sanitized_topic = sanitize_topic_name(topic_name);
                let cache_key = format!("flow.{flow_key}.replay_id.{sanitized_topic}");
                if let Err(e) = cache.delete(&cache_key).await {
                    error!(
                        "Failed to delete invalid replay_id from cache for flow {} topic {}: {}",
                        flow_name, topic_name, e
                    );
                }
                return Err(Error::PubSub { source: e });
            }
            Err(source) => return Err(Error::PubSub { source }),
        };

        while let Some(event) = stream.next().await {
            if self.task_context.cancellation_token.is_cancelled() {
                return Ok(());
            }

            let events = match event {
                Ok(fr) => fr.events,
                Err(e) => {
                    return Err(Error::PubSub {
                        source: PubSubError::Tonic(Box::new(e)),
                    });
                }
            };

            self.process_events(events, &schema_info, topic_name)
                .await?;

            if self.task_context.cancellation_token.is_cancelled() {
                return Ok(());
            }
        }

        Err(Error::StreamEnded)
    }
}

/// Subscribes to a Salesforce Pub/Sub topic, reconnecting with backoff
/// whenever the stream fails.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Subscribe>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self
            .config
            .render(&serde_json::Map::new())
            .map_err(|source| Error::ConfigRender { source })?;
        let pubsub = PubSub::connect(
            &self.task_context,
            self.task_type,
            &config.credentials_path,
            config.endpoint.as_deref(),
        )
        .await?;

        Ok(EventHandler {
            pubsub,
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let cancellation_token = self.task_context.cancellation_token.clone();
        let mut reconnect_backoff = retry_config.reconnect_strategy();

        let mut lost_connectivity = false;

        // Subscribers must keep connectivity for as long as the task runs.
        loop {
            if cancellation_token.is_cancelled() {
                return Ok(());
            }

            let init_future = tokio_retry::Retry::spawn(
                retry_config.init_strategy(self.task_context.startup_delay),
                || async {
                    self.init().await.map_err(|e| {
                        error!(error = %e, "Subscriber initialization failed");
                        tokio_retry::RetryError::transient(e)
                    })
                },
            );

            let event_handler = tokio::select! {
                _ = cancellation_token.cancelled() => return Ok(()),
                result = init_future => match result {
                    Ok(handler) => {
                        reconnect_backoff = retry_config.reconnect_strategy();
                        if lost_connectivity {
                            warn!("Subscriber reconnected successfully");
                        }
                        handler
                    }
                    Err(e) => {
                        let delay = match reconnect_backoff.next() {
                            Some(d) => d,
                            None => retry_config.initial_backoff,
                        };
                        error!(error = %e, delay_ms = %delay.as_millis(), "Subscriber initialization exhausted retry attempts, will retry after backoff");
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = cancellation_token.cancelled() => return Ok(()),
                        }
                        continue;
                    }
                },
            };

            // Run the event loop until it fails, then reinitialize.
            let client_key = event_handler.pubsub.client_key.clone();
            let channel_key = event_handler.pubsub.channel_key.clone();
            tokio::select! {
                _ = cancellation_token.cancelled() => return Ok(()),
                result = event_handler.handle() => match result {
                    Ok(()) => warn!("Subscriber lost connectivity, reinitializing"),
                    Err(e) => error!(error = %e, "Subscriber lost connectivity, reinitializing"),
                },
            }
            lost_connectivity = true;

            self.task_context.client_registry.remove(&client_key).await;
            self.task_context.client_registry.remove(&channel_key).await;

            let delay = match reconnect_backoff.next() {
                Some(d) => d,
                None => retry_config.initial_backoff,
            };
            warn!(delay_ms = %delay.as_millis(), "Reconnect backoff");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = cancellation_token.cancelled() => return Ok(()),
            }
        }
    }
}

/// Builds a [`Processor`].
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<Subscribe>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    /// An empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// The task's config.
    pub fn config(mut self, config: Arc<Subscribe>) -> Self {
        self.config = Some(config);
        self
    }

    /// Where the task sends events.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// The task's position in its flow.
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    /// The flow's shared context: cache, retry defaults, cancellation.
    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// The task type, as written in the flow.
    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// The processor, or the first missing attribute.
    pub fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or(Error::MissingBuilderAttribute("config"))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or(Error::MissingBuilderAttribute("task_context"))?,
            task_type: self
                .task_type
                .ok_or(Error::MissingBuilderAttribute("task_type"))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_topic_name() {
        assert_eq!(sanitize_topic_name("/event/Test__e"), "event-Test__e");
        assert_eq!(sanitize_topic_name("/data/MyData/"), "data-MyData");
        assert_eq!(
            sanitize_topic_name("/event/BulkApi2JobEvent"),
            "event-BulkApi2JobEvent"
        );
        assert_eq!(sanitize_topic_name("simple"), "simple");
        assert_eq!(sanitize_topic_name("///"), "");
    }

    #[test]
    fn a_builder_without_a_config_fails() {
        let result = ProcessorBuilder::new().task_type("test").build();
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute("config"))
        ));
    }

    #[test]
    fn test_subscriber_config_serialization() {
        let subscriber = Subscribe {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: Topic {
                name: "/event/Test__e".to_string(),
                durable_consumer_options: Some(DurableConsumerOptions {
                    enabled: true,
                    managed_subscription: false,
                    name: "TestConsumer".to_string(),
                    replay_preset: ReplayPreset::Latest,
                }),
                num_requested: Some(50),
            },
            endpoint: Some("api.pubsub.salesforce.com:7443".to_string()),
            ack_timeout: None,
            depends_on: None,
            retry: None,
        };

        let json = serde_json::to_string(&subscriber).unwrap();
        let deserialized: Subscribe = serde_json::from_str(&json).unwrap();
        assert_eq!(subscriber, deserialized);
    }

    #[test]
    fn replay_presets_map_to_the_grpc_values() {
        assert_eq!(ReplayPreset::Latest.to_i32(), 0);
        assert_eq!(ReplayPreset::Earliest.to_i32(), 1);
        assert_eq!(
            serde_json::from_str::<ReplayPreset>("\"earliest\"").unwrap(),
            ReplayPreset::Earliest
        );
    }
}
