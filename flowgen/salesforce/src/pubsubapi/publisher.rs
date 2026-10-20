//! The `salesforce_pubsubapi_publisher` task: publishes each event to a
//! Pub/Sub API topic, rebuilding the connection after a publish fails.

use crate::processor::{failure, Error, Failure, Payload, PubSub};
use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use chrono::Utc;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventData, EventExt};
use flowgen_core::task::runner::Runner;
use flowgen_core::task::TaskConfig;
use futures_util::future;
use salesforce_core::pubsubapi::{ProducerEvent, PublishRequest, SchemaRequest, TopicRequest};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn, Instrument};

/// Configuration for publishing events to a Salesforce Pub/Sub API topic.
///
/// # Examples
///
/// ```yaml
/// salesforce_pubsubapi_publisher:
///   name: order_status
///   credentials_path: /path/to/salesforce_creds.json
///   topic: /event/Order_Status__e
///   payload:
///     Order_ID__c: "{{event.data.order_id}}"
///     Status__c: Shipped
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Publisher {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to credentials file containing Salesforce authentication details.
    pub credentials_path: PathBuf,
    /// Target topic name for publishing events (e.g., "/event/CustomEvent__e").
    pub topic: String,
    /// Event payload: either explicit field values or from_event to use incoming event data.
    pub payload: Payload,
    /// Optional Salesforce Pub/Sub endpoint (e.g., "api.pubsub.salesforce.com:7443" or "api.deu.pubsub.salesforce.com:7443").
    pub endpoint: Option<String>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for Publisher {}

/// Publishes events to one Salesforce Pub/Sub topic.
pub struct EventHandler {
    config: Arc<Publisher>,
    /// Pub/Sub client and the registry keys a rebuild after a failed publish
    /// evicts, so the replacement doesn't get the same dead connection.
    pubsub: PubSub,
    topic: String,
    schema_id: String,
    schema: Arc<AvroSchema>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    task_type: &'static str,
}

impl EventHandler {
    /// Publishes `event` and emits the publish response.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let event = Arc::new(event);
        let completion_tx = event.completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            let config = self
                .config
                .render_for_event(&event)
                .map_err(|source| Error::ConfigRender { source })?;

            let mut fields = config.payload.fields(&event)?;
            fields.insert(
                "CreatedDate".to_string(),
                Value::from(Utc::now().timestamp_millis()),
            );

            let record = AvroValue::from(Value::Object(fields))
                .resolve(self.schema.as_ref())
                .map_err(|source| Error::Avro { source })?;
            // Salesforce expects the datum alone, without a schema header.
            let payload = apache_avro::to_avro_datum(self.schema.as_ref(), record)
                .map_err(|source| Error::Avro { source })?;

            let response = self
                .pubsub
                .client
                .lock()
                .await
                .publish(PublishRequest {
                    topic_name: self.topic.clone(),
                    events: vec![ProducerEvent {
                        schema_id: self.schema_id.clone(),
                        payload,
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .await
                .map_err(|source| Error::PubSub { source })?
                .into_inner();

            let subject = match self
                .topic
                .replace('/', ".")
                .to_lowercase()
                .strip_prefix('.')
            {
                Some(stripped) => stripped.to_string(),
                None => self.topic.to_owned(),
            };
            let data = crate::processor::to_json(&response)?;

            let mut e = flowgen_core::event::EventBuilder::new()
                .data(EventData::Json(data))
                .subject(subject)
                .id(response.rpc_id)
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()?;

            match self.tx {
                None => {
                    if let Some(completion) = completion_tx.as_ref() {
                        completion.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => e.completion_tx = completion_tx,
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })
        })
        .await
    }
}

/// Receives events and publishes them to the configured topic.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Publisher>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

impl Processor {
    /// Builds an `EventHandler`, retrying on failure with the init strategy.
    /// Used both for the initial connection and to rebuild it after a publish
    /// exhausts its retries.
    async fn init_retrying(
        &self,
        retry_config: &flowgen_core::retry::RetryConfig,
    ) -> Result<Arc<EventHandler>, Error> {
        tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                self.init().await.map_err(|e| {
                    error!(error = %e, "Failed to initialize publisher");
                    tokio_retry::RetryError::transient(e)
                })
            },
        )
        .await
        .map(Arc::new)
    }
}

#[async_trait::async_trait]
impl Runner for Processor {
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

        let topic_info = pubsub
            .client
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: config.topic.clone(),
            })
            .await
            .map_err(|source| Error::PubSub { source })?
            .into_inner();

        let schema_info = pubsub
            .client
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(|source| Error::PubSub { source })?
            .into_inner();

        let schema = AvroSchema::parse_str(&schema_info.schema_json)
            .map_err(|source| Error::Avro { source })?;

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            pubsub,
            topic: config.topic,
            schema_id: schema_info.schema_id,
            schema: Arc::new(schema),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        // The loop holds the receiver mutably while `init_retrying` borrows
        // the rest of `self` to rebuild, so it moves out here. The channel
        // left behind is never read.
        let mut rx = std::mem::replace(&mut self.rx, tokio::sync::mpsc::channel(1).1);

        let mut event_handler = self.init_retrying(&retry_config).await?;

        // Set by a publish that exhausted its retries. The gRPC channel is
        // established once at init and never repaired in place, so a dropped
        // connection would otherwise leave this task publishing into a dead
        // transport until the pod restarts. Rebuilding before the next event
        // reconnects instead, as the subscriber does after its stream fails.
        let needs_reinit = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let max_in_flight = match self.config.max_in_flight {
            Some(max_in_flight) => max_in_flight,
            None => self.task_context.max_in_flight,
        };
        let slots = Arc::new(tokio::sync::Semaphore::new(max_in_flight.get()));
        let mut handlers = Vec::new();

        loop {
            let slot = tokio::select! {
                biased;
                _ = self.task_context.cancellation_token.cancelled() => None,
                slot = Arc::clone(&slots).acquire_owned() => slot.ok(),
            };
            let event = match slot {
                Some(_) => tokio::select! {
                    biased;
                    _ = self.task_context.cancellation_token.cancelled() => None,
                    event = rx.recv() => event,
                },
                None => None,
            };

            let (slot, event) = match (slot, event) {
                (Some(slot), Some(event)) => (slot, event),
                _ => {
                    rx.close();
                    future::join_all(handlers).await;
                    return Ok(());
                }
            };

            if needs_reinit.swap(false, std::sync::atomic::Ordering::AcqRel) {
                warn!("Rebuilding publisher connection after failed publish");
                self.task_context
                    .client_registry
                    .remove(&event_handler.pubsub.channel_key)
                    .await;
                self.task_context
                    .client_registry
                    .remove(&event_handler.pubsub.client_key)
                    .await;
                let rebuilt = tokio::select! {
                    _ = self.task_context.cancellation_token.cancelled() => {
                        future::join_all(handlers).await;
                        return Ok(());
                    }
                    result = self.init_retrying(&retry_config) => result,
                };
                match rebuilt {
                    Ok(handler) => event_handler = handler,
                    Err(e) => {
                        // Keep the old handler and let the next event try
                        // again: a publisher that gives up here would stay
                        // dead for the pod's lifetime, which is the failure
                        // this rebuild exists to prevent.
                        error!(error = %e, "Failed to rebuild publisher connection, will retry on next event");
                        needs_reinit.store(true, std::sync::atomic::Ordering::Release);
                    }
                }
            }

            let event_handler = Arc::clone(&event_handler);
            let needs_reinit = Arc::clone(&needs_reinit);
            let retry_strategy = retry_config.strategy();
            let handle = tokio::spawn(
                async move {
                    let _slot = slot;
                    let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                        let e = match event_handler.handle(event.clone()).await {
                            Ok(()) => return Ok(()),
                            Err(e) => e,
                        };
                        error!(error = %e, "Failed to publish message");
                        match failure(&e) {
                            Failure::Auth => {
                                if let Err(source) =
                                    event_handler.pubsub.client.lock().await.reconnect().await
                                {
                                    return Err(tokio_retry::RetryError::transient(
                                        Error::PubSub { source },
                                    ));
                                }
                                Err(tokio_retry::RetryError::transient(e))
                            }
                            Failure::Transient => Err(tokio_retry::RetryError::transient(e)),
                            Failure::Permanent => Err(tokio_retry::RetryError::permanent(e)),
                        }
                    })
                    .await;

                    if let Err(err) = result {
                        error!(error = %err, "Failed to publish message after all retry attempts");
                        needs_reinit.store(true, std::sync::atomic::Ordering::Release);
                        event
                            .forward_failure(event_handler.tx.as_ref(), err.to_string())
                            .await;
                    }
                }
                .instrument(tracing::Span::current()),
            );
            handlers.push(handle);
            handlers.retain(|h| !h.is_finished());
        }
    }
}

/// Builds a [`Processor`].
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<Publisher>>,
    rx: Option<Receiver<Event>>,
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
    pub fn config(mut self, config: Arc<Publisher>) -> Self {
        self.config = Some(config);
        self
    }

    /// Where the task receives events.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Where the task sends events; unset for the last task of a flow.
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
            rx: self.rx.ok_or(Error::MissingBuilderAttribute("receiver"))?,
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
    use serde_json::Map;

    #[test]
    fn a_builder_without_a_config_fails() {
        let (_tx, rx) = tokio::sync::mpsc::channel::<Event>(1);
        let result = ProcessorBuilder::new()
            .receiver(rx)
            .task_type("test")
            .build();
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute("config"))
        ));
    }

    #[test]
    fn test_publisher_config_serialization() {
        for payload in [
            Payload::Fields(Map::from_iter([(
                "Status__c".to_string(),
                Value::from("Shipped"),
            )])),
            Payload::FromEvent { from_event: true },
        ] {
            let publisher = Publisher {
                name: "order_publisher".to_string(),
                credentials_path: PathBuf::from("sf_creds"),
                topic: "/event/Order_Status__e".to_string(),
                payload,
                endpoint: Some("api.pubsub.salesforce.com:7443".to_string()),
                depends_on: None,
                retry: None,
                max_in_flight: None,
            };

            let json = serde_json::to_string(&publisher).unwrap();
            let deserialized: Publisher = serde_json::from_str(&json).unwrap();
            assert_eq!(publisher, deserialized);
        }
    }
}
