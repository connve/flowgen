use super::message::NatsMessageExt;
use async_nats::jetstream::{self, AckKind};
use flowgen_core::{
    client::Client,
    config::ConfigExt,
    event::{new_completion_channel, CompletionRx, Event, EventExt},
    task::TaskConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{error, warn, Instrument};

/// How often a message in the flow is reported as in progress when the
/// consumer's ack wait is unknown.
const DEFAULT_PROGRESS_INTERVAL: Duration = Duration::from_secs(15);

/// `nats_jetstream_subscribe` task config.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, TaskConfig)]
#[serde(transparent)]
pub struct Subscribe(pub super::config::Config);

/// Errors that can occur during NATS JetStream subscription operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("NATS client error: {source}")]
    Client {
        #[source]
        source: crate::client::Error,
    },
    #[error("Message conversion error: {source}")]
    MessageConversion {
        #[source]
        source: crate::jetstream::message::Error,
    },
    #[error("Consumer creation error: {source}")]
    Consumer {
        #[source]
        source: async_nats::jetstream::stream::ConsumerError,
    },
    #[error("Failed to request messages: {source}")]
    Batch {
        #[source]
        source: async_nats::jetstream::consumer::pull::BatchError,
    },
    #[error("Stream management error: {source}")]
    StreamManagement {
        #[source]
        source: super::stream::Error,
    },
    #[error("Error getting stream: {source}")]
    GetStream {
        #[source]
        source: async_nats::jetstream::context::GetStreamError,
    },
    #[error("Subscription error: {source}")]
    Subscribe {
        #[source]
        source: async_nats::SubscribeError,
    },
    #[error("Consumer configuration check error")]
    ConsumerInfoFailed,
    #[error("Consumer '{consumer}' exists with different filter subject '{existing}', expected '{expected}'. Please delete the existing consumer or use a different durable name")]
    ConsumerFilterMismatch {
        consumer: String,
        existing: String,
        expected: String,
    },
    #[error("Missing stream configuration")]
    MissingStreamConfig,
    #[error("Durable name is required for JetStream consumers")]
    MissingDurableName,
    #[error("Other subscriber error")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Stream ended unexpectedly, connection may have been lost")]
    StreamEnded,
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// Event handler for processing NATS messages.
pub struct EventHandler {
    consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    /// Checked via `connection_state()` to distinguish a dead connection
    /// from a live consumer with nothing to deliver.
    client: async_nats::Client,
    /// Same key `init()` used to cache the NATS client, so `run()` can evict
    /// the exact entry that may be stale rather than recomputing it (and
    /// risking drift if `credentials_path`/`url` are templated).
    client_key: flowgen_core::client_registry::ClientKey,
    config: Arc<super::config::Config>,
    delivery: Arc<Delivery>,
}

/// What each message needs from its delivery to its ack.
struct Delivery {
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// The consumer's ack wait: how long NATS waits for an ack before redelivering.
    ack_wait: Duration,
    /// How long the flow may take before the message is redelivered.
    ack_timeout: Option<Duration>,
    backoff: Vec<Duration>,
}

impl EventHandler {
    /// Pulls messages only into free `max_in_flight` slots and hands each to
    /// its own task, so the next message is read while earlier ones are still
    /// in the flow and no pulled message waits unacknowledged in a buffer.
    async fn handle(self) -> Result<(), Error> {
        let max_in_flight = match self.config.max_in_flight {
            Some(max_in_flight) => max_in_flight,
            None => self.delivery.task_context.max_in_flight,
        };
        let slots = Arc::new(Semaphore::new(max_in_flight.get()));
        let cancellation = self.delivery.task_context.cancellation_token.clone();
        let mut throttle = self.config.throttle.map(|period| {
            let mut interval = tokio::time::interval(period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        });
        let mut in_flight = JoinSet::new();

        loop {
            let first = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                slot = Arc::clone(&slots).acquire_owned() => match slot {
                    Ok(slot) => slot,
                    Err(_) => return Ok(()),
                },
            };
            let mut permits = vec![first];
            while permits.len() < self.config.max_messages_per_batch {
                match Arc::clone(&slots).try_acquire_owned() {
                    Ok(permit) => permits.push(permit),
                    Err(_) => break,
                }
            }

            let mut batch = self
                .consumer
                .batch()
                .max_messages(permits.len())
                .expires(self.config.batch_expires)
                .heartbeat(self.config.batch_expires / 4)
                .messages()
                .await
                .map_err(|source| Error::Batch { source })?;

            let mut received = 0;
            loop {
                let next = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Ok(()),
                    next = batch.next() => next,
                };
                let message = match next {
                    Some(Ok(message)) => message,
                    Some(Err(source)) => {
                        while in_flight.join_next().await.is_some() {}
                        return Err(Error::Other(source));
                    }
                    None => break,
                };
                let Some(permit) = permits.pop() else {
                    break;
                };
                if let Some(throttle) = throttle.as_mut() {
                    throttle.tick().await;
                }
                received += 1;
                let span = tracing::info_span!("task.handle", duration_ms = tracing::field::Empty);
                in_flight.spawn(
                    Arc::clone(&self.delivery)
                        .process(message, permit)
                        .instrument(span),
                );
            }
            while in_flight.try_join_next().is_some() {}

            if received == 0
                && self.client.connection_state() == async_nats::connection::State::Disconnected
            {
                while in_flight.join_next().await.is_some() {}
                return Err(Error::StreamEnded);
            }
        }
    }
}

impl Delivery {
    /// Sends the message into the flow and acks it once every leaf has
    /// completed, sending progress acks meanwhile so NATS doesn't redeliver
    /// it while the flow is still running.
    async fn process(self: Arc<Self>, message: jetstream::Message, _slot: OwnedSemaphorePermit) {
        // The completion channel waits for every leaf of the flow's graph.
        let (completion_state, completion_rx) =
            new_completion_channel(self.task_context.leaf_count);
        let mut event = match message.to_event(self.task_type, self.task_id) {
            Ok(event) => event,
            Err(source) => {
                error!(error = %Error::MessageConversion { source }, "Failed to read the NATS message");
                self.nak(&message).await;
                return;
            }
        };
        event.completion_tx = Some(completion_state);
        if let Err(source) = event.send_with_logging(self.tx.as_ref()).await {
            error!(error = %Error::SendMessage { source }, "Failed to send the event to the next task");
            self.nak(&message).await;
            return;
        }

        match self.completed(&message, completion_rx).await {
            Some(true) => {
                if let Err(source) = message.double_ack().await {
                    error!(error = %source, "NATS did not confirm the ack, the message will be redelivered");
                }
            }
            Some(false) => {
                warn!("Flow completion failed or timed out");
                self.nak(&message).await;
            }
            None => {}
        }
    }

    /// Whether the flow completed for this message; `None` when the task is stopping.
    async fn completed(
        &self,
        message: &jetstream::Message,
        mut completion_rx: CompletionRx,
    ) -> Option<bool> {
        let every = match self.ack_wait / 2 {
            every if every.is_zero() => DEFAULT_PROGRESS_INTERVAL,
            every => every,
        };
        let mut progress = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        let deadline = async {
            match self.ack_timeout {
                Some(ack_timeout) => tokio::time::sleep(ack_timeout).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                _ = self.task_context.cancellation_token.cancelled() => return None,
                result = &mut completion_rx => return Some(matches!(result, Ok(Ok(_)))),
                _ = &mut deadline => return Some(false),
                _ = progress.tick() => {
                    if let Err(source) = message.ack_with(AckKind::Progress).await {
                        warn!(error = %source, "Failed to tell NATS the message is still in progress");
                    }
                }
            }
        }
    }

    /// Asks NATS to redeliver the message after the `backoff` delay for this
    /// delivery, or after the ack wait without a `backoff`.
    async fn nak(&self, message: &jetstream::Message) {
        let delivered = match message.info() {
            Ok(info) => usize::try_from(info.delivered).unwrap_or(1),
            Err(_) => 1,
        };
        let delay = match self
            .backoff
            .get(delivered.saturating_sub(1))
            .or(self.backoff.last())
        {
            Some(delay) => *delay,
            None => self.ack_wait,
        };
        if let Err(source) = message.ack_with(AckKind::Nak(Some(delay))).await {
            warn!(error = %source, "Failed to ask NATS to redeliver the message");
        }
    }
}

/// NATS JetStream subscriber that consumes messages and converts them to flowgen events.
#[derive(Debug)]
pub struct Processor {
    /// Subscriber configuration including stream and consumer settings.
    config: Arc<super::config::Config>,
    /// Sender for forwarding converted events.
    tx: Option<Sender<Event>>,
    /// Task identifier for event tagging.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the subscriber by establishing connection and creating consumer.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Connecting to NATS with credentials
    /// - Getting or creating JetStream stream and consumer
    /// - Validating consumer configuration
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let nats_key = flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
            .field("credentials_path", &init_config.credentials_path)
            .field("url", &init_config.url)
            .build();
        let jetstream_ctx = self
            .task_context
            .client_registry
            .get_or_init(nats_key.clone(), || {
                let credentials_path = init_config.credentials_path.clone();
                let url = init_config.url.clone();
                async move {
                    let mut builder = crate::client::ClientBuilder::new();
                    builder.url(url);
                    if let Some(path) = credentials_path {
                        builder.credentials_path(path);
                    }
                    let client = builder
                        .build()
                        .map_err(|source| Error::Client { source })?
                        .connect()
                        .await
                        .map_err(|source| Error::Client { source })?;
                    client.jetstream.ok_or_else(|| {
                        Error::Other("JetStream context not available".to_string().into())
                    })
                }
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        {
            let mut jetstream = (*jetstream_ctx).clone();
            let stream_opts = self
                .config
                .stream
                .as_ref()
                .ok_or_else(|| Error::MissingStreamConfig)?;

            // Set timeout on JetStream context if configured.
            if let Some(timeout) = init_config.ack_timeout {
                jetstream.set_timeout(timeout);
            }

            let jetstream = match stream_opts.create_or_update {
                true => super::stream::create_or_update_stream(jetstream, stream_opts)
                    .await
                    .map_err(|source| Error::StreamManagement { source })?,
                false => jetstream,
            };

            let stream = jetstream
                .get_stream(&stream_opts.name)
                .await
                .map_err(|source| Error::GetStream { source })?;

            let durable_name = init_config
                .durable_name
                .as_ref()
                .ok_or_else(|| Error::MissingDurableName)?;

            let mut consumer_config = jetstream::consumer::pull::Config {
                durable_name: Some(durable_name.clone()),
                filter_subject: init_config.subject.clone(),
                ..Default::default()
            };

            if let Some(ack_timeout) = init_config.ack_timeout {
                consumer_config.ack_wait = ack_timeout;
            }

            if let Some(max_ack_pending) = init_config.max_ack_pending {
                consumer_config.max_ack_pending = max_ack_pending;
            }

            if let Some(max_waiting) = init_config.max_waiting {
                consumer_config.max_waiting = max_waiting;
            }

            if let Some(max_deliver) = init_config.max_deliver {
                consumer_config.max_deliver = max_deliver;
            }

            if !init_config.backoff.is_empty() {
                consumer_config.backoff = init_config.backoff.clone();
            }

            let consumer = match stream.get_consumer(durable_name).await {
                Ok(mut existing_consumer) => {
                    let consumer_info = existing_consumer
                        .info()
                        .await
                        .map_err(|_| Error::ConsumerInfoFailed)?;
                    let current_filter = consumer_info.config.filter_subject.clone();

                    if current_filter != init_config.subject {
                        return Err(Error::ConsumerFilterMismatch {
                            consumer: durable_name.clone(),
                            existing: current_filter,
                            expected: init_config.subject.clone(),
                        });
                    } else {
                        existing_consumer
                    }
                }
                Err(_) => stream
                    .create_consumer(consumer_config)
                    .await
                    .map_err(|e| Error::Consumer { source: e })?,
            };

            let delivery = Delivery {
                tx: self.tx.clone(),
                task_id: self.task_id,
                task_type: self.task_type,
                task_context: Arc::clone(&self.task_context),
                ack_wait: consumer.cached_info().config.ack_wait,
                ack_timeout: init_config.ack_timeout,
                backoff: init_config.backoff.clone(),
            };
            Ok(EventHandler {
                client: jetstream_ctx.client(),
                client_key: nats_key,
                consumer,
                config: Arc::clone(&self.config),
                delivery: Arc::new(delivery),
            })
        }
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let cancellation_token = self.task_context.cancellation_token.clone();
        let mut reconnect_backoff = retry_config.reconnect_strategy();

        let mut lost_connectivity = false;

        // Infinite retry loop: subscribers must maintain connectivity indefinitely.
        loop {
            if cancellation_token.is_cancelled() {
                return Ok(());
            }

            // Initialize with circuit breaker to detect permanent errors (bad config, filter mismatch, etc.).
            let init_future = tokio_retry::Retry::spawn(
                retry_config.init_strategy(self.task_context.startup_delay),
                || async {
                    match self.init().await {
                        Ok(handler) => Ok(handler),
                        Err(e) => {
                            let is_retriable = !matches!(&e, Error::ConsumerFilterMismatch { .. });

                            if is_retriable {
                                error!(error = %e, "Subscriber initialization failed");
                                Err(tokio_retry::RetryError::transient(e))
                            } else {
                                error!(error = %e, "Permanent initialization error");
                                Err(tokio_retry::RetryError::permanent(e))
                            }
                        }
                    }
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

            // Run event loop until failure, then reinitialize.
            let client_key = event_handler.client_key.clone();
            let handle_future = event_handler.handle();
            tokio::select! {
                _ = cancellation_token.cancelled() => return Ok(()),
                result = handle_future => match result {
                    Ok(()) => warn!("Subscriber lost connectivity, reinitializing"),
                    Err(e) => error!(error = %e, "Subscriber lost connectivity, reinitializing"),
                },
            }
            lost_connectivity = true;

            // Evict the cached NATS client so the next init() creates a fresh connection
            // instead of reusing the dead one from the registry.
            self.task_context.client_registry.remove(&client_key).await;

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

/// Builder for configuring and creating NATS JetStream subscribers.
#[derive(Default)]
pub struct ProcessorBuilder {
    /// Optional subscriber configuration.
    config: Option<Arc<super::config::Config>>,
    /// Optional event sender.
    tx: Option<Sender<Event>>,
    /// Task identifier for event processing.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization.
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Config>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
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

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
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
    use serde_json::{Map, Value};
    use std::{path::PathBuf, time::Duration};
    use tokio::sync::mpsc;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
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
    async fn test_subscriber_builder() {
        let config = Arc::new(super::super::config::Config {
            name: "test_subscriber".to_string(),
            credentials_path: Some(PathBuf::from("/test/creds.jwt")),
            subject: "test.subject".to_string(),
            stream: Some(super::super::config::StreamOptions {
                name: "test_stream".to_string(),
                subjects: vec!["test.>".to_string()],
                create_or_update: true,
                retention: Some(super::super::config::RetentionPolicy::Limits),
                discard: Some(super::super::config::DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("test_consumer".to_string()),
            max_messages_per_batch: 100,
            delay: Some(Duration::from_secs(5)),
            throttle: None,
            ..Default::default()
        });
        let (tx, _rx) = mpsc::channel(100);

        // Success case.
        let subscriber = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .task_id(1)
            .task_type("test_task")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(subscriber.is_ok());

        // Error case - missing config.
        let (tx2, _rx2) = mpsc::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_subscriber_builder_build_missing_task_context() {
        let config = Arc::new(super::super::config::Config {
            name: "test_subscriber".to_string(),
            credentials_path: Some(PathBuf::from("/test/creds.jwt")),
            subject: "test.subject".to_string(),
            stream: Some(super::super::config::StreamOptions {
                name: "test_stream".to_string(),
                subjects: vec!["test.>".to_string()],
                create_or_update: true,
                retention: Some(super::super::config::RetentionPolicy::Limits),
                discard: Some(super::super::config::DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("test_consumer".to_string()),
            max_messages_per_batch: 50,
            delay: None,
            throttle: None,
            ..Default::default()
        });
        let (tx, _rx) = mpsc::channel(100);

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingBuilderAttribute(attr) if attr == "task_context")
        );
    }
}
