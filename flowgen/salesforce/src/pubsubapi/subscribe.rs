//! The `salesforce_pubsubapi_subscribe` task: subscribes to a Pub/Sub API
//! topic and emits each event, reconnecting whenever the stream breaks.
//!
//! Batches are delivered one at a time, in order, while the stream keeps
//! being read. The next batch is requested when the current one starts, so
//! one batch waits at most. An event the flow fails to complete is delivered
//! again after the retry backoff, until it succeeds or `max_deliver` runs out.

use crate::processor::{Error, PubSub};
use flowgen_core::{
    cache::Cache,
    config::ConfigExt,
    delivery::{Delivered, Delivery},
    event::{AvroData, Event, EventBuilder, EventData, EventExt},
    retry::RetryConfig,
    task::TaskConfig,
};
use futures_util::future::{Fuse, FutureExt};
use salesforce_core::pubsubapi::{
    eventbus::v1::{
        CommitReplayRequest, ConsumerEvent, FetchResponse, ManagedFetchRequest,
        ManagedFetchResponse, SchemaInfo,
    },
    Client, FetchRequest, PubSubError, SchemaRequest, TopicRequest,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn, Instrument};

/// Most events the server sends for one request.
const MAX_NUM_REQUESTED: i32 = 100;
/// The server closes a stream that is owed no events for a minute.
const FETCH_DEADLINE: Duration = Duration::from_secs(30);
const DEFAULT_TOPIC_PREFIX_DATA: &str = "/data/";
const DEFAULT_TOPIC_PREFIX_EVENT: &str = "/event/";
const REPLAY_PRESET_CUSTOM: i32 = 2;

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
    pub ack_timeout: Option<Duration>,
    /// How many times an event is sent through the flow before it is skipped.
    /// Delivered until the flow completes it when omitted.
    #[serde(default)]
    pub max_deliver: Option<NonZeroU32>,
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

impl Subscribe {
    /// Checks the config before the task runs.
    pub fn validate(&self) -> Result<(), Error> {
        match self.topic.num_requested {
            Some(num_requested) if !(1..=MAX_NUM_REQUESTED).contains(&num_requested) => {
                Err(Error::InvalidNumRequested { num_requested })
            }
            _ => Ok(()),
        }
    }
}

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
    /// Events requested per batch, 1 to 100 (defaults to 100, the server maximum).
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

fn stream_error(status: tonic::Status) -> Error {
    Error::PubSub {
        source: PubSubError::Tonic(Box::new(status)),
    }
}

/// Runs `open`, logging in again and retrying once when the session expired.
async fn reauthenticating<T, F, Fut>(client: &Mutex<Client>, open: F) -> Result<T, Error>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    match open().await {
        Err(Error::PubSub { source }) if crate::processor::is_pubsub_auth_error(&source) => {
            client
                .lock()
                .await
                .reconnect()
                .await
                .map_err(|source| Error::PubSub { source })?;
            open().await
        }
        result => result,
    }
}

/// Opens a subscription with `first` as its first request; the returned
/// sender asks for more events.
async fn subscribe(
    client: &Mutex<Client>,
    first: FetchRequest,
) -> Result<(Sender<FetchRequest>, tonic::Streaming<FetchResponse>), Error> {
    let (requests, rx) = mpsc::channel(1);
    requests
        .send(first)
        .await
        .map_err(|_| Error::RequestClosed)?;
    let stream = client
        .lock()
        .await
        .subscribe(rx)
        .await
        .map_err(|source| Error::PubSub { source })?
        .into_inner();
    Ok((requests, stream))
}

/// Opens a managed subscription with `first` as its first request; the
/// returned sender asks for more events and commits replay IDs.
async fn managed_subscribe(
    client: &Mutex<Client>,
    first: ManagedFetchRequest,
) -> Result<
    (
        Sender<ManagedFetchRequest>,
        tonic::Streaming<ManagedFetchResponse>,
    ),
    Error,
> {
    let (requests, rx) = mpsc::channel(1);
    requests
        .send(first)
        .await
        .map_err(|_| Error::RequestClosed)?;
    let stream = client
        .lock()
        .await
        .managed_subscribe(rx)
        .await
        .map_err(|source| Error::PubSub { source })?
        .into_inner();
    Ok((requests, stream))
}

/// A response of either kind of subscription.
struct Fetched {
    events: Vec<ConsumerEvent>,
    /// The topic position a keepalive reports; empty on other responses.
    latest_replay_id: Vec<u8>,
    pending_num_requested: i32,
}

impl From<FetchResponse> for Fetched {
    fn from(response: FetchResponse) -> Self {
        Self {
            events: response.events,
            latest_replay_id: response.latest_replay_id,
            pending_num_requested: response.pending_num_requested,
        }
    }
}

/// A managed subscription's response, logging a commit Salesforce rejected.
fn committed(response: ManagedFetchResponse) -> Fetched {
    let latest_replay_id = match response.commit_response {
        Some(commit) => {
            if let Some(e) = &commit.error {
                warn!(commit_request_id = %commit.commit_request_id, code = e.code, error = %e.msg, "Salesforce did not commit the replay ID");
            }
            Vec::new()
        }
        None => response.latest_replay_id,
    };
    Fetched {
        events: response.events,
        latest_replay_id,
        pending_num_requested: response.pending_num_requested,
    }
}

/// Where a subscription asks for events and keeps its position in the topic.
enum Position<'a> {
    /// A subscription, its replay ID cached under `key` for a durable consumer.
    Subscribe {
        requests: Sender<FetchRequest>,
        cache: &'a dyn Cache,
        key: Option<&'a str>,
    },
    /// A managed subscription, its replay ID committed to Salesforce.
    Managed {
        requests: Sender<ManagedFetchRequest>,
        commits: u64,
    },
}

impl Position<'_> {
    /// Asks the server for `num_requested` more events.
    async fn fetch(&self, num_requested: i32) -> Result<(), Error> {
        match self {
            Position::Subscribe { requests, .. } => requests
                .send(FetchRequest {
                    num_requested,
                    ..Default::default()
                })
                .await
                .map_err(|_| Error::RequestClosed),
            Position::Managed { requests, .. } => requests
                .send(ManagedFetchRequest {
                    num_requested,
                    ..Default::default()
                })
                .await
                .map_err(|_| Error::RequestClosed),
        }
    }

    /// Keeps `replay_id` as the position the next subscription resumes after.
    async fn save(&mut self, replay_id: Vec<u8>) -> Result<(), Error> {
        match self {
            Position::Subscribe { key: None, .. } => Ok(()),
            Position::Subscribe {
                cache,
                key: Some(key),
                ..
            } => {
                if let Err(e) = cache.put(key, replay_id.into(), None).await {
                    error!(key = %key, error = %e, "Failed to cache the replay ID");
                }
                Ok(())
            }
            Position::Managed { requests, commits } => {
                *commits += 1;
                requests
                    .send(ManagedFetchRequest {
                        commit_replay_id_request: Some(CommitReplayRequest {
                            commit_request_id: commits.to_string(),
                            replay_id,
                        }),
                        ..Default::default()
                    })
                    .await
                    .map_err(|_| Error::RequestClosed)
            }
        }
    }
}

/// The batches a subscription received but has not finished, and the request
/// it owes the server.
#[derive(Default)]
struct Backlog {
    /// Batches waiting for the one being delivered.
    waiting: VecDeque<Vec<ConsumerEvent>>,
    delivering: bool,
    /// When the server, owed no events, must be asked for more.
    fetch_due: Option<Instant>,
}

impl Backlog {
    /// Queues the events of `fetched`. Returns a keepalive's latest replay ID
    /// to save when every batch is finished.
    fn receive(&mut self, fetched: Fetched, now: Instant) -> Option<Vec<u8>> {
        match (fetched.events.is_empty(), self.is_idle()) {
            (true, true) if !fetched.latest_replay_id.is_empty() => Some(fetched.latest_replay_id),
            (true, _) => None,
            (false, _) => {
                if fetched.pending_num_requested == 0 {
                    self.fetch_due = Some(now + FETCH_DEADLINE);
                }
                self.waiting.push_back(fetched.events);
                None
            }
        }
    }

    fn is_idle(&self) -> bool {
        !self.delivering && self.waiting.is_empty()
    }

    /// The next batch to deliver, once the previous one finished.
    fn next_batch(&mut self) -> Option<Vec<ConsumerEvent>> {
        if self.delivering {
            return None;
        }
        let batch = self.waiting.pop_front()?;
        self.delivering = true;
        Some(batch)
    }

    /// Ends the batch being delivered.
    fn finish(&mut self) {
        self.delivering = false;
    }

    /// How many events to ask for now: a batch once none waits, or one event
    /// at the deadline, which keeps the stream open without buffering more.
    fn fetch(&mut self, num_requested: i32, now: Instant) -> Option<i32> {
        match self.fetch_due {
            Some(_) if self.waiting.is_empty() => {
                self.fetch_due = None;
                Some(num_requested)
            }
            Some(due) if due <= now => {
                self.fetch_due = None;
                Some(1)
            }
            _ => None,
        }
    }
}

/// Waits until `due`, or forever when nothing is due.
async fn until(due: Option<Instant>) {
    match due {
        Some(due) => tokio::time::sleep_until(due).await,
        None => std::future::pending().await,
    }
}

/// Sends events through the flow until it completes them.
struct Deliverer {
    tx: Option<Sender<Event>>,
    leaf_count: usize,
    ack_timeout: Option<Duration>,
    max_deliver: Option<NonZeroU32>,
    retry_config: RetryConfig,
    cancellation: CancellationToken,
}

impl Deliverer {
    async fn deliver(&self, event: Event) -> Result<Delivered, Error> {
        let delivery = Delivery {
            downstream: self.tx.is_some(),
            leaf_count: self.leaf_count,
            ack_timeout: self.ack_timeout,
            max_deliver: self.max_deliver,
            retry: &self.retry_config,
            cancellation: &self.cancellation,
        };
        delivery
            .deliver(&event, |attempt| {
                attempt.send_with_logging(self.tx.as_ref())
            })
            .await
            .map_err(|source| Error::SendMessage { source })
    }
}

/// The subject of a topic's events: its name without the `/data/` or
/// `/event/` prefix, in lowercase.
fn subject(topic_name: &str) -> String {
    let name = match topic_name.strip_prefix(DEFAULT_TOPIC_PREFIX_DATA) {
        Some(name) => name,
        None => match topic_name.strip_prefix(DEFAULT_TOPIC_PREFIX_EVENT) {
            Some(name) => name,
            None => topic_name,
        },
    };
    name.to_lowercase()
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
    /// Sends events through the flow.
    deliverer: Arc<Deliverer>,
    /// Task identifier for event tracking
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Whether the stream delivered a response, which starts the reconnect
    /// backoff over.
    streamed: AtomicBool,
}

impl EventHandler {
    /// Delivers a batch, its events concurrently. Returns the replay ID of
    /// the last event once every event is done, or `None` for an empty batch
    /// or a stop.
    async fn process_events(
        &self,
        events: Vec<ConsumerEvent>,
        schema_info: &SchemaInfo,
        topic_name: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(last_replay_id) = events.last().map(|ce| ce.replay_id.clone()) else {
            return Ok(None);
        };
        let subject = subject(topic_name);
        let built = events
            .into_iter()
            .filter_map(|ce| ce.event)
            .map(|event| {
                EventBuilder::new()
                    .data(EventData::Avro(AvroData {
                        schema: schema_info.schema_json.clone(),
                        raw_bytes: event.payload.to_vec(),
                    }))
                    .subject(subject.clone())
                    .id(event.id)
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut deliveries = JoinSet::new();
        for event in built {
            let deliverer = Arc::clone(&self.deliverer);
            deliveries.spawn(async move { deliverer.deliver(event).await }.instrument(
                tracing::info_span!(
                    parent: tracing::Span::current(),
                    "task.handle",
                    duration_ms = tracing::field::Empty,
                ),
            ));
        }

        let mut finished = true;
        while let Some(joined) = deliveries.join_next().await {
            match joined.map_err(|source| Error::DeliveryTask { source })?? {
                Delivered::Completed | Delivered::Skipped => {}
                Delivered::Stopped => finished = false,
            }
        }
        match finished {
            true => Ok(Some(last_replay_id)),
            false => Ok(None),
        }
    }

    /// Fetches topic and schema info, then reads the topic until the stream
    /// breaks or the task stops.
    async fn handle(&self) -> Result<(), Error> {
        let client = Arc::clone(&self.pubsub.client);
        let topic_info = client
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.name.clone(),
            })
            .await
            .map_err(|source| Error::PubSub { source })?
            .into_inner();

        let schema_info = client
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
            None => MAX_NUM_REQUESTED,
        };
        let topic_name = topic_info.topic_name.as_str();
        let durable = self
            .config
            .topic
            .durable_consumer_options
            .as_ref()
            .filter(|opts| opts.enabled);

        match durable {
            Some(opts) if opts.managed_subscription => {
                self.read_managed(&client, &opts.name, num_requested, &schema_info, topic_name)
                    .await
            }
            durable => {
                self.read(&client, durable, num_requested, &schema_info, topic_name)
                    .await
            }
        }
    }

    /// Reads the topic, storing the replay ID of each finished batch for a
    /// durable consumer.
    async fn read(
        &self,
        client: &Mutex<Client>,
        durable: Option<&DurableConsumerOptions>,
        num_requested: i32,
        schema_info: &SchemaInfo,
        topic_name: &str,
    ) -> Result<(), Error> {
        let cache = &self.task_context.cache;
        let flow_name = self.task_context.flow.identity();
        let cache_key = format!(
            "flow.{}.replay_id.{}",
            self.task_context.flow.id(),
            sanitize_topic_name(topic_name)
        );

        let mut first = FetchRequest {
            topic_name: topic_name.to_string(),
            num_requested,
            ..Default::default()
        };
        if let Some(durable) = durable {
            let stored = cache
                .get(&cache_key)
                .await
                .map_err(|source| Error::ReplayIdCache {
                    key: cache_key.clone(),
                    source,
                })?;
            match stored {
                Some(replay_id) => {
                    first.replay_id = replay_id.into();
                    first.replay_preset = REPLAY_PRESET_CUSTOM;
                }
                None => {
                    first.replay_preset = durable.replay_preset.to_i32();
                    warn!(
                        "No cached replay_id found for flow {flow_name} topic {topic_name}, starting from {:?}",
                        &durable.replay_preset
                    );
                }
            }
        }

        let result = match reauthenticating(client, || subscribe(client, first.clone())).await {
            Ok((requests, stream)) => {
                let position = Position::Subscribe {
                    requests,
                    cache: cache.as_ref(),
                    key: durable.map(|_| cache_key.as_str()),
                };
                let stream = stream.map(|response| response.map(Fetched::from));
                self.consume(stream, position, num_requested, schema_info, topic_name)
                    .await
            }
            Err(e) => Err(e),
        };
        match result {
            Err(Error::PubSub { source })
                if !first.replay_id.is_empty()
                    && !self.streamed.load(Ordering::Relaxed)
                    && is_replay_id_error(&source) =>
            {
                warn!("Invalid replay_id detected, clearing from cache and retrying");
                if let Err(e) = cache.delete(&cache_key).await {
                    error!(
                        "Failed to delete invalid replay_id from cache for flow {flow_name} topic {topic_name}: {e}"
                    );
                }
                Err(Error::PubSub { source })
            }
            result => result,
        }
    }

    /// Reads a managed subscription, committing the replay ID of each
    /// finished batch to Salesforce.
    async fn read_managed(
        &self,
        client: &Mutex<Client>,
        developer_name: &str,
        num_requested: i32,
        schema_info: &SchemaInfo,
        topic_name: &str,
    ) -> Result<(), Error> {
        let first = ManagedFetchRequest {
            developer_name: developer_name.to_string(),
            num_requested,
            ..Default::default()
        };
        let (requests, stream) =
            reauthenticating(client, || managed_subscribe(client, first.clone())).await?;
        let position = Position::Managed {
            requests,
            commits: 0,
        };
        let stream = stream.map(|response| response.map(committed));
        self.consume(stream, position, num_requested, schema_info, topic_name)
            .await
    }

    /// Delivers the batches of `stream` one at a time while reading on, so
    /// the server is asked for more before it closes a stream owed nothing.
    async fn consume<S>(
        &self,
        mut stream: S,
        mut position: Position<'_>,
        num_requested: i32,
        schema_info: &SchemaInfo,
        topic_name: &str,
    ) -> Result<(), Error>
    where
        S: Stream<Item = Result<Fetched, tonic::Status>> + Unpin,
    {
        let cancellation = &self.task_context.cancellation_token;
        let mut backlog = Backlog::default();
        let mut delivering = std::pin::pin!(Fuse::terminated());
        loop {
            if let Some(events) = backlog.next_batch() {
                delivering.set(self.process_events(events, schema_info, topic_name).fuse());
            }
            if let Some(num_requested) = backlog.fetch(num_requested, Instant::now()) {
                position.fetch(num_requested).await?;
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                finished = &mut delivering => {
                    backlog.finish();
                    if let Some(replay_id) = finished? {
                        position.save(replay_id).await?;
                    }
                }
                response = stream.next() => {
                    let fetched = match response {
                        Some(response) => response.map_err(stream_error)?,
                        None => return Err(Error::StreamEnded),
                    };
                    self.streamed.store(true, Ordering::Relaxed);
                    if let Some(replay_id) = backlog.receive(fetched, Instant::now()) {
                        position.save(replay_id).await?;
                    }
                }
                _ = until(backlog.fetch_due) => {}
            }
        }
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
            deliverer: Arc::new(Deliverer {
                tx: self.tx.clone(),
                leaf_count: self.task_context.leaf_count,
                ack_timeout: config.ack_timeout,
                max_deliver: config.max_deliver,
                retry_config: RetryConfig::merge(&self.task_context.retry, &config.retry),
                cancellation: self.task_context.cancellation_token.clone(),
            }),
            config: Arc::new(config),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
            streamed: AtomicBool::new(false),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        self.config.validate()?;
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
            tokio::select! {
                biased;
                _ = cancellation_token.cancelled() => return Ok(()),
                result = event_handler.handle() => match result {
                    _ if cancellation_token.is_cancelled() => return Ok(()),
                    Ok(()) => warn!("Subscriber lost connectivity, reinitializing"),
                    Err(e) => error!(error = %e, "Subscriber lost connectivity, reinitializing"),
                },
            }
            lost_connectivity = true;
            if event_handler.streamed.load(Ordering::Relaxed) {
                reconnect_backoff = retry_config.reconnect_strategy();
            }

            let registry = &self.task_context.client_registry;
            registry.remove(&event_handler.pubsub.client_key).await;
            registry.remove(&event_handler.pubsub.channel_key).await;

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
            max_deliver: NonZeroU32::new(3),
            depends_on: None,
            retry: None,
        };

        let json = serde_json::to_string(&subscriber).unwrap();
        let deserialized: Subscribe = serde_json::from_str(&json).unwrap();
        assert_eq!(subscriber, deserialized);
    }

    #[test]
    fn subjects_drop_the_topic_prefix() {
        assert_eq!(subject("/data/AccountChangeEvent"), "accountchangeevent");
        assert_eq!(subject("/event/Order_Update__e"), "order_update__e");
        assert_eq!(subject("Custom"), "custom");
    }

    #[test]
    fn a_zero_max_deliver_is_rejected() {
        let json = r#"{
            "name": "n",
            "credentials_path": "c",
            "topic": { "name": "/event/Test__e" },
            "max_deliver": 0
        }"#;
        assert!(serde_json::from_str::<Subscribe>(json).is_err());
    }

    fn with_num_requested(num_requested: Option<i32>) -> Subscribe {
        Subscribe {
            topic: Topic {
                name: "/event/Test__e".to_string(),
                num_requested,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn num_requested_must_be_within_the_server_maximum() {
        for accepted in [None, Some(1), Some(100)] {
            assert!(with_num_requested(accepted).validate().is_ok());
        }
        for rejected in [0, -1, 101, 200] {
            assert!(matches!(
                with_num_requested(Some(rejected)).validate(),
                Err(Error::InvalidNumRequested { num_requested }) if num_requested == rejected
            ));
        }
    }

    fn batch(replay_ids: &[u8], pending_num_requested: i32) -> Fetched {
        Fetched {
            events: replay_ids
                .iter()
                .map(|replay_id| ConsumerEvent {
                    replay_id: vec![*replay_id],
                    ..Default::default()
                })
                .collect(),
            latest_replay_id: Vec::new(),
            pending_num_requested,
        }
    }

    fn keepalive(latest_replay_id: &[u8]) -> Fetched {
        Fetched {
            events: Vec::new(),
            latest_replay_id: latest_replay_id.to_vec(),
            pending_num_requested: 100,
        }
    }

    fn replay_ids(batch: Option<Vec<ConsumerEvent>>) -> Option<Vec<Vec<u8>>> {
        batch.map(|events| events.into_iter().map(|event| event.replay_id).collect())
    }

    #[test]
    fn the_next_batch_is_requested_before_the_last_one_is_delivered() {
        let now = Instant::now();
        let mut backlog = Backlog::default();

        assert_eq!(backlog.receive(batch(&[1, 2], 0), now), None);
        assert_eq!(
            replay_ids(backlog.next_batch()),
            Some(vec![vec![1], vec![2]])
        );
        assert_eq!(backlog.fetch(100, now), Some(100));
        assert_eq!(backlog.fetch(100, now), None);
    }

    #[test]
    fn a_batch_still_owed_events_requests_nothing() {
        let now = Instant::now();
        let mut backlog = Backlog::default();

        backlog.receive(batch(&[1], 99), now);
        assert!(backlog.next_batch().is_some());
        assert_eq!(backlog.fetch(100, now + FETCH_DEADLINE * 10), None);
    }

    #[test]
    fn the_request_owed_for_a_waiting_batch_is_sent_when_it_starts() {
        let now = Instant::now();
        let mut backlog = Backlog::default();
        backlog.receive(batch(&[1], 0), now);
        backlog.next_batch();
        backlog.fetch(100, now);

        backlog.receive(batch(&[2], 0), now);
        assert!(backlog.next_batch().is_none());
        assert_eq!(backlog.fetch(100, now), None);

        backlog.finish();
        assert_eq!(replay_ids(backlog.next_batch()), Some(vec![vec![2]]));
        assert_eq!(backlog.fetch(100, now), Some(100));
    }

    #[test]
    fn a_long_delivery_keeps_the_stream_open_one_event_at_a_time() {
        let now = Instant::now();
        let mut backlog = Backlog::default();
        backlog.receive(batch(&[1], 0), now);
        backlog.next_batch();
        backlog.fetch(100, now);
        backlog.receive(batch(&[2], 0), now);

        assert_eq!(backlog.fetch(100, now + FETCH_DEADLINE), Some(1));
        assert_eq!(backlog.fetch(100, now + FETCH_DEADLINE), None);

        let later = now + FETCH_DEADLINE;
        backlog.receive(batch(&[3], 0), later);
        assert_eq!(backlog.fetch(100, later), None);
        assert_eq!(backlog.fetch(100, later + FETCH_DEADLINE), Some(1));
    }

    #[test]
    fn batches_are_delivered_one_at_a_time_in_order() {
        let now = Instant::now();
        let mut backlog = Backlog::default();
        backlog.receive(batch(&[1], 50), now);
        backlog.receive(batch(&[2], 0), now);

        assert_eq!(replay_ids(backlog.next_batch()), Some(vec![vec![1]]));
        assert!(backlog.next_batch().is_none());
        backlog.finish();
        assert_eq!(replay_ids(backlog.next_batch()), Some(vec![vec![2]]));
        backlog.finish();
        assert!(backlog.next_batch().is_none());
    }

    #[test]
    fn a_keepalive_position_is_saved_only_when_every_batch_is_finished() {
        let now = Instant::now();
        let mut backlog = Backlog::default();

        assert_eq!(backlog.receive(keepalive(&[7]), now), Some(vec![7]));
        assert_eq!(backlog.receive(keepalive(&[]), now), None);

        backlog.receive(batch(&[1], 99), now);
        assert_eq!(backlog.receive(keepalive(&[7]), now), None);
        backlog.next_batch();
        assert_eq!(backlog.receive(keepalive(&[7]), now), None);
        backlog.finish();
        assert_eq!(backlog.receive(keepalive(&[8]), now), Some(vec![8]));
    }

    #[test]
    fn a_commit_response_is_not_a_keepalive() {
        let response = ManagedFetchResponse {
            latest_replay_id: vec![7],
            pending_num_requested: 100,
            commit_response: Some(Default::default()),
            ..Default::default()
        };
        assert!(committed(response).latest_replay_id.is_empty());

        let response = ManagedFetchResponse {
            latest_replay_id: vec![7],
            pending_num_requested: 100,
            ..Default::default()
        };
        assert_eq!(committed(response).latest_replay_id, vec![7]);
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
