//! # Kafka Subscribe
//!
//! Consumes every partition of a Kafka topic and emits each record as an
//! event. The next offset of each partition is kept in the flowgen cache and
//! advances once the flow has completed the record. A record the flow fails
//! to complete is delivered again, until it succeeds or `max_deliver` runs out.

use crate::config::{default_brokers, ConfigError};
use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::delivery::{Delivered, Delivery};
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::retry::RetryConfig;
use flowgen_core::task::TaskConfig;
use futures_util::{future, StreamExt};
use rskafka::client::consumer::{StartOffset as ConsumerStart, StreamConsumerBuilder};
use rskafka::client::error::{Error as KafkaError, ProtocolError};
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::record::RecordAndOffset;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Where the partitions start when none of them has a stored offset.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub enum StartOffset {
    /// The oldest record the topic still retains.
    Earliest,
    /// The next record written after the subscriber starts.
    #[default]
    Latest,
    /// The first record written at or after this time.
    Timestamp(chrono::DateTime<chrono::Utc>),
}

const START_OFFSET_EARLIEST: &str = "earliest";
const START_OFFSET_LATEST: &str = "latest";

impl TryFrom<String> for StartOffset {
    type Error = ConfigError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            START_OFFSET_EARLIEST => Ok(StartOffset::Earliest),
            START_OFFSET_LATEST => Ok(StartOffset::Latest),
            other => match chrono::DateTime::parse_from_rfc3339(other) {
                Ok(timestamp) if timestamp.timestamp_millis() >= 0 => {
                    Ok(StartOffset::Timestamp(timestamp.to_utc()))
                }
                _ => Err(ConfigError::InvalidStartOffset { value }),
            },
        }
    }
}

impl From<StartOffset> for String {
    fn from(start_offset: StartOffset) -> Self {
        match start_offset {
            StartOffset::Earliest => START_OFFSET_EARLIEST.to_string(),
            StartOffset::Latest => START_OFFSET_LATEST.to_string(),
            StartOffset::Timestamp(timestamp) => timestamp.to_rfc3339(),
        }
    }
}

/// Kafka subscribe task configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Subscribe {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to credentials file containing SASL/SSL authentication details.
    /// Omit to connect without authentication.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// Comma-separated bootstrap broker addresses.
    #[serde(default = "default_brokers")]
    pub brokers: String,
    /// Topic to consume.
    pub topic: String,
    /// Where the partitions start when none of them has a stored offset, or
    /// when a stored offset is past the end of its partition.
    #[serde(default)]
    pub start_offset: StartOffset,
    /// How long to wait for the flow to complete a record. Waits
    /// indefinitely when omitted.
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<Duration>,
    /// How many times a record is sent through the flow before it is skipped.
    /// Delivered until the flow completes it when omitted.
    #[serde(default)]
    pub max_deliver: Option<std::num::NonZeroU32>,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration for connecting and redelivery (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl Default for Subscribe {
    fn default() -> Self {
        Self {
            name: String::new(),
            credentials_path: None,
            brokers: default_brokers(),
            topic: String::new(),
            start_offset: StartOffset::default(),
            ack_timeout: None,
            max_deliver: None,
            depends_on: None,
            retry: None,
        }
    }
}

impl ConfigExt for Subscribe {}

impl Subscribe {
    /// Validates the config after any templating has been applied.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.topic.trim().is_empty() {
            return Err(ConfigError::EmptyTopic);
        }
        if self.brokers.trim().is_empty() {
            return Err(ConfigError::EmptyBrokers);
        }
        if matches!(self.ack_timeout, Some(ack_timeout) if ack_timeout.is_zero()) {
            return Err(ConfigError::ZeroAckTimeout);
        }
        Ok(())
    }
}

/// Bounds each request to the brokers and the retries around it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the topic's partitions are checked for additions.
const METADATA_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Record metadata the event has no field for, merged into `event.meta`.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RecordMeta {
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub headers: BTreeMap<String, String>,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Kafka client error: {source}")]
    ClientAuth {
        #[source]
        source: crate::client::Error,
    },
    #[error("Client is missing or not initialized")]
    MissingClient,
    #[error(
        "Client registry type mismatch -- same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("Metadata fetch error: {source}")]
    MetadataFetch {
        #[source]
        source: Box<KafkaError>,
    },
    #[error("Topic '{topic}' does not exist on the Kafka cluster")]
    TopicNotFound { topic: String },
    #[error("Topic '{topic}' has no partitions")]
    NoPartitions { topic: String },
    #[error("Error connecting to partition {partition} of '{topic}': {source}")]
    PartitionClient {
        topic: String,
        partition: i32,
        #[source]
        source: Box<KafkaError>,
    },
    #[error("Error consuming partition {partition} of '{topic}': {source}")]
    Consume {
        topic: String,
        partition: i32,
        #[source]
        source: Box<KafkaError>,
    },
    #[error("Error looking up the start offset of partition {partition} of '{topic}': {source}")]
    OffsetLookup {
        topic: String,
        partition: i32,
        #[source]
        source: Box<KafkaError>,
    },
    #[error("Consumer for partition {partition} of '{topic}' ended unexpectedly")]
    StreamEnded { topic: String, partition: i32 },
    #[error("Topic '{topic}' gained partitions {partitions:?}")]
    PartitionsAdded { topic: String, partitions: Vec<i32> },
    #[error("Cache error for offset key '{key}': {source}")]
    Cache {
        key: String,
        #[source]
        source: flowgen_core::cache::CacheError,
    },
    #[error("Cached offset under '{key}' is not a number")]
    InvalidCachedOffset { key: String },
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
}

impl Error {
    /// Whether initializing again can only produce the same failure.
    fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::ConfigRender { .. }
                | Error::Config(_)
                | Error::ClientRegistryMismatch
                | Error::InvalidCachedOffset { .. }
        )
    }
}

/// The offset a partition without a usable stored offset starts at. A
/// timestamp with no record at or after it starts at the latest offset.
async fn resolve_start_offset(
    client: &PartitionClient,
    start_offset: StartOffset,
) -> Result<i64, Error> {
    match start_offset {
        StartOffset::Earliest => partition_offset(client, OffsetAt::Earliest).await,
        StartOffset::Latest => partition_offset(client, OffsetAt::Latest).await,
        StartOffset::Timestamp(timestamp) => {
            match partition_offset(client, OffsetAt::Timestamp(timestamp)).await? {
                offset if offset < 0 => partition_offset(client, OffsetAt::Latest).await,
                offset => Ok(offset),
            }
        }
    }
}

/// The offset a partition without a stored offset starts at: `start_offset`
/// when no partition of the topic has one, and the earliest offset otherwise,
/// since the partition was added after the others started.
async fn initial_offset(
    client: &PartitionClient,
    start_offset: StartOffset,
    first_start: bool,
) -> Result<i64, Error> {
    match first_start {
        true => resolve_start_offset(client, start_offset).await,
        false => {
            let earliest = partition_offset(client, OffsetAt::Earliest).await?;
            info!(
                partition = client.partition(),
                earliest, "Partition has no stored offset while others of the topic do, starting it at its earliest offset"
            );
            Ok(earliest)
        }
    }
}

/// Cache key holding the next offset to consume from one partition.
fn offset_key(flow_key: &str, topic: &str, partition: i32) -> String {
    format!("flow.{flow_key}.kafka_offset.{topic}.{partition}")
}

fn parse_offset(key: &str, value: &[u8]) -> Result<i64, Error> {
    let invalid = || Error::InvalidCachedOffset {
        key: key.to_string(),
    };
    match std::str::from_utf8(value) {
        Ok(value) => value.parse().map_err(|_| invalid()),
        Err(_) => Err(invalid()),
    }
}

/// Record value as event data: JSON when it parses, raw bytes otherwise,
/// and JSON null for a tombstone.
fn record_data(value: Option<&[u8]>) -> EventData {
    match value {
        None => EventData::Json(serde_json::Value::Null),
        Some(value) => match serde_json::from_slice(value) {
            Ok(json) => EventData::Json(json),
            Err(_) => EventData::Bytes(bytes::Bytes::copy_from_slice(value)),
        },
    }
}

fn record_meta(partition: i32, record: &RecordAndOffset) -> RecordMeta {
    RecordMeta {
        partition,
        offset: record.offset,
        key: record
            .record
            .key
            .as_deref()
            .map(|key| String::from_utf8_lossy(key).into_owned()),
        headers: record
            .record
            .headers
            .iter()
            .map(|(name, value)| (name.clone(), String::from_utf8_lossy(value).into_owned()))
            .collect(),
    }
}

async fn stored_offset(
    cache: &dyn flowgen_core::cache::Cache,
    key: &str,
) -> Result<Option<i64>, Error> {
    let value = cache.get(key).await.map_err(|source| Error::Cache {
        key: key.to_string(),
        source,
    })?;
    match value {
        Some(value) => parse_offset(key, &value).map(Some),
        None => Ok(None),
    }
}

async fn store_offset(
    cache: &dyn flowgen_core::cache::Cache,
    key: &str,
    offset: i64,
) -> Result<(), Error> {
    cache
        .put(key, offset.to_string().into(), None)
        .await
        .map_err(|source| Error::Cache {
            key: key.to_string(),
            source,
        })
}

async fn partition_offset(client: &PartitionClient, at: OffsetAt) -> Result<i64, Error> {
    client
        .get_offset(at)
        .await
        .map_err(|source| Error::OffsetLookup {
            topic: client.topic().to_string(),
            partition: client.partition(),
            source: Box::new(source),
        })
}

/// A partition with the cache key of its offset and the offset to read next.
struct Partition {
    client: Arc<PartitionClient>,
    key: String,
    next_offset: i64,
}

pub struct EventHandler {
    client_key: flowgen_core::client_registry::ClientKey,
    client: Arc<rskafka::client::Client>,
    partitions: Vec<Partition>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    config: Arc<Subscribe>,
    retry_config: RetryConfig,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

fn build_event(
    topic: &str,
    partition: i32,
    record: &RecordAndOffset,
    task_id: usize,
    task_type: &'static str,
) -> Result<Event, Error> {
    let meta = match serde_json::to_value(record_meta(partition, record)) {
        Ok(serde_json::Value::Object(meta)) => meta,
        Ok(_) => serde_json::Map::new(),
        Err(source) => return Err(Error::SerdeJson { source }),
    };
    EventBuilder::new()
        .subject(topic.to_string())
        .id(format!("{topic}-{partition}-{}", record.offset))
        .timestamp(record.record.timestamp.timestamp_micros())
        .data(record_data(record.record.value.as_deref()))
        .meta_merge(meta)
        .task_id(task_id)
        .task_type(task_type)
        .build()
        .map_err(|source| Error::EventBuilder { source })
}

impl EventHandler {
    /// Sends the record through the flow until it completes, or until
    /// `max_deliver` deliveries have failed, then stores the next offset; a
    /// stop first leaves the offset as it is.
    #[tracing::instrument(
        skip(self, record, key),
        name = "task.handle",
        fields(duration_ms = tracing::field::Empty)
    )]
    async fn process_record(
        &self,
        partition: i32,
        record: &RecordAndOffset,
        key: &str,
    ) -> Result<Delivered, Error> {
        let event = build_event(
            &self.config.topic,
            partition,
            record,
            self.task_id,
            self.task_type,
        )?;
        let delivery = Delivery {
            downstream: self.tx.is_some(),
            leaf_count: self.task_context.leaf_count,
            ack_timeout: self.config.ack_timeout,
            max_deliver: self.config.max_deliver,
            retry: &self.retry_config,
            cancellation: &self.task_context.cancellation_token,
        };
        let delivered = delivery
            .deliver(&event, |attempt| {
                attempt
                    .send_with_logging(self.tx.as_ref())
                    .context("partition", partition)
                    .context("offset", record.offset)
            })
            .await
            .map_err(|source| Error::SendMessage { source })?;
        if delivered == Delivered::Stopped {
            return Ok(delivered);
        }

        if let Err(e) = store_offset(self.task_context.cache.as_ref(), key, record.offset + 1).await
        {
            error!(partition, offset = record.offset, error = %e, "Failed to store the next offset");
        }
        Ok(delivered)
    }

    /// Where to continue a partition whose next offset the topic no longer
    /// holds: its earliest record when retention removed the offset, or
    /// `start_offset` when the offset is past the end of the partition.
    async fn recover_offset(
        &self,
        client: &PartitionClient,
        next_offset: i64,
    ) -> Result<i64, Error> {
        let earliest = partition_offset(client, OffsetAt::Earliest).await?;
        if next_offset < earliest {
            warn!(partition = client.partition(), next_offset, earliest, "Records before the earliest retained offset were removed before they were read, continuing from the earliest offset");
            Ok(earliest)
        } else {
            warn!(partition = client.partition(), next_offset, start_offset = ?self.config.start_offset, "Offset is past the end of the partition, continuing from start_offset");
            resolve_start_offset(client, self.config.start_offset).await
        }
    }

    /// Consumes one partition in order, resuming after a failed fetch, until
    /// `cancellation` stops it after the record in flight.
    async fn consume_partition(
        &self,
        partition: &Partition,
        cancellation: &CancellationToken,
    ) -> Result<(), Error> {
        let client = &partition.client;
        let mut next_offset = partition.next_offset;
        let mut backoff = self.retry_config.reconnect_strategy();
        loop {
            let mut stream =
                StreamConsumerBuilder::new(Arc::clone(client), ConsumerStart::At(next_offset))
                    .build();
            let failure = loop {
                let next = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Ok(()),
                    next = stream.next() => next,
                };
                match next {
                    Some(Ok((record, _high_watermark))) => {
                        let delivered = self
                            .process_record(client.partition(), &record, &partition.key)
                            .await?;
                        if delivered == Delivered::Stopped {
                            return Ok(());
                        }
                        next_offset = record.offset + 1;
                        backoff = self.retry_config.reconnect_strategy();
                    }
                    Some(Err(source)) => break Some(source),
                    None => break None,
                }
            };

            let recovered = match failure {
                Some(KafkaError::ServerError {
                    protocol_error: ProtocolError::OffsetOutOfRange,
                    ..
                }) => tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Ok(()),
                    recovered = self.recover_offset(client, next_offset) => recovered,
                },
                Some(source) => Err(Error::Consume {
                    topic: self.config.topic.clone(),
                    partition: client.partition(),
                    source: Box::new(source),
                }),
                None => Err(Error::StreamEnded {
                    topic: self.config.topic.clone(),
                    partition: client.partition(),
                }),
            };
            match recovered {
                Ok(offset) => next_offset = offset,
                Err(e) => {
                    let delay = match backoff.next() {
                        Some(delay) => delay,
                        None => self.retry_config.initial_backoff,
                    };
                    warn!(partition = client.partition(), error = %e, delay_ms = %delay.as_millis(), "Consuming partition failed, resuming");
                    tokio::select! {
                        _ = cancellation.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }

    /// Returns once the topic has partitions this handler does not consume,
    /// which the next initialization starts at their earliest offsets.
    async fn watch_partitions(&self) -> Result<(), Error> {
        let topic = &self.config.topic;
        loop {
            tokio::time::sleep(METADATA_REFRESH_INTERVAL).await;
            let topics = match self.client.list_topics().await {
                Ok(topics) => topics,
                Err(e) => {
                    warn!(error = %e, "Failed to refresh topic metadata");
                    continue;
                }
            };
            let Some(metadata) = topics.into_iter().find(|t| &t.name == topic) else {
                continue;
            };
            let added: Vec<i32> = metadata
                .partitions
                .into_iter()
                .filter(|p| {
                    !self
                        .partitions
                        .iter()
                        .any(|known| known.client.partition() == *p)
                })
                .collect();
            if added.is_empty() {
                continue;
            }
            return Err(Error::PartitionsAdded {
                topic: topic.clone(),
                partitions: added,
            });
        }
    }

    /// Consumes all partitions concurrently until the task stops, the flow
    /// stops taking events or the topic gains partitions.
    async fn handle(&self) -> Result<(), Error> {
        let partitions = self.task_context.cancellation_token.child_token();
        let consume = future::try_join_all(
            self.partitions
                .iter()
                .map(|partition| self.consume_partition(partition, &partitions)),
        );
        tokio::pin!(consume);
        let added = tokio::select! {
            result = &mut consume => return result.map(|_| ()),
            added = self.watch_partitions() => added,
        };
        partitions.cancel();
        consume.await?;
        added
    }
}

#[derive(Debug)]
pub struct Subscriber {
    config: Arc<Subscribe>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Subscriber {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;
        init_config.validate()?;

        let client_key = flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
            .field("credentials_path", &init_config.credentials_path)
            .field("brokers", &init_config.brokers)
            .build();
        let client = self
            .task_context
            .client_registry
            .get_or_init(client_key.clone(), || {
                let credentials_path = init_config.credentials_path.clone();
                let brokers = init_config.brokers.clone();
                async move {
                    crate::client::Client::new(credentials_path, Some(brokers), REQUEST_TIMEOUT)
                        .connect()
                        .await
                        .map_err(|source| Error::ClientAuth { source })?
                        .client
                        .ok_or(Error::MissingClient)
                }
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        let topic = init_config.topic.clone();
        let topics = client
            .list_topics()
            .await
            .map_err(|source| Error::MetadataFetch {
                source: Box::new(source),
            })?;
        let Some(metadata) = topics.into_iter().find(|t| t.name == topic) else {
            return Err(Error::TopicNotFound { topic });
        };
        if metadata.partitions.is_empty() {
            return Err(Error::NoPartitions { topic });
        }

        let cache = self.task_context.cache.as_ref();
        let flow_key = self.task_context.flow.id();
        let mut stored = Vec::with_capacity(metadata.partitions.len());
        for partition in metadata.partitions {
            let key = offset_key(&flow_key, &topic, partition);
            let offset = stored_offset(cache, &key).await?;
            stored.push((partition, key, offset));
        }
        let first_start = stored.iter().all(|(_, _, offset)| offset.is_none());

        let mut partitions = Vec::with_capacity(stored.len());
        let mut started = Vec::new();
        for (partition, key, offset) in stored {
            let partition_client = client
                .partition_client(topic.as_str(), partition, UnknownTopicHandling::Retry)
                .await
                .map_err(|source| Error::PartitionClient {
                    topic: topic.clone(),
                    partition,
                    source: Box::new(source),
                })?;
            let next_offset = match offset {
                Some(offset) => offset,
                None => {
                    let offset =
                        initial_offset(&partition_client, init_config.start_offset, first_start)
                            .await?;
                    started.push((key.clone(), offset));
                    offset
                }
            };
            partitions.push(Partition {
                client: Arc::new(partition_client),
                key,
                next_offset,
            });
        }
        for (key, offset) in started {
            store_offset(cache, &key, offset).await?;
        }

        Ok(EventHandler {
            client_key,
            client: Arc::clone(&client),
            partitions,
            tx: self.tx.clone(),
            task_id: self.task_id,
            config: Arc::new(init_config),
            retry_config: RetryConfig::merge(&self.task_context.retry, &self.config.retry),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config = RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let cancellation_token = self.task_context.cancellation_token.clone();
        let mut reconnect_backoff = retry_config.reconnect_strategy();
        let mut lost_connectivity = false;

        loop {
            if cancellation_token.is_cancelled() {
                return Ok(());
            }

            let init_future = tokio_retry::Retry::spawn(
                retry_config.init_strategy(self.task_context.startup_delay),
                || async {
                    match self.init().await {
                        Ok(handler) => Ok(handler),
                        Err(e) if e.is_permanent() => {
                            error!(error = %e, "Permanent initialization error");
                            Err(tokio_retry::RetryError::permanent(e))
                        }
                        Err(e) => {
                            error!(error = %e, "Subscriber initialization failed");
                            Err(tokio_retry::RetryError::transient(e))
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

            match event_handler.handle().await {
                _ if cancellation_token.is_cancelled() => return Ok(()),
                Ok(()) => warn!("Subscriber lost connectivity, reinitializing"),
                Err(e) => error!(error = %e, "Subscriber lost connectivity, reinitializing"),
            }
            lost_connectivity = true;

            self.task_context
                .client_registry
                .remove(&event_handler.client_key)
                .await;

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

#[derive(Default)]
pub struct SubscriberBuilder {
    config: Option<Arc<Subscribe>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<Subscribe>) -> Self {
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

    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
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
    use rskafka::record::Record;

    fn record(key: Option<&[u8]>, value: Option<&[u8]>, offset: i64) -> RecordAndOffset {
        RecordAndOffset {
            record: Record {
                key: key.map(<[u8]>::to_vec),
                value: value.map(<[u8]>::to_vec),
                headers: [("source".to_string(), b"test".to_vec())]
                    .into_iter()
                    .collect(),
                timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
            },
            offset,
        }
    }

    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
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
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_offset_key_is_per_flow_topic_and_partition() {
        assert_eq!(
            offset_key("Zmxvdw", "orders", 3),
            "flow.Zmxvdw.kafka_offset.orders.3"
        );
        assert_ne!(offset_key("a", "t", 0), offset_key("a", "t", 1));
        assert_ne!(offset_key("a", "t", 0), offset_key("b", "t", 0));
    }

    #[test]
    fn test_parse_offset() {
        assert_eq!(parse_offset("k", b"42").unwrap(), 42);
        assert!(matches!(
            parse_offset("k", b"forty-two"),
            Err(Error::InvalidCachedOffset { key }) if key == "k"
        ));
        assert!(parse_offset("k", &[0xff]).is_err());
    }

    #[test]
    fn test_record_data_parses_json() {
        assert!(matches!(
            record_data(Some(br#"{"id": 1}"#)),
            EventData::Json(json) if json == serde_json::json!({"id": 1})
        ));
    }

    #[test]
    fn test_record_data_keeps_non_json_as_bytes() {
        assert!(matches!(
            record_data(Some(b"plain text")),
            EventData::Bytes(bytes) if bytes.as_ref() == b"plain text"
        ));
    }

    #[test]
    fn test_record_data_maps_a_tombstone_to_null() {
        assert!(matches!(
            record_data(None),
            EventData::Json(serde_json::Value::Null)
        ));
    }

    #[test]
    fn test_record_meta() {
        let meta = record_meta(2, &record(Some(b"key-1"), Some(b"{}"), 7));

        assert_eq!(
            meta,
            RecordMeta {
                partition: 2,
                offset: 7,
                key: Some("key-1".to_string()),
                headers: [("source".to_string(), "test".to_string())]
                    .into_iter()
                    .collect(),
            }
        );
    }

    #[test]
    fn test_record_meta_without_key() {
        assert_eq!(record_meta(0, &record(None, None, 0)).key, None);
    }

    #[test]
    fn test_added_partitions_reinitialize_the_subscriber() {
        let added = Error::PartitionsAdded {
            topic: "t".to_string(),
            partitions: vec![1],
        };
        assert!(!added.is_permanent());
    }

    #[test]
    fn test_build_event_maps_the_record_onto_the_event() {
        let event = build_event(
            "orders",
            2,
            &record(Some(b"key-1"), Some(br#"{"id": 1}"#), 7),
            1,
            "kafka_subscribe",
        )
        .unwrap();
        let meta = event.meta.expect("meta");

        assert_eq!(event.subject, "orders");
        assert_eq!(event.id.as_deref(), Some("orders-2-7"));
        assert_eq!(event.timestamp, 1_700_000_000_000_000);
        assert!(
            matches!(event.data, EventData::Json(json) if json == serde_json::json!({"id": 1}))
        );
        assert_eq!(meta["partition"], 2);
        assert_eq!(meta["offset"], 7);
        assert_eq!(meta["key"], "key-1");
        assert_eq!(meta["headers"]["source"], "test");
        assert!(meta.contains_key("correlation_id"));
    }

    #[tokio::test]
    async fn test_subscriber_builder_missing_each_field() {
        let config = Arc::new(Subscribe {
            name: "n".into(),
            topic: "t".into(),
            ..Default::default()
        });

        let e = SubscriberBuilder::new()
            .task_context(create_mock_task_context())
            .task_type("kafka_subscribe")
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::MissingBuilderAttribute(attr) if attr == "config"));

        let e = SubscriberBuilder::new()
            .config(Arc::clone(&config))
            .task_type("kafka_subscribe")
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::MissingBuilderAttribute(attr) if attr == "task_context"));

        let e = SubscriberBuilder::new()
            .config(config)
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::MissingBuilderAttribute(attr) if attr == "task_type"));
    }

    #[test]
    fn test_subscribe_defaults() {
        let json = r#"{ "name": "n", "topic": "t" }"#;
        let config: Subscribe = serde_json::from_str(json).unwrap();

        assert_eq!(
            config,
            Subscribe {
                name: "n".into(),
                topic: "t".into(),
                ..Default::default()
            }
        );
        assert_eq!(config.start_offset, StartOffset::Latest);
        assert_eq!(config.ack_timeout, None);
    }

    #[test]
    fn test_subscribe_parses_start_offset_and_ack_timeout() {
        let json =
            r#"{ "name": "n", "topic": "t", "start_offset": "earliest", "ack_timeout": "2m" }"#;
        let config: Subscribe = serde_json::from_str(json).unwrap();

        assert_eq!(config.start_offset, StartOffset::Earliest);
        assert_eq!(config.ack_timeout, Some(Duration::from_secs(120)));
    }

    #[test]
    fn test_subscribe_parses_a_timestamp_start_offset() {
        let json = r#"{ "name": "n", "topic": "t", "start_offset": "2026-09-01T02:00:00+02:00" }"#;
        let config: Subscribe = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.start_offset,
            StartOffset::Timestamp("2026-09-01T00:00:00Z".parse().unwrap())
        );
    }

    #[test]
    fn test_start_offset_round_trips() {
        for start_offset in [
            StartOffset::Earliest,
            StartOffset::Latest,
            StartOffset::Timestamp("2026-09-01T00:00:00Z".parse().unwrap()),
        ] {
            let json = serde_json::to_string(&start_offset).unwrap();
            assert_eq!(
                serde_json::from_str::<StartOffset>(&json).unwrap(),
                start_offset
            );
        }
    }

    #[test]
    fn test_subscribe_rejects_unknown_start_offset() {
        assert_eq!(
            StartOffset::try_from("middle".to_string()),
            Err(ConfigError::InvalidStartOffset {
                value: "middle".to_string()
            })
        );
        let json = r#"{ "name": "n", "topic": "t", "start_offset": "middle" }"#;
        assert!(serde_json::from_str::<Subscribe>(json).is_err());
    }

    #[test]
    fn test_start_offset_rejects_a_timestamp_before_1970() {
        assert!(matches!(
            StartOffset::try_from("1969-12-31T23:59:59Z".to_string()),
            Err(ConfigError::InvalidStartOffset { .. })
        ));
    }

    #[test]
    fn test_subscribe_parses_max_deliver() {
        let json = r#"{ "name": "n", "topic": "t", "max_deliver": 3 }"#;
        let config: Subscribe = serde_json::from_str(json).unwrap();

        assert_eq!(config.max_deliver, std::num::NonZeroU32::new(3));
        assert_eq!(config.render(&serde_json::json!({})).unwrap(), config);
    }

    #[test]
    fn test_subscribe_rejects_a_backoff_list() {
        let json = r#"{ "name": "n", "topic": "t", "backoff": ["1s"] }"#;
        assert!(serde_json::from_str::<Subscribe>(json).is_err());
    }

    #[test]
    fn test_subscribe_rejects_zero_max_deliver() {
        let json = r#"{ "name": "n", "topic": "t", "max_deliver": 0 }"#;
        assert!(serde_json::from_str::<Subscribe>(json).is_err());
    }

    #[test]
    fn test_subscribe_rejects_unknown_fields() {
        let json = r#"{ "name": "n", "topic": "t", "group_id": "g" }"#;
        assert!(serde_json::from_str::<Subscribe>(json).is_err());
    }

    #[test]
    fn test_subscribe_validate_rejects_empty_topic_and_brokers() {
        let empty_topic = Subscribe {
            name: "n".into(),
            ..Default::default()
        };
        assert_eq!(empty_topic.validate(), Err(ConfigError::EmptyTopic));

        let empty_brokers = Subscribe {
            name: "n".into(),
            topic: "t".into(),
            brokers: " ".into(),
            ..Default::default()
        };
        assert_eq!(empty_brokers.validate(), Err(ConfigError::EmptyBrokers));
    }

    #[test]
    fn test_subscribe_validate_rejects_a_zero_ack_timeout() {
        let config = |ack_timeout| Subscribe {
            name: "n".into(),
            topic: "t".into(),
            ack_timeout,
            ..Default::default()
        };

        assert_eq!(
            config(Some(Duration::ZERO)).validate(),
            Err(ConfigError::ZeroAckTimeout)
        );
        assert_eq!(config(Some(Duration::from_secs(1))).validate(), Ok(()));
        assert_eq!(config(None).validate(), Ok(()));
    }
}
