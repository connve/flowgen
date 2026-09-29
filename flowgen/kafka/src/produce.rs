//! # Kafka Produce
//!
//! Publishes the incoming event to a Kafka topic and emits the delivery
//! result downstream. The payload keeps the event's native shape: JSON is
//! serialized as-is, bytes and Avro are sent raw, and Arrow record batches
//! become an Arrow IPC stream.

use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use futures_util::future;
use rskafka::client::error::{Error as KafkaError, ProtocolError};
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::record::Record;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ProduceResult {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
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
    #[error("Produce error: {source}")]
    Produce {
        #[source]
        source: Box<KafkaError>,
    },
    #[error("Broker did not acknowledge the message to '{topic}' within {timeout:?}")]
    ProduceTimeout {
        topic: String,
        timeout: std::time::Duration,
    },
    #[error("Broker acknowledged the message to '{topic}' without an offset")]
    MissingOffset { topic: String },
    #[error("Topic '{topic}' does not exist on the Kafka cluster")]
    TopicNotFound { topic: String },
    #[error("Topic '{topic}' has no partitions")]
    NoPartitions { topic: String },
    #[error("Topic creation error for '{topic}': {source}")]
    TopicCreation {
        topic: String,
        #[source]
        source: Box<KafkaError>,
    },
    #[error("Broker rejected creation of topic '{topic}': {protocol_error}")]
    TopicCreationRejected {
        topic: String,
        protocol_error: ProtocolError,
    },
    #[error("Metadata fetch error: {source}")]
    MetadataFetch {
        #[source]
        source: Box<KafkaError>,
    },
    #[error("Error connecting to partition {partition} of '{topic}': {source}")]
    PartitionClient {
        topic: String,
        partition: i32,
        #[source]
        source: Box<KafkaError>,
    },
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Arrow serialization error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Client is missing or not initialized")]
    MissingClient,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error(
        "Client registry type mismatch -- same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),
    #[error("Replication factor {value} is out of range")]
    ReplicationFactorOutOfRange { value: i32 },
    #[error(
        "Topic '{topic}' was created but did not appear in the cluster metadata within {timeout:?}"
    )]
    TopicNotVisible {
        topic: String,
        timeout: std::time::Duration,
    },
}

impl Error {
    /// Whether retrying this error can only produce the same failure.
    ///
    /// Bad config or an unserializable payload does not become valid on the
    /// next attempt, so retrying one just delays the failure by the full
    /// backoff.
    fn is_permanent(&self) -> bool {
        match self {
            Error::ClientAuth {
                source: crate::client::Error::Connect { .. },
            } => false,
            Error::ConfigRender { .. }
            | Error::SerdeJson { .. }
            | Error::Arrow { .. }
            | Error::TopicNotFound { .. }
            | Error::NoPartitions { .. }
            | Error::TopicCreationRejected { .. }
            | Error::MissingClient
            | Error::ClientAuth { .. }
            | Error::ClientRegistryMismatch
            | Error::Config(_)
            | Error::ReplicationFactorOutOfRange { .. } => true,
            _ => false,
        }
    }
}

fn serialize_event_to_bytes(event: &Event) -> Result<Vec<u8>, Error> {
    match &event.data {
        EventData::ArrowRecordBatch(data) => {
            let mut buffer = Vec::new();
            let mut stream_writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &data.schema())
                    .map_err(|e| Error::Arrow { source: e })?;
            stream_writer
                .write(data)
                .map_err(|e| Error::Arrow { source: e })?;
            stream_writer
                .finish()
                .map_err(|e| Error::Arrow { source: e })?;
            Ok(buffer)
        }
        EventData::Avro(data) => Ok(data.raw_bytes.clone()),
        EventData::Json(data) => {
            serde_json::to_vec(data).map_err(|e| Error::SerdeJson { source: e })
        }
        EventData::Bytes(bytes) => Ok(bytes.to_vec()),
    }
}

/// Patches a UUID v7 id into the "event.id" field of the render context
/// when the incoming event has no id, so templates like "{{event.id}}" in
/// `message_key` always resolve to a value.
fn ensure_event_id(event_value: &mut serde_json::Value) {
    let id_is_null = event_value
        .get("event")
        .and_then(|e| e.get("id"))
        .is_none_or(|id| id.is_null());
    if id_is_null {
        if let Some(event_obj) = event_value.get_mut("event").and_then(|e| e.as_object_mut()) {
            event_obj.insert(
                "id".to_string(),
                serde_json::json!(uuid::Uuid::now_v7().to_string()),
            );
        }
    }
}

/// Kafka's murmur2 hash, as used by the Java client's default partitioner.
fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let mut h = SEED ^ data.len() as u32;
    let (chunks, tail) = data.as_chunks::<4>();
    for chunk in chunks {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    match tail.len() {
        3 => {
            h ^= u32::from(tail[2]) << 16;
            h ^= u32::from(tail[1]) << 8;
            h ^= u32::from(tail[0]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(tail[1]) << 8;
            h ^= u32::from(tail[0]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(tail[0]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

/// Partition index for a keyed message, matching the Java client's default
/// partitioner so producers in other languages agree on where a key lands.
fn partition_for_key(key: &[u8], partitions: usize) -> usize {
    (murmur2(key) & 0x7fff_ffff) as usize % partitions
}

/// Ensures the configured topic exists.
///
/// When `create_or_update` is `true` the topic is created from
/// `topic_options` if it does not already exist. When `false` an error is
/// returned if the topic is absent from the cluster.
///
/// Existence is checked against the metadata of every topic rather than by
/// asking for this one by name: a broker with `auto.create.topics.enable`
/// creates a topic asked for by name right there, with broker defaults,
/// before `topic_options` can be applied.
async fn setup_topic(
    client: &rskafka::client::Client,
    config: &super::config::Produce,
) -> Result<(), Error> {
    let topic = config.topic.as_str();
    let topics = client
        .list_topics()
        .await
        .map_err(|source| Error::MetadataFetch {
            source: Box::new(source),
        })?;
    if topics.iter().any(|t| t.name == topic) {
        return Ok(());
    }
    if !config.create_or_update {
        return Err(Error::TopicNotFound {
            topic: topic.to_string(),
        });
    }

    let topic_options = &config.topic_options;
    let replication_factor = i16::try_from(topic_options.replication_factor).map_err(|_| {
        Error::ReplicationFactorOutOfRange {
            value: topic_options.replication_factor,
        }
    })?;
    let timeout = crate::client::clamp_timeout(config.ack_timeout);
    let controller = client
        .controller_client()
        .map_err(|source| Error::TopicCreation {
            topic: topic.to_string(),
            source: Box::new(source),
        })?;
    match controller
        .create_topic_with_configs(
            topic,
            topic_options.partitions,
            replication_factor,
            topic_options.broker_config().into_iter().collect(),
            timeout.as_millis() as i32,
        )
        .await
    {
        Ok(())
        | Err(KafkaError::ServerError {
            protocol_error: ProtocolError::TopicAlreadyExists,
            ..
        }) => {}
        Err(KafkaError::ServerError { protocol_error, .. }) => {
            return Err(Error::TopicCreationRejected {
                topic: topic.to_string(),
                protocol_error,
            })
        }
        Err(source) => {
            return Err(Error::TopicCreation {
                topic: topic.to_string(),
                source: Box::new(source),
            })
        }
    }
    wait_for_topic(client, topic, timeout).await
}

/// Waits until a just-created topic shows up in the cluster metadata, which
/// a broker other than the controller can take a moment to learn about.
async fn wait_for_topic(
    client: &rskafka::client::Client,
    topic: &str,
    timeout: std::time::Duration,
) -> Result<(), Error> {
    let visible = async {
        loop {
            let topics = client
                .list_topics()
                .await
                .map_err(|source| Error::MetadataFetch {
                    source: Box::new(source),
                })?;
            if topics.iter().any(|t| t.name == topic) {
                return Ok(());
            }
            tokio::time::sleep(TOPIC_VISIBILITY_POLL_INTERVAL).await;
        }
    };
    match tokio::time::timeout(timeout, visible).await {
        Ok(result) => result,
        Err(_) => Err(Error::TopicNotVisible {
            topic: topic.to_string(),
            timeout,
        }),
    }
}

/// How often a just-created topic is looked up until it becomes visible.
const TOPIC_VISIBILITY_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// How long a topic's partition list is used before it is looked up again,
/// so partitions added to the topic are picked up.
const METADATA_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// A topic's partition clients and when they were looked up.
struct TopicPartitions {
    clients: Arc<Vec<PartitionClient>>,
    fetched_at: std::time::Instant,
}

pub struct EventHandler {
    client: Arc<rskafka::client::Client>,
    partitions: tokio::sync::Mutex<HashMap<String, TopicPartitions>>,
    next_partition: AtomicUsize,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<super::config::Produce>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    async fn partition_clients(&self, topic: &str) -> Result<Arc<Vec<PartitionClient>>, Error> {
        let mut cache = self.partitions.lock().await;
        if let Some(cached) = cache.get(topic) {
            if cached.fetched_at.elapsed() < METADATA_MAX_AGE {
                return Ok(Arc::clone(&cached.clients));
            }
        }
        let topics = self
            .client
            .list_topics()
            .await
            .map_err(|source| Error::MetadataFetch {
                source: Box::new(source),
            })?;
        let Some(metadata) = topics.into_iter().find(|t| t.name == topic) else {
            return Err(Error::TopicNotFound {
                topic: topic.to_string(),
            });
        };
        let mut clients = Vec::with_capacity(metadata.partitions.len());
        for partition in metadata.partitions {
            let client = self
                .client
                .partition_client(topic, partition, UnknownTopicHandling::Retry)
                .await
                .map_err(|source| Error::PartitionClient {
                    topic: topic.to_string(),
                    partition,
                    source: Box::new(source),
                })?;
            clients.push(client);
        }
        if clients.is_empty() {
            return Err(Error::NoPartitions {
                topic: topic.to_string(),
            });
        }
        let clients = Arc::new(clients);
        cache.insert(
            topic.to_string(),
            TopicPartitions {
                clients: Arc::clone(&clients),
                fetched_at: std::time::Instant::now(),
            },
        );
        Ok(clients)
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx = event.completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let mut event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;

            ensure_event_id(&mut event_value);

            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;
            config.validate()?;

            let payload = serialize_event_to_bytes(event.as_ref())?;

            // `config` is already rendered, so the key is too. Rendering it a
            // second time would treat event data containing `{{ }}` as a
            // template of its own.
            let key = config
                .message_key
                .as_ref()
                .map(|key| key.as_bytes().to_vec());
            let clients = self.partition_clients(&config.topic).await?;
            let index = match &key {
                Some(key) => partition_for_key(key, clients.len()),
                None => self.next_partition.fetch_add(1, Ordering::Relaxed) % clients.len(),
            };
            let partition_client = &clients[index];
            let record = Record {
                key: match key {
                    Some(key) => Some(key),
                    None => Some(Vec::new()),
                },
                value: Some(payload),
                headers: BTreeMap::new(),
                timestamp: chrono::Utc::now(),
            };

            let timeout = crate::client::clamp_timeout(config.ack_timeout);
            let produced = tokio::time::timeout(
                timeout,
                partition_client.produce(vec![record], Compression::NoCompression),
            )
            .await
            .map_err(|_| Error::ProduceTimeout {
                topic: config.topic.clone(),
                timeout,
            })?
            .map_err(|source| Error::Produce {
                source: Box::new(source),
            })?;
            let partition = partition_client.partition();
            let offset = produced
                .offsets
                .first()
                .copied()
                .ok_or_else(|| Error::MissingOffset {
                    topic: config.topic.clone(),
                })?;

            let result = ProduceResult {
                topic: config.topic.clone(),
                partition,
                offset,
            };

            let result_json =
                serde_json::to_value(&result).map_err(|e| Error::SerdeJson { source: e })?;

            let mut e = EventBuilder::new()
                .subject(self.config.name.clone())
                .data(EventData::Json(result_json))
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            match self.tx {
                None => {
                    if let Some(tx) = completion_tx.as_ref() {
                        tx.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => {
                    e.completion_tx = completion_tx.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .context("topic", &config.topic)
                .context("partition", partition)
                .context("offset", offset)
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

#[derive(Debug)]
pub struct Producer {
    config: Arc<super::config::Produce>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Producer {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        init_config.validate()?;

        let kafka_key = flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
            .field("credentials_path", &init_config.credentials_path)
            .field("brokers", &init_config.brokers)
            .field("ack_timeout", &init_config.ack_timeout)
            .build();
        let client = self
            .task_context
            .client_registry
            .get_or_init(kafka_key, || {
                let credentials_path = init_config.credentials_path.clone();
                let brokers = init_config.brokers.clone();
                let ack_timeout = init_config.ack_timeout;
                async move {
                    let client =
                        crate::client::Client::new(credentials_path, Some(brokers), ack_timeout);
                    client
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

        setup_topic(&client, &init_config).await?;

        let event_handler = EventHandler {
            client: Arc::clone(&client),
            partitions: tokio::sync::Mutex::new(HashMap::new()),
            next_partition: AtomicUsize::new(0),
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) if e.is_permanent() => {
                        error!(error = %e, "Failed to initialize Kafka producer");
                        Err(tokio_retry::RetryError::permanent(e))
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to initialize Kafka producer");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        let mut handlers = Vec::new();

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                future::join_all(handlers).await;
                return Ok(());
            }

            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    let handle = tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) if e.is_permanent() => {
                                        error!(error = %e, "Failed to produce message");
                                        Err(tokio_retry::RetryError::permanent(e))
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Failed to produce message");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Failed to produce message after all retry attempts");
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                } else if let Some(arc) = event.completion_tx.as_ref() {
                                    arc.signal_completion_with_error(err.to_string());
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                    handlers.push(handle);
                    handlers.retain(|h| !h.is_finished());
                }
                None => {
                    future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

#[derive(Default)]
pub struct ProducerBuilder {
    config: Option<Arc<super::config::Produce>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProducerBuilder {
    pub fn new() -> ProducerBuilder {
        ProducerBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Produce>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
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

    pub async fn build(self) -> Result<Producer, Error> {
        Ok(Producer {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
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
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::{Map, Value};
    use std::sync::Arc as StdArc;
    use tokio::sync::mpsc;

    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Producer Test".to_string()),
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

    // ------------------------------------------------------------------
    // Error display
    // ------------------------------------------------------------------

    #[test]
    fn test_unreachable_broker_is_retried_but_bad_credentials_are_not() {
        let unreachable = Error::ClientAuth {
            source: crate::client::Error::Connect {
                source: Box::new(KafkaError::InvalidResponse("Connection refused".into())),
            },
        };
        let no_credentials = Error::ClientAuth {
            source: crate::client::Error::NoCredentials,
        };

        assert!(!unreachable.is_permanent());
        assert!(no_credentials.is_permanent());
    }

    #[test]
    fn test_error_display() {
        assert_eq!(
            Error::TopicNotFound { topic: "x".into() }.to_string(),
            "Topic 'x' does not exist on the Kafka cluster"
        );
        assert_eq!(
            Error::MissingClient.to_string(),
            "Client is missing or not initialized"
        );
        assert_eq!(
            Error::MissingBuilderAttribute("foo".into()).to_string(),
            "Missing required builder attribute: foo"
        );
        assert_eq!(
            Error::ClientRegistryMismatch.to_string(),
            "Client registry type mismatch -- same credentials used with incompatible client types"
        );
    }

    // ------------------------------------------------------------------
    // ProduceResult round-trip
    // ------------------------------------------------------------------

    #[test]
    fn test_produce_result_round_trip() {
        let r = ProduceResult {
            topic: "t".into(),
            partition: 2,
            offset: 99,
        };
        let json = serde_json::to_value(&r).unwrap();
        let back: ProduceResult = serde_json::from_value(json).unwrap();
        assert_eq!(back.topic, "t");
        assert_eq!(back.partition, 2);
        assert_eq!(back.offset, 99);
    }

    // ------------------------------------------------------------------
    // serialize_event_to_bytes
    // ------------------------------------------------------------------

    #[test]
    fn test_serialize_json() {
        let event = Event {
            data: EventData::Json(serde_json::json!({"hello": "world"})),
            subject: "s".into(),
            id: None,
            timestamp: 0,
            task_id: 0,
            task_type: "",
            meta: None,
            error: None,
            completion_tx: None,
        };
        let bytes = serialize_event_to_bytes(&event).unwrap();
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, serde_json::json!({"hello": "world"}));
    }

    #[test]
    fn test_serialize_bytes() {
        let event = Event {
            data: EventData::Bytes(bytes::Bytes::from(&b"raw data"[..])),
            subject: "s".into(),
            id: None,
            timestamp: 0,
            task_id: 0,
            task_type: "",
            meta: None,
            error: None,
            completion_tx: None,
        };
        let bytes = serialize_event_to_bytes(&event).unwrap();
        assert_eq!(bytes, b"raw data");
    }

    #[test]
    fn test_serialize_arrow() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            StdArc::new(schema),
            vec![StdArc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let event = Event {
            data: EventData::ArrowRecordBatch(batch),
            subject: "s".into(),
            id: None,
            timestamp: 0,
            task_id: 0,
            task_type: "",
            meta: None,
            error: None,
            completion_tx: None,
        };
        let bytes = serialize_event_to_bytes(&event).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_serialize_avro() {
        let data = EventData::Avro(flowgen_core::event::AvroData {
            schema: r#"{"type":"record","name":"r","fields":[{"name":"x","type":"int"}]}"#.into(),
            raw_bytes: vec![0x01],
        });
        let event = Event {
            data,
            subject: "s".into(),
            id: None,
            timestamp: 0,
            task_id: 0,
            task_type: "",
            meta: None,
            error: None,
            completion_tx: None,
        };
        let bytes = serialize_event_to_bytes(&event).unwrap();
        assert_eq!(bytes, vec![0x01]);
    }

    // ------------------------------------------------------------------
    // ProducerBuilder
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_producer_builder_success() {
        let config = Arc::new(super::super::config::Produce {
            name: "test_kafka_producer".to_string(),
            brokers: "localhost:9092".to_string(),
            topic: "test-topic".to_string(),
            ..Default::default()
        });
        let (tx, rx) = mpsc::channel(100);

        let producer = ProducerBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(producer.is_ok());

        let p = producer.unwrap();
        assert_eq!(p.config.name, "test_kafka_producer");
        assert!(p.tx.is_some());
    }

    #[tokio::test]
    async fn test_producer_builder_without_sender() {
        let config = Arc::new(super::super::config::Produce {
            name: "leaf".to_string(),
            brokers: "localhost:9092".to_string(),
            topic: "leaf-topic".to_string(),
            ..Default::default()
        });
        let (_tx, rx) = mpsc::channel(100);
        let producer = ProducerBuilder::new()
            .config(config)
            .receiver(rx)
            .task_id(2)
            .task_type("leaf")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(producer.is_ok());
        assert!(producer.unwrap().tx.is_none());
    }

    #[tokio::test]
    async fn test_producer_builder_missing_each_field() {
        let full_config = || {
            Arc::new(super::super::config::Produce {
                name: "t".into(),
                brokers: "b:9092".into(),
                topic: "t".into(),
                ..Default::default()
            })
        };
        let ctx = create_mock_task_context();

        // Missing config
        let (_, rx) = mpsc::channel(10);
        let e = ProducerBuilder::new()
            .receiver(rx)
            .task_context(ctx.clone())
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::MissingBuilderAttribute(attr) if attr == "config"));

        // Missing receiver
        let (tx, _) = mpsc::channel(10);
        let e = ProducerBuilder::new()
            .config(full_config())
            .sender(tx)
            .task_context(ctx.clone())
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::MissingBuilderAttribute(attr) if attr == "receiver"));

        // Missing task_context
        let (_, rx) = mpsc::channel(10);
        let e = ProducerBuilder::new()
            .config(full_config())
            .receiver(rx)
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::MissingBuilderAttribute(attr) if attr == "task_context"));
    }

    // ------------------------------------------------------------------
    // Config create_or_update field
    // ------------------------------------------------------------------

    #[test]
    fn test_config_create_or_update_default() {
        let config = super::super::config::Produce::default();
        assert!(!config.create_or_update);
    }

    #[test]
    fn test_config_create_or_update_round_trip() {
        let config = super::super::config::Produce {
            name: "test".into(),
            brokers: "b:9092".into(),
            topic: "t".into(),
            create_or_update: true,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: super::super::config::Produce = serde_json::from_str(&json).unwrap();
        assert!(deserialized.create_or_update);
    }

    // ------------------------------------------------------------------
    // message_key id fallback
    // ------------------------------------------------------------------

    #[test]
    fn test_murmur2_matches_the_java_client() {
        let cases: [(&[u8], i32); 6] = [
            (b"21", -973_932_308),
            (b"foobar", -790_332_482),
            (b"a-little-bit-long-string", -985_981_536),
            (b"a-little-bit-longer-string", -1_486_304_829),
            (
                b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58_897_971,
            ),
            (b"abc", 479_470_107),
        ];

        for (key, expected) in cases {
            assert_eq!(murmur2(key) as i32, expected, "murmur2({key:?})");
        }
    }

    #[test]
    fn test_partition_for_key_is_stable_and_in_range() {
        for partitions in 1..=8 {
            let partition = partition_for_key(b"customer-42", partitions);
            assert!(partition < partitions);
            assert_eq!(partition, partition_for_key(b"customer-42", partitions));
        }
    }

    #[test]
    fn test_ensure_event_id_patches_null_id() {
        let mut event_value = serde_json::json!({
            "event": { "id": null, "subject": "s", "data": 42 }
        });
        ensure_event_id(&mut event_value);
        let id = event_value["event"]["id"].as_str().unwrap();
        assert!(!id.is_empty());
        uuid::Uuid::parse_str(id).is_ok().then_some(()).unwrap();
    }

    #[test]
    fn test_ensure_event_id_preserves_existing_id() {
        let mut event_value = serde_json::json!({
            "event": { "id": "existing-id", "subject": "s" }
        });
        ensure_event_id(&mut event_value);
        assert_eq!(event_value["event"]["id"], "existing-id");
    }

    #[test]
    fn test_message_key_template_resolves_fallback_id() {
        let mut event_value = serde_json::json!({
            "event": { "id": null, "subject": "s", "data": 42 }
        });
        ensure_event_id(&mut event_value);
        let rendered =
            flowgen_core::config::render_template("key-{{event.id}}", &event_value).unwrap();
        let id = event_value["event"]["id"].as_str().unwrap();
        assert_eq!(rendered, format!("key-{id}"));
        assert_ne!(rendered, "key-");
    }

    #[test]
    fn test_message_key_does_not_re_render_event_data() {
        let config = super::super::config::Produce {
            name: "p".to_string(),
            topic: "t".to_string(),
            message_key: Some("key-{{event.data.k}}".to_string()),
            ..Default::default()
        };
        let event_value = serde_json::json!({
            "event": { "id": "i", "data": { "k": "{{event.id}}" } }
        });

        let rendered = config.render(&event_value).expect("render");

        assert_eq!(
            rendered.message_key.as_deref(),
            Some("key-{{event.id}}"),
            "data that looks like a template must survive as a literal key"
        );
    }
}
