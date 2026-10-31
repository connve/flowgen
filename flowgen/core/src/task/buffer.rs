//! The `buffer` task: collects individual events into batches before forwarding them
//! downstream, flushing on size and timeout triggers.
//!
//! This enables efficient input/output operations, reduces API calls to cloud storage,
//! and is essential for columnar formats like Parquet.

use crate::config::ConfigExt;
use crate::event::{
    new_completion_channel, Event, EventBuilder, EventData, EventExt, SharedCompletionTx,
};
use crate::task::TaskConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Instant};
use tracing::{error, Instrument};

/// Configuration for buffer processor task.
///
/// Accumulates individual events into batches and emits them based on size or timeout triggers.
///
/// # Examples
///
/// Basic buffer configuration:
/// ```yaml
/// buffer:
///   name: "batch_rows"
///   size: 1000
/// ```
///
/// Buffer with custom timeout:
/// ```yaml
/// buffer:
///   name: "batch_rows"
///   size: 75
///   timeout: "10s"
/// ```
///
/// Buffer with partitioned grouping:
/// ```yaml
/// buffer:
///   name: "batch_by_program_country"
///   size: 75
///   timeout: "10s"
///   partition_key: "{{event.data.program_id}}.{{event.data.country}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Buffer {
    /// Unique name identifier for this buffer task.
    pub name: String,
    /// Number of events to collect before flushing the buffer.
    pub size: usize,
    /// Optional timeout duration to flush buffer even if not full (default: 30s),
    /// measured from the first event in the batch.
    /// Accepts duration strings: "100ms", "30s", "5m", etc.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Option<Duration>,
    /// Optional key template for partitioning events into separate buffers.
    /// When specified, events are partitioned by the rendered key value and each partition
    /// is buffered and flushed independently based on size and timeout.
    pub partition_key: Option<String>,
    /// When true, flush as soon as the source's batch is complete instead
    /// of waiting for `size` or `timeout`. Sources that emit a known-finite
    /// batch (`git_sync`, `oci_sync`) attach a completion handle to its
    /// final event. Events from an `iterate` are batched per array, and
    /// the array's batches flush once every element has arrived.
    #[serde(default)]
    pub flush_on_completion: bool,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    pub retry: Option<crate::retry::RetryConfig>,
}

/// Default timeout value of 30 seconds.
fn default_timeout() -> Option<Duration> {
    Some(Duration::from_secs(30))
}

impl ConfigExt for Buffer {}

/// Errors that can occur during buffer processing operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: crate::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("Expected JSON event data, got ArrowRecordBatch")]
    ExpectedJsonGotArrowRecordBatch,
    #[error("Expected JSON event data, got Avro")]
    ExpectedJsonGotAvro,
    #[error("Expected JSON event data, got binary Bytes")]
    ExpectedJsonGotBytes,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Error rendering buffer key template: {source}")]
    Render {
        #[source]
        source: crate::config::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Buffer flush serialization error: {source}")]
    FlushSerialization {
        #[source]
        source: serde_json::Error,
    },
    #[error("Partition key template is configured but rendered to empty value")]
    MissingPartitionKey,
}

/// Flush reason for tracking why a buffer was flushed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FlushReason {
    /// Buffer reached the configured size limit.
    Size,
    /// Timeout elapsed since the batch's first event.
    Timeout,
    /// Shutdown signal received.
    Shutdown,
    /// The source's batch is complete and the buffer was configured with
    /// `flush_on_completion`.
    Completion,
}

/// Output structure for flushed buffer data.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FlushData {
    /// Array of buffered events.
    batch: Vec<Value>,
    /// Number of events in this batch.
    batch_size: usize,
    /// Reason why this buffer was flushed.
    flush_reason: FlushReason,
    /// Optional key that was used to group these events.
    #[serde(skip_serializing_if = "Option::is_none")]
    partition_key: Option<String>,
}

/// What a flushed event passes on from the events in its batch.
#[derive(Default)]
struct Origin {
    /// Meta of the batch's first event.
    meta: Option<Map<String, Value>>,
    /// The batch's user with their newest token; none once the batch mixes users.
    session: Option<Arc<crate::auth::UserSession>>,
    /// Call stack of the batch's first event.
    call_stack: Vec<String>,
}

impl Origin {
    fn new(
        meta: Option<Map<String, Value>>,
        session: Option<Arc<crate::auth::UserSession>>,
        call_stack: Vec<String>,
    ) -> Self {
        Self {
            meta,
            session,
            call_stack,
        }
    }

    /// Takes in the session of a later event in the batch.
    fn add(&mut self, session: Option<&Arc<crate::auth::UserSession>>) {
        self.session = match (self.session.take(), session) {
            (Some(current), Some(next)) if current.user.user_id == next.user.user_id => {
                Some(Arc::clone(next))
            }
            _ => None,
        };
    }
}

/// Buffer processor that accumulates events into batches.
///
/// Unlike typical processors that handle events individually, this processor maintains
/// state across events and uses `tokio::select!` to handle multiple triggers:
/// - Size trigger when buffer reaches configured size
/// - Timeout trigger to flush partial batches
/// - Shutdown trigger to flush remaining events on graceful shutdown
#[derive(Debug)]
pub struct Processor {
    /// Buffer processor configuration.
    config: Arc<Buffer>,
    /// Channel sender for processed events.
    tx: Option<Sender<Event>>,
    /// Channel receiver for incoming events.
    rx: Receiver<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

/// The `iterate` array an event came from.
struct IterateArray {
    id: String,
    count: usize,
}

/// Removes the `iterate` meta from an event, so it never reaches the meta a
/// batch passes on, and returns the array it names.
fn take_iterate_array(meta: &mut Option<Map<String, Value>>) -> Option<IterateArray> {
    let meta = meta.as_mut()?;
    match (
        meta.remove(super::iterate::ITERATE_ID_META),
        meta.remove(super::iterate::ITERATE_COUNT_META),
    ) {
        (Some(Value::String(id)), Some(Value::Number(count))) => Some(IterateArray {
            id,
            count: usize::try_from(count.as_u64()?).ok()?,
        }),
        _ => None,
    }
}

/// Renders `template` for `event` as text, so a key like `{{a}}.{{b}}` keeps
/// its digits as written. An empty key, from a missing or null field, fails.
fn partition_key(template: &str, event: &Event) -> Result<String, Error> {
    let event_value =
        crate::event::template_value(event, true).map_err(|source| Error::Render {
            source: crate::config::Error::Event {
                source: Box::new(source),
            },
        })?;
    match crate::config::render_template(template, &event_value) {
        Ok(key) if key.is_empty() => Err(Error::MissingPartitionKey),
        Ok(key) => Ok(key),
        Err(source) => Err(Error::Render { source }),
    }
}

/// When an event shows the source's batch is complete: an `iterate` array once
/// every element arrived, any other batch once its final event arrives.
enum CompletionCue {
    Never,
    FinalEvent,
    Array(IterateArray),
}

impl CompletionCue {
    fn of(flush_on_completion: bool, array: Option<IterateArray>, final_event: bool) -> Self {
        match (flush_on_completion, array, final_event) {
            (false, _, _) => Self::Never,
            (true, Some(array), _) => Self::Array(array),
            (true, None, true) => Self::FinalEvent,
            (true, None, false) => Self::Never,
        }
    }
}

/// Identifies a batch: its rendered partition key and, with
/// `flush_on_completion`, the `iterate` array its events came from.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BatchKey {
    partition: Option<String>,
    array: Option<String>,
}

/// Events collected under one key until they are flushed.
struct Batch {
    events: Vec<Value>,
    origin: Origin,
    /// Distinct upstream completions to signal once the batch is flushed.
    completions: Vec<SharedCompletionTx>,
    /// When the batch got its first event; the timeout runs from here.
    started: Instant,
}

impl Batch {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            origin: Origin::default(),
            completions: Vec::new(),
            started: Instant::now(),
        }
    }

    fn push(&mut self, data: Value, event: Event) {
        match self.events.is_empty() {
            true => self.origin = Origin::new(event.meta, event.session, event.call_stack),
            false => self.origin.add(event.session.as_ref()),
        }
        self.events.push(data);
        if let Some(tx) = event.completion_tx {
            absorb_completion(&mut self.completions, tx);
        }
    }
}

/// How many elements of an `iterate` array arrived, across its batches' flushes.
struct ArrayProgress {
    received: usize,
    /// When the last element arrived; the progress is dropped a timeout later.
    last_received: Instant,
}

/// The batches being collected and the `iterate` arrays they count.
#[derive(Default)]
struct Batches {
    open: HashMap<BatchKey, Batch>,
    arrays: HashMap<String, ArrayProgress>,
}

impl Batches {
    fn is_empty(&self) -> bool {
        self.open.is_empty() && self.arrays.is_empty()
    }

    /// Time until the earliest batch or array progress times out.
    fn until_timeout(&self, timeout: Duration) -> Duration {
        self.open
            .values()
            .map(|batch| batch.started)
            .chain(self.arrays.values().map(|progress| progress.last_received))
            .map(|since| timeout.saturating_sub(since.elapsed()))
            .min()
            .unwrap_or(timeout)
    }

    /// Removes the batch under `key`.
    fn take(&mut self, key: &BatchKey) -> Vec<(BatchKey, Batch)> {
        self.open.remove_entry(key).into_iter().collect()
    }

    /// Counts an element of `array`; once every element arrived, removes the
    /// array's batches in all partitions.
    fn receive_element(&mut self, array: &IterateArray) -> Option<Vec<(BatchKey, Batch)>> {
        let progress = self
            .arrays
            .entry(array.id.clone())
            .or_insert(ArrayProgress {
                received: 0,
                last_received: Instant::now(),
            });
        progress.received += 1;
        progress.last_received = Instant::now();
        if progress.received < array.count {
            return None;
        }
        self.arrays.remove(&array.id);
        Some(
            self.open
                .extract_if(|key, _| key.array.as_deref() == Some(array.id.as_str()))
                .collect(),
        )
    }

    /// Drops array progress and removes batches whose timeout elapsed.
    fn take_expired(&mut self, timeout: Duration) -> Vec<(BatchKey, Batch)> {
        self.arrays
            .retain(|_, progress| progress.last_received.elapsed() < timeout);
        self.open
            .extract_if(|_, batch| batch.started.elapsed() >= timeout)
            .collect()
    }
}

/// Adds an upstream completion tx to the absorbed set if not already present.
/// Dedupes by Arc pointer so cloned-from-same-source events do not produce
/// duplicate signals on flush. Same source = same Arc; concurrent sources =
/// distinct Arcs.
fn absorb_completion(absorbed: &mut Vec<SharedCompletionTx>, tx: SharedCompletionTx) {
    let ptr = Arc::as_ptr(&tx);
    if !absorbed.iter().any(|existing| Arc::as_ptr(existing) == ptr) {
        absorbed.push(tx);
    }
}

/// Builds the completion handle to attach to a flushed batch event.
///
/// - Zero absorbed upstreams: no completion to forward.
/// - One absorbed upstream: pass it through directly — no proxy task needed.
/// - Two or more distinct upstreams: create a proxy `CompletionState`, attach
///   it to the flushed event, and spawn a task that fans the eventual ack
///   signal back out to every absorbed upstream. This keeps the per-source
///   ack contract intact when the buffer coalesces events from concurrent
///   sources (e.g. parallel webhook requests).
fn spawn_completion_proxy(
    mut upstreams: Vec<SharedCompletionTx>,
    downstream_leaf_count: usize,
) -> Option<SharedCompletionTx> {
    match upstreams.len() {
        0 => None,
        1 => upstreams.pop(),
        _ => {
            let (proxy_tx, proxy_rx) = new_completion_channel(downstream_leaf_count.max(1));
            tokio::spawn(async move {
                let payload = match proxy_rx.await {
                    Ok(Ok(p)) => p,
                    // Channel closed or completion errored — upstreams will
                    // fall back to their own ack timeouts. Nothing useful we
                    // can forward.
                    _ => return,
                };
                for upstream in &upstreams {
                    upstream.signal_completion(payload.clone());
                }
            });
            Some(proxy_tx)
        }
    }
}

impl Processor {
    /// Flushes the buffer by emitting a single event containing all buffered events.
    ///
    /// Waits until the next task takes the batch, so a slow task also slows
    /// down the buffer and everything before it.
    ///
    /// The flushed event carries a single completion downstream; once it fires,
    /// it is fanned back out to every distinct source absorbed into the batch,
    /// so every source gets acked. When the buffer is itself a leaf, every
    /// source is signalled directly.
    async fn flush_buffer(
        &self,
        batch: Batch,
        reason: FlushReason,
        partition_key: Option<String>,
    ) -> Result<(), Error> {
        let Batch {
            events: buffer,
            origin,
            completions: upstream_completions,
            ..
        } = batch;
        let tx = self.tx.clone();
        let config_name = self.config.name.clone();
        let task_id = self.task_id;
        let task_type = self.task_type;
        let leaf_count = self.task_context.leaf_count;

        async move {
            let batch_size = buffer.len();
            let flush_data = FlushData {
                batch: buffer,
                batch_size,
                flush_reason: reason,
                partition_key,
            };

            let flush_result = serde_json::to_value(flush_data)
                .map_err(|source| Error::FlushSerialization { source })?;

            let mut event_builder = EventBuilder::new()
                .data(EventData::Json(flush_result))
                .subject(config_name)
                .task_id(task_id)
                .task_type(task_type);

            if let Some(meta) = origin.meta {
                event_builder = event_builder.meta(meta);
            }
            if let Some(session) = origin.session {
                event_builder = event_builder.session(session);
            }
            event_builder.call_stack = origin.call_stack;

            let mut event = event_builder
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            match tx {
                Some(_) => {
                    event.completion_tx = spawn_completion_proxy(upstream_completions, leaf_count);
                }
                None => {
                    // Leaf task: signal every absorbed upstream directly.
                    let payload = event.data_as_json().ok();
                    for arc in &upstream_completions {
                        arc.signal_completion(payload.clone());
                    }
                }
            }

            event
                .send_with_logging(tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })
        }
        .instrument(tracing::info_span!(
            parent: tracing::Span::current(),
            "task.handle",
            duration_ms = tracing::field::Empty,
        ))
        .await
    }

    async fn flush_all(
        &self,
        batches: Vec<(BatchKey, Batch)>,
        reason: FlushReason,
    ) -> Result<(), Error> {
        for (key, batch) in batches {
            self.flush_buffer(batch, reason, key.partition).await?;
        }
        Ok(())
    }

    /// Processes the main event loop with buffer accumulation and flush triggers.
    ///
    /// Uses `tokio::select!` to handle three concurrent conditions:
    /// 1. Receiving new events and accumulating them in their batch
    /// 2. Timeout expiration to flush partial batches
    /// 3. Shutdown signal to flush remaining events
    ///
    /// Each partition, and with `flush_on_completion` each `iterate` array,
    /// has its own batch with independent size and timeout tracking. Returns
    /// an error once a flush fails, since the next task no longer takes events.
    async fn process_events(&mut self) -> Result<(), Error> {
        let timeout = self.config.timeout.unwrap_or(Duration::from_secs(30));
        let mut batches = Batches::default();

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                return Ok(());
            }

            let until_timeout = batches.until_timeout(timeout);
            tokio::select! {
                result = self.rx.recv() => match result {
                    Some(event) => self.receive(&mut batches, event).await?,
                    None => {
                        let remaining = batches.open.drain().collect();
                        return self.flush_all(remaining, FlushReason::Shutdown).await;
                    }
                },
                _ = sleep(until_timeout), if !batches.is_empty() => {
                    let expired = batches.take_expired(timeout);
                    self.flush_all(expired, FlushReason::Timeout).await?;
                }
            }
        }
    }

    /// Adds an event to its batch and flushes the batches it fills or completes.
    ///
    /// An event whose upstream task failed, or that cannot join a batch, is
    /// passed on as failed instead.
    async fn receive(&self, batches: &mut Batches, mut event: Event) -> Result<(), Error> {
        if let Some(error) = event.error.take() {
            return self.pass_on_failed(batches, event, error).await;
        }
        let (partition, data) = match self.batch_entry(&mut event) {
            Ok(entry) => entry,
            Err(error) => {
                error!(error = %error, correlation_id = %event.correlation_id(), "Failed to add event to a batch");
                return self.pass_on_failed(batches, event, error.to_string()).await;
            }
        };
        let cue = CompletionCue::of(
            self.config.flush_on_completion,
            take_iterate_array(&mut event.meta),
            event.completion_tx.is_some(),
        );

        let key = BatchKey {
            partition,
            array: match &cue {
                CompletionCue::Array(array) => Some(array.id.clone()),
                CompletionCue::FinalEvent | CompletionCue::Never => None,
            },
        };
        let batch = batches.open.entry(key.clone()).or_insert_with(Batch::new);
        batch.push(data, event);
        let full = batch.events.len() >= self.config.size;

        let completed = match &cue {
            CompletionCue::Array(array) => batches.receive_element(array),
            CompletionCue::FinalEvent => Some(batches.take(&key)),
            CompletionCue::Never => None,
        };
        match (completed, full) {
            (Some(completed), _) => self.flush_all(completed, FlushReason::Completion).await,
            (None, true) => self.flush_all(batches.take(&key), FlushReason::Size).await,
            (None, false) => Ok(()),
        }
    }

    /// Renders the event's partition key and takes its JSON data, leaving
    /// any other data in place.
    fn batch_entry(&self, event: &mut Event) -> Result<(Option<String>, Value), Error> {
        let partition = match &self.config.partition_key {
            Some(template) => Some(partition_key(template, event)?),
            None => None,
        };
        let data = match &mut event.data {
            EventData::Json(data) => std::mem::take(data),
            EventData::ArrowRecordBatch(_) => return Err(Error::ExpectedJsonGotArrowRecordBatch),
            EventData::Avro(_) => return Err(Error::ExpectedJsonGotAvro),
            EventData::Bytes(_) => return Err(Error::ExpectedJsonGotBytes),
        };
        Ok((partition, data))
    }

    /// Passes on a failed event, which still counts as an arrived element of
    /// its `iterate` array.
    async fn pass_on_failed(
        &self,
        batches: &mut Batches,
        mut event: Event,
        error: String,
    ) -> Result<(), Error> {
        let array = take_iterate_array(&mut event.meta);
        event.forward_failure(self.tx.as_ref(), error).await;
        match (self.config.flush_on_completion, array) {
            (true, Some(array)) => match batches.receive_element(&array) {
                Some(completed) => self.flush_all(completed, FlushReason::Completion).await,
                None => Ok(()),
            },
            _ => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = ();

    /// Initializes the buffer processor.
    ///
    /// Buffer processor doesn't use a separate EventHandler since it needs to maintain
    /// state across events, so this returns unit type.
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        Ok(())
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        // Initialize (no-op for buffer processor).
        match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize buffer processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        };

        self.process_events().await
    }
}

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Buffer processor configuration (required for build).
    config: Option<Arc<Buffer>>,
    /// Event sender for passing events to next task (optional if this is the last task).
    tx: Option<Sender<Event>>,
    /// Event receiver for incoming events (required for build).
    rx: Option<Receiver<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<crate::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<Buffer>) -> Self {
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

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
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
    use serde_json::Map;
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn session(user_id: &str, token: &str) -> Arc<crate::auth::UserSession> {
        Arc::new(crate::auth::UserSession::with_token(
            crate::auth::UserContext {
                user_id: user_id.to_string(),
                claims: Default::default(),
            },
            token,
        ))
    }

    #[test]
    fn a_batch_from_one_user_acts_for_them_with_their_newest_token() {
        use secrecy::ExposeSecret;
        let mut origin = Origin::new(None, Some(session("u1", "t1")), Vec::new());

        origin.add(Some(&session("u1", "t2")));

        let kept = origin.session.expect("session");
        assert_eq!(kept.user.user_id, "u1");
        assert_eq!(kept.token.as_ref().unwrap().expose_secret(), "t2");
    }

    #[test]
    fn a_batch_from_different_users_acts_for_none_of_them() {
        let mut origin = Origin::new(None, Some(session("u1", "t1")), Vec::new());

        origin.add(Some(&session("u2", "t2")));
        origin.add(Some(&session("u1", "t3")));

        assert!(origin.session.is_none());
    }

    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Buffer Test".to_string()),
        );
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_processor_builder_success() {
        let config = Arc::new(Buffer {
            name: "test".to_string(),
            size: 100,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        });
        let (tx, rx) = mpsc::channel(100);

        let processor = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .receiver(rx)
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(processor.is_ok());
    }

    #[tokio::test]
    async fn test_processor_builder_missing_config() {
        let (tx, rx) = mpsc::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx)
            .receiver(rx)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn test_idle_gap_longer_than_timeout_does_not_split_next_batch() {
        use crate::event::EventData;
        use serde_json::json;

        let config = Arc::new(Buffer {
            name: "collect".to_string(),
            size: 10_000,
            timeout: Some(Duration::from_secs(5)),
            partition_key: None,
            flush_on_completion: true,
            depends_on: None,
            retry: None,
        });
        let (downstream_tx, mut downstream_rx) = mpsc::channel(10);
        let (upstream_tx, upstream_rx) = mpsc::channel(100);
        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();
        let handle = tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });

        tokio::time::sleep(Duration::from_secs(30)).await;

        let (completion, _completion_rx) = new_completion_channel(1);
        for i in 0..25 {
            let builder = EventBuilder::new()
                .data(EventData::Json(json!({"layer": i})))
                .subject("pull_repo".into())
                .task_id(0)
                .task_type("test");
            let builder = match i {
                24 => builder.completion_tx(completion.clone()),
                _ => builder,
            };
            upstream_tx.send(builder.build().unwrap()).await.unwrap();
            if i == 0 {
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
            }
        }
        drop(upstream_tx);
        let _ = handle.await;

        let mut batch_sizes = Vec::new();
        while let Some(flushed) = downstream_rx.recv().await {
            batch_sizes.push(flushed.data_as_json().unwrap()["batch_size"].clone());
        }
        assert_eq!(batch_sizes, vec![json!(25)]);
    }

    #[tokio::test]
    async fn test_absorb_completion_dedup() {
        let (state_a, _rx_a) = new_completion_channel(1);
        let (state_b, _rx_b) = new_completion_channel(1);

        let mut absorbed = Vec::new();
        absorb_completion(&mut absorbed, state_a.clone());
        absorb_completion(&mut absorbed, state_a.clone());
        absorb_completion(&mut absorbed, state_b.clone());
        absorb_completion(&mut absorbed, state_a.clone());

        assert_eq!(
            absorbed.len(),
            2,
            "distinct Arcs kept, duplicates collapsed"
        );
    }

    #[tokio::test]
    async fn test_flush_signals_every_distinct_upstream() {
        use crate::event::EventData;
        use serde_json::json;

        let config = Arc::new(Buffer {
            name: "test".to_string(),
            size: 2,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        });

        let (downstream_tx, mut downstream_rx) = mpsc::channel(10);
        let (upstream_tx, upstream_rx) = mpsc::channel(10);

        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        let (state_a, mut rx_a) = new_completion_channel(1);
        let (state_b, mut rx_b) = new_completion_channel(1);

        let evt_a = EventBuilder::new()
            .data(EventData::Json(json!({"src": "a"})))
            .subject("a".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(state_a)
            .build()
            .unwrap();
        let evt_b = EventBuilder::new()
            .data(EventData::Json(json!({"src": "b"})))
            .subject("b".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(state_b)
            .build()
            .unwrap();

        let handle = tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });

        upstream_tx.send(evt_a).await.unwrap();
        upstream_tx.send(evt_b).await.unwrap();

        let flushed = downstream_rx.recv().await.expect("flush event");
        let proxy_tx = flushed.completion_tx.expect("proxy completion attached");

        proxy_tx.signal_completion(None);

        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut rx_a)
                .await
                .is_ok(),
            "upstream A did not receive completion"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut rx_b)
                .await
                .is_ok(),
            "upstream B did not receive completion"
        );

        drop(upstream_tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_flush_passes_single_upstream_through_directly() {
        use crate::event::EventData;
        use serde_json::json;

        let config = Arc::new(Buffer {
            name: "test".to_string(),
            size: 2,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        });

        let (downstream_tx, mut downstream_rx) = mpsc::channel(10);
        let (upstream_tx, upstream_rx) = mpsc::channel(10);

        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        let (shared_state, _rx) = new_completion_channel(1);
        let shared_ptr = Arc::as_ptr(&shared_state);

        let evt_a = EventBuilder::new()
            .data(EventData::Json(json!({"branch": "a"})))
            .subject("a".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(shared_state.clone())
            .build()
            .unwrap();
        let evt_b = EventBuilder::new()
            .data(EventData::Json(json!({"branch": "b"})))
            .subject("b".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(shared_state)
            .build()
            .unwrap();

        let handle = tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });

        upstream_tx.send(evt_a).await.unwrap();
        upstream_tx.send(evt_b).await.unwrap();

        let flushed = downstream_rx.recv().await.expect("flush event");
        let attached = flushed
            .completion_tx
            .expect("upstream completion passed through");

        assert_eq!(
            Arc::as_ptr(&attached),
            shared_ptr,
            "single distinct upstream should pass through, not proxied"
        );

        drop(upstream_tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_absorb_completion_clone_preserves_identity() {
        let (state, _rx) = new_completion_channel(1);

        let mut absorbed = Vec::new();
        for _ in 0..5 {
            absorb_completion(&mut absorbed, state.clone());
        }

        assert_eq!(absorbed.len(), 1, "fan-out clones share identity");
        assert_eq!(Arc::as_ptr(&absorbed[0]), Arc::as_ptr(&state));
    }

    #[tokio::test]
    async fn test_flush_signals_three_concurrent_sources() {
        use crate::event::EventData;
        use serde_json::json;

        let config = Arc::new(Buffer {
            name: "test".to_string(),
            size: 3,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        });

        let (downstream_tx, mut downstream_rx) = mpsc::channel(10);
        let (upstream_tx, upstream_rx) = mpsc::channel(10);

        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        let (state_a, mut rx_a) = new_completion_channel(1);
        let (state_b, mut rx_b) = new_completion_channel(1);
        let (state_c, mut rx_c) = new_completion_channel(1);

        let mk_event = |name: &'static str, state: SharedCompletionTx| {
            EventBuilder::new()
                .data(EventData::Json(json!({ "src": name })))
                .subject(name.into())
                .task_id(0)
                .task_type("test")
                .completion_tx(state)
                .build()
                .unwrap()
        };

        let handle = tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });

        upstream_tx.send(mk_event("a", state_a)).await.unwrap();
        upstream_tx.send(mk_event("b", state_b)).await.unwrap();
        upstream_tx.send(mk_event("c", state_c)).await.unwrap();

        let flushed = downstream_rx.recv().await.expect("flush event");
        let proxy_tx = flushed.completion_tx.expect("proxy completion attached");

        proxy_tx.signal_completion(Some(json!({"ok": true})));

        for (label, rx) in [("a", &mut rx_a), ("b", &mut rx_b), ("c", &mut rx_c)] {
            let result = match tokio::time::timeout(Duration::from_secs(1), rx).await {
                Ok(Ok(Ok(payload))) => payload,
                other => panic!("upstream {label} did not receive payload: {other:?}"),
            };
            assert_eq!(
                result,
                Some(json!({"ok": true})),
                "upstream {label} payload mismatch"
            );
        }

        drop(upstream_tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_leaf_buffer_signals_every_concurrent_source() {
        use crate::event::EventData;
        use serde_json::json;

        let config = Arc::new(Buffer {
            name: "leaf".to_string(),
            size: 2,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        });

        let (upstream_tx, upstream_rx) = mpsc::channel(10);

        let processor = ProcessorBuilder::new()
            .config(config)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        let (state_a, mut rx_a) = new_completion_channel(1);
        let (state_b, mut rx_b) = new_completion_channel(1);

        let evt_a = EventBuilder::new()
            .data(EventData::Json(json!({"src": "a"})))
            .subject("a".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(state_a)
            .build()
            .unwrap();
        let evt_b = EventBuilder::new()
            .data(EventData::Json(json!({"src": "b"})))
            .subject("b".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(state_b)
            .build()
            .unwrap();

        let handle = tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });

        upstream_tx.send(evt_a).await.unwrap();
        upstream_tx.send(evt_b).await.unwrap();

        for (label, rx) in [("a", &mut rx_a), ("b", &mut rx_b)] {
            match tokio::time::timeout(Duration::from_secs(1), rx).await {
                Ok(Ok(Ok(_))) => {}
                other => panic!("leaf upstream {label} did not complete: {other:?}"),
            }
        }

        drop(upstream_tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_proxy_one_downstream_signal_acks_every_upstream() {
        use crate::event::EventData;
        use serde_json::json;

        let config = Arc::new(Buffer {
            name: "test".to_string(),
            size: 2,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        });

        let (downstream_tx, mut downstream_rx) = mpsc::channel(10);
        let (upstream_tx, upstream_rx) = mpsc::channel(10);

        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        let (state_a, mut rx_a) = new_completion_channel(1);
        let (state_b, mut rx_b) = new_completion_channel(1);

        let evt_a = EventBuilder::new()
            .data(EventData::Json(json!({"src": "a"})))
            .subject("a".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(state_a)
            .build()
            .unwrap();
        let evt_b = EventBuilder::new()
            .data(EventData::Json(json!({"src": "b"})))
            .subject("b".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(state_b)
            .build()
            .unwrap();

        let handle = tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });

        upstream_tx.send(evt_a).await.unwrap();
        upstream_tx.send(evt_b).await.unwrap();

        let flushed = downstream_rx.recv().await.expect("flush event");
        let proxy = flushed.completion_tx.expect("proxy attached");

        proxy.signal_completion(Some(json!({"ack": true})));

        for (label, rx) in [("a", &mut rx_a), ("b", &mut rx_b)] {
            let payload = match tokio::time::timeout(Duration::from_secs(1), rx).await {
                Ok(Ok(Ok(p))) => p,
                other => panic!("upstream {label} did not receive ack: {other:?}"),
            };
            assert_eq!(payload, Some(json!({"ack": true})));
        }

        drop(upstream_tx);
        let _ = handle.await;
    }

    fn buffer_config(
        size: usize,
        partition_key: Option<&str>,
        flush_on_completion: bool,
    ) -> Buffer {
        Buffer {
            name: "test".to_string(),
            size,
            timeout: Some(Duration::from_secs(30)),
            partition_key: partition_key.map(String::from),
            flush_on_completion,
            depends_on: None,
            retry: None,
        }
    }

    async fn spawn_buffer(config: Buffer) -> (mpsc::Sender<Event>, mpsc::Receiver<Event>) {
        let (downstream_tx, downstream_rx) = mpsc::channel(100);
        let (upstream_tx, upstream_rx) = mpsc::channel(100);
        let processor = ProcessorBuilder::new()
            .config(Arc::new(config))
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();
        tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });
        (upstream_tx, downstream_rx)
    }

    fn element(data: Value, array_id: &str, count: usize) -> Event {
        use super::super::iterate::{ITERATE_COUNT_META, ITERATE_ID_META};
        let (completion, _) = new_completion_channel(1);
        let mut event = EventBuilder::new()
            .data(EventData::Json(data))
            .subject("element".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(completion)
            .build()
            .unwrap();
        let meta = event.meta.get_or_insert_with(Default::default);
        meta.insert("correlation_id".to_string(), Value::from("c1"));
        meta.insert(ITERATE_ID_META.to_string(), Value::from(array_id));
        meta.insert(ITERATE_COUNT_META.to_string(), Value::from(count));
        event
    }

    async fn next_flush(downstream_rx: &mut mpsc::Receiver<Event>) -> Event {
        tokio::time::timeout(Duration::from_secs(1), downstream_rx.recv())
            .await
            .expect("no flush within a second")
            .expect("downstream channel closed")
    }

    fn has_iterate_meta(event: &Event) -> bool {
        use super::super::iterate::{ITERATE_COUNT_META, ITERATE_ID_META};
        match &event.meta {
            Some(meta) => {
                meta.contains_key(ITERATE_COUNT_META) || meta.contains_key(ITERATE_ID_META)
            }
            None => false,
        }
    }

    async fn an_iterate_completes_once_every_element_arrived(partition_key: Option<&str>) {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) =
            spawn_buffer(buffer_config(100, partition_key, true)).await;

        upstream_tx.send(element(json!(0), "a1", 3)).await.unwrap();
        upstream_tx.send(element(json!(1), "a1", 3)).await.unwrap();
        let early = tokio::time::timeout(Duration::from_millis(200), downstream_rx.recv()).await;
        upstream_tx.send(element(json!(2), "a1", 3)).await.unwrap();
        let flushed = next_flush(&mut downstream_rx).await;

        assert!(early.is_err());
        let data = flushed.data_as_json().unwrap();
        assert_eq!(data["batch_size"], 3);
        assert_eq!(data["flush_reason"], "completion");
        assert!(!has_iterate_meta(&flushed));
    }

    #[tokio::test]
    async fn an_iterate_completes_once_every_element_arrived_without_partitions() {
        an_iterate_completes_once_every_element_arrived(None).await;
    }

    #[tokio::test]
    async fn an_iterate_completes_once_every_element_arrived_per_partition() {
        an_iterate_completes_once_every_element_arrived(Some("{{event.meta.correlation_id}}"))
            .await;
    }

    async fn an_array_longer_than_size_flushes_its_tail_on_completion(partition_key: Option<&str>) {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) =
            spawn_buffer(buffer_config(2, partition_key, true)).await;

        for i in 0..5 {
            upstream_tx.send(element(json!(i), "a1", 5)).await.unwrap();
        }
        let mut flushes = Vec::new();
        for _ in 0..3 {
            let data = next_flush(&mut downstream_rx).await.data_as_json().unwrap();
            flushes.push((data["batch_size"].clone(), data["flush_reason"].clone()));
        }

        assert_eq!(
            flushes,
            vec![
                (json!(2), json!("size")),
                (json!(2), json!("size")),
                (json!(1), json!("completion")),
            ]
        );
    }

    #[tokio::test]
    async fn an_array_longer_than_size_flushes_its_tail_on_completion_without_partitions() {
        an_array_longer_than_size_flushes_its_tail_on_completion(None).await;
    }

    #[tokio::test]
    async fn an_array_longer_than_size_flushes_its_tail_on_completion_per_partition() {
        an_array_longer_than_size_flushes_its_tail_on_completion(Some(
            "{{event.meta.correlation_id}}",
        ))
        .await;
    }

    #[tokio::test]
    async fn arrays_arriving_interleaved_under_one_correlation_id_are_batched_apart() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) = spawn_buffer(buffer_config(100, None, true)).await;

        upstream_tx
            .send(element(json!("x1"), "x", 2))
            .await
            .unwrap();
        upstream_tx
            .send(element(json!("y1"), "y", 2))
            .await
            .unwrap();
        upstream_tx
            .send(element(json!("x2"), "x", 2))
            .await
            .unwrap();
        upstream_tx
            .send(element(json!("y2"), "y", 2))
            .await
            .unwrap();
        let first = next_flush(&mut downstream_rx).await.data_as_json().unwrap();
        let second = next_flush(&mut downstream_rx).await.data_as_json().unwrap();

        assert_eq!(first["batch"], json!(["x1", "x2"]));
        assert_eq!(first["flush_reason"], "completion");
        assert_eq!(second["batch"], json!(["y1", "y2"]));
        assert_eq!(second["flush_reason"], "completion");
    }

    #[tokio::test]
    async fn nested_iterates_give_each_inner_array_its_own_batch() {
        use crate::task::iterate::{Iterate, ProcessorBuilder as IterateBuilder};
        use crate::task::runner::Runner;
        use serde_json::json;
        let (source_tx, outer_rx) = mpsc::channel(10);
        let (outer_tx, inner_rx) = mpsc::channel(10);
        let (inner_tx, buffer_rx) = mpsc::channel(10);
        let (buffer_tx, mut downstream_rx) = mpsc::channel(10);
        for (name, rx, tx) in [("outer", outer_rx, outer_tx), ("inner", inner_rx, inner_tx)] {
            let iterate = IterateBuilder::new()
                .config(Arc::new(Iterate {
                    name: name.to_string(),
                    ..Default::default()
                }))
                .receiver(rx)
                .sender(tx)
                .task_type("iterate")
                .task_context(create_mock_task_context())
                .build()
                .await
                .unwrap();
            tokio::spawn(async move {
                let _ = iterate.run().await;
            });
        }
        let buffer = ProcessorBuilder::new()
            .config(Arc::new(buffer_config(100, None, true)))
            .receiver(buffer_rx)
            .sender(buffer_tx)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = buffer.run().await;
        });

        let source = EventBuilder::new()
            .data(EventData::Json(json!([["a", "b", "c"], ["d"]])))
            .subject("source".into())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();
        source_tx.send(source).await.unwrap();
        let mut batches = Vec::new();
        for _ in 0..2 {
            let flushed = next_flush(&mut downstream_rx).await;
            assert!(!has_iterate_meta(&flushed));
            let data = flushed.data_as_json().unwrap();
            assert_eq!(data["flush_reason"], "completion");
            batches.push(data["batch"].clone());
        }
        batches.sort_by_key(|batch| batch.to_string());

        assert_eq!(batches, vec![json!(["a", "b", "c"]), json!(["d"])]);
    }

    #[tokio::test]
    async fn an_array_split_across_partitions_flushes_every_partition_on_completion() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) =
            spawn_buffer(buffer_config(100, Some("{{event.data.k}}"), true)).await;

        for k in ["p", "q", "p"] {
            upstream_tx
                .send(element(json!({ "k": k }), "a1", 3))
                .await
                .unwrap();
        }
        let mut flushes = Vec::new();
        for _ in 0..2 {
            let data = next_flush(&mut downstream_rx).await.data_as_json().unwrap();
            flushes.push((
                data["partition_key"].clone(),
                data["batch_size"].clone(),
                data["flush_reason"].clone(),
            ));
        }
        flushes.sort_by_key(|flush| flush.0.to_string());

        assert_eq!(
            flushes,
            vec![
                (json!("p"), json!(2), json!("completion")),
                (json!("q"), json!(1), json!("completion")),
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_element_is_passed_on_and_still_counts_toward_its_array() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) = spawn_buffer(buffer_config(100, None, true)).await;

        upstream_tx.send(element(json!(0), "a1", 3)).await.unwrap();
        upstream_tx.send(element(json!(1), "a1", 3)).await.unwrap();
        let mut failed = element(json!(2), "a1", 3);
        failed.error = Some("upstream failed".to_string());
        upstream_tx.send(failed).await.unwrap();
        let passed_on = next_flush(&mut downstream_rx).await;
        let batch = next_flush(&mut downstream_rx).await;

        assert_eq!(passed_on.error.as_deref(), Some("upstream failed"));
        assert!(!has_iterate_meta(&passed_on));
        let data = batch.data_as_json().unwrap();
        assert_eq!(data["batch"], json!([0, 1]));
        assert_eq!(data["flush_reason"], "completion");
    }

    #[tokio::test]
    async fn iterate_meta_never_reaches_the_batch_meta_without_flush_on_completion() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) = spawn_buffer(buffer_config(2, None, false)).await;

        upstream_tx.send(element(json!(0), "a1", 2)).await.unwrap();
        upstream_tx.send(element(json!(1), "a1", 2)).await.unwrap();
        let flushed = next_flush(&mut downstream_rx).await;

        assert_eq!(flushed.data_as_json().unwrap()["flush_reason"], "size");
        assert!(!has_iterate_meta(&flushed));
    }

    #[tokio::test]
    async fn the_first_event_of_a_partition_flushes_when_it_fills_the_batch() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) =
            spawn_buffer(buffer_config(1, Some("{{event.data.k}}"), false)).await;
        let event = EventBuilder::new()
            .data(EventData::Json(json!({ "k": "p" })))
            .subject("a".into())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();

        upstream_tx.send(event).await.unwrap();
        let data = next_flush(&mut downstream_rx).await.data_as_json().unwrap();

        assert_eq!(data["batch_size"], 1);
        assert_eq!(data["flush_reason"], "size");
        assert_eq!(data["partition_key"], "p");
    }

    #[tokio::test]
    async fn flush_on_completion_waits_for_last_event_in_batch() {
        use crate::event::EventData;
        use serde_json::json;

        let config = Arc::new(Buffer {
            name: "test".to_string(),
            size: 10000,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: true,
            depends_on: None,
            retry: None,
        });

        let (downstream_tx, mut downstream_rx) = mpsc::channel(10);
        let (upstream_tx, upstream_rx) = mpsc::channel(10);

        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        let handle = tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });

        for i in 0..5 {
            let evt = EventBuilder::new()
                .data(EventData::Json(json!({ "i": i })))
                .subject("layer".into())
                .task_id(0)
                .task_type("test")
                .build()
                .unwrap();
            upstream_tx.send(evt).await.unwrap();
        }

        assert!(
            tokio::time::timeout(Duration::from_millis(200), downstream_rx.recv())
                .await
                .is_err(),
            "buffer flushed before the completion-bearing event arrived",
        );

        let (state, _rx) = new_completion_channel(1);
        let last = EventBuilder::new()
            .data(EventData::Json(json!({ "i": 5 })))
            .subject("layer".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(state)
            .build()
            .unwrap();
        upstream_tx.send(last).await.unwrap();

        let flushed = tokio::time::timeout(Duration::from_secs(1), downstream_rx.recv())
            .await
            .expect("buffer must flush after final event")
            .expect("downstream channel closed");

        let data = flushed.data_as_json().unwrap();
        let batch = data["batch"].as_array().unwrap();
        assert_eq!(
            batch.len(),
            6,
            "completion flush must include every event the source emitted, got {batch:?}"
        );
        for (i, item) in batch.iter().enumerate() {
            assert_eq!(
                item["i"].as_u64().unwrap(),
                i as u64,
                "batch order must match upstream emit order"
            );
        }

        drop(upstream_tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn flush_on_completion_off_does_not_flush_on_completion() {
        use crate::event::EventData;
        use serde_json::json;

        let config = Arc::new(Buffer {
            name: "test".to_string(),
            size: 10000,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        });

        let (downstream_tx, mut downstream_rx) = mpsc::channel(10);
        let (upstream_tx, upstream_rx) = mpsc::channel(10);

        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        let handle = tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });

        let (state, _rx) = new_completion_channel(1);
        let evt = EventBuilder::new()
            .data(EventData::Json(json!({ "i": 0 })))
            .subject("layer".into())
            .task_id(0)
            .task_type("test")
            .completion_tx(state)
            .build()
            .unwrap();
        upstream_tx.send(evt).await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_millis(200), downstream_rx.recv())
                .await
                .is_err(),
            "flush_on_completion is off — completion_tx must not trigger a flush",
        );

        drop(upstream_tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn an_event_whose_upstream_failed_is_passed_on_and_not_batched() {
        use serde_json::json;

        let config = Arc::new(Buffer {
            name: "test".to_string(),
            size: 10,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        });
        let (downstream_tx, mut downstream_rx) = mpsc::channel(10);
        let (upstream_tx, upstream_rx) = mpsc::channel(10);
        let processor = ProcessorBuilder::new()
            .config(config)
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();
        let handle = tokio::spawn(async move {
            use crate::task::runner::Runner;
            let _ = processor.run().await;
        });

        let event = |i: i64| {
            EventBuilder::new()
                .data(EventData::Json(json!({ "i": i })))
                .subject("a".into())
                .task_id(0)
                .task_type("test")
                .build()
                .unwrap()
        };
        let mut failed = event(0);
        failed.error = Some("upstream failed".to_string());
        upstream_tx.send(failed).await.unwrap();
        upstream_tx.send(event(1)).await.unwrap();
        drop(upstream_tx);
        let _ = handle.await;

        let passed_on = downstream_rx.recv().await.unwrap();
        let batch = downstream_rx.recv().await.unwrap();
        assert_eq!(passed_on.error.as_deref(), Some("upstream failed"));
        assert_eq!(batch.error, None);
        assert_eq!(batch.data_as_json().unwrap()["batch_size"], json!(1));
    }

    fn event_with(data: EventData) -> Event {
        EventBuilder::new()
            .data(data)
            .subject("a".into())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn an_event_without_a_partition_key_fails_alone_and_the_buffer_keeps_running() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) =
            spawn_buffer(buffer_config(1, Some("{{event.data.k}}"), false)).await;

        upstream_tx
            .send(event_with(EventData::Json(json!({ "k": null }))))
            .await
            .unwrap();
        upstream_tx
            .send(event_with(EventData::Json(json!({ "k": "p" }))))
            .await
            .unwrap();
        let failed = next_flush(&mut downstream_rx).await;
        let batch = next_flush(&mut downstream_rx).await;

        assert_eq!(failed.error, Some(Error::MissingPartitionKey.to_string()));
        assert_eq!(failed.data_as_json().unwrap(), json!({ "k": null }));
        assert_eq!(batch.error, None);
        assert_eq!(batch.data_as_json().unwrap()["partition_key"], "p");
    }

    #[tokio::test]
    async fn partition_keys_keep_their_digits_as_written() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) = spawn_buffer(buffer_config(
            1,
            Some("{{event.data.a}}.{{event.data.b}}"),
            false,
        ))
        .await;

        for data in [json!({ "a": 1, "b": 20 }), json!({ "a": 1, "b": 2 })] {
            upstream_tx
                .send(event_with(EventData::Json(data)))
                .await
                .unwrap();
        }
        let first = next_flush(&mut downstream_rx).await;
        let second = next_flush(&mut downstream_rx).await;

        assert_eq!(first.data_as_json().unwrap()["partition_key"], "1.20");
        assert_eq!(second.data_as_json().unwrap()["partition_key"], "1.2");
    }

    #[tokio::test]
    async fn a_number_is_a_partition_key_and_a_missing_field_is_none() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) =
            spawn_buffer(buffer_config(1, Some("{{event.data.k}}"), false)).await;

        upstream_tx
            .send(event_with(EventData::Json(json!({ "k": 7 }))))
            .await
            .unwrap();
        upstream_tx
            .send(event_with(EventData::Json(json!({ "other": 1 }))))
            .await
            .unwrap();
        let batch = next_flush(&mut downstream_rx).await;
        let failed = next_flush(&mut downstream_rx).await;

        assert_eq!(batch.data_as_json().unwrap()["partition_key"], "7");
        assert_eq!(failed.error, Some(Error::MissingPartitionKey.to_string()));
    }

    #[tokio::test]
    async fn an_event_that_is_not_json_fails_alone_and_keeps_its_data() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) = spawn_buffer(buffer_config(1, None, false)).await;

        let raw = event_with(EventData::Bytes(bytes::Bytes::from_static(b"raw")));
        upstream_tx.send(raw).await.unwrap();
        upstream_tx
            .send(event_with(EventData::Json(json!({ "i": 1 }))))
            .await
            .unwrap();
        let failed = next_flush(&mut downstream_rx).await;
        let batch = next_flush(&mut downstream_rx).await;

        assert_eq!(failed.error, Some(Error::ExpectedJsonGotBytes.to_string()));
        assert!(matches!(&failed.data, EventData::Bytes(raw) if raw.as_ref() == b"raw"));
        assert_eq!(batch.data_as_json().unwrap()["batch"], json!([{ "i": 1 }]));
    }

    #[tokio::test]
    async fn an_element_that_cannot_join_a_batch_still_counts_toward_its_array() {
        use serde_json::json;
        let (upstream_tx, mut downstream_rx) =
            spawn_buffer(buffer_config(100, Some("{{event.data.k}}"), true)).await;

        upstream_tx
            .send(element(json!({ "k": "p" }), "a1", 2))
            .await
            .unwrap();
        upstream_tx
            .send(element(json!({ "k": null }), "a1", 2))
            .await
            .unwrap();
        let failed = next_flush(&mut downstream_rx).await;
        let batch = next_flush(&mut downstream_rx).await;

        assert_eq!(failed.error, Some(Error::MissingPartitionKey.to_string()));
        assert!(!has_iterate_meta(&failed));
        let data = batch.data_as_json().unwrap();
        assert_eq!(data["batch_size"], 1);
        assert_eq!(data["flush_reason"], "completion");
    }

    #[tokio::test]
    async fn an_event_that_fails_at_a_leaf_buffer_reports_the_error_to_the_source() {
        use crate::task::runner::Runner;
        use serde_json::json;
        let (upstream_tx, upstream_rx) = mpsc::channel(10);
        let processor = ProcessorBuilder::new()
            .config(Arc::new(buffer_config(1, Some("{{event.data.k}}"), false)))
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();
        let running = tokio::spawn(processor.run());
        let (completion, completion_rx) = new_completion_channel(1);
        let mut event = event_with(EventData::Json(json!({ "k": null })));
        event.completion_tx = Some(completion);

        upstream_tx.send(event).await.unwrap();
        let reported = tokio::time::timeout(Duration::from_secs(1), completion_rx)
            .await
            .expect("the source hears back")
            .unwrap();
        drop(upstream_tx);
        let stopped = running.await.unwrap();

        assert_eq!(
            reported.map_err(|error| error.to_string()),
            Err(Error::MissingPartitionKey.to_string())
        );
        assert!(stopped.is_ok());
    }

    #[tokio::test]
    async fn a_flush_the_next_task_no_longer_takes_stops_the_buffer_with_an_error() {
        use crate::task::runner::Runner;
        use serde_json::json;
        let (downstream_tx, downstream_rx) = mpsc::channel(1);
        drop(downstream_rx);
        let (upstream_tx, upstream_rx) = mpsc::channel(10);
        let processor = ProcessorBuilder::new()
            .config(Arc::new(buffer_config(1, None, false)))
            .sender(downstream_tx)
            .receiver(upstream_rx)
            .task_id(1)
            .task_type("buffer")
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();
        let running = tokio::spawn(processor.run());

        upstream_tx
            .send(event_with(EventData::Json(json!({ "i": 1 }))))
            .await
            .unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(1), running)
            .await
            .expect("the buffer stops")
            .unwrap();

        assert!(matches!(stopped, Err(Error::SendMessage { .. })));
    }

    #[test]
    fn test_config_serialization() {
        let config = Buffer {
            name: "test_buffer".to_string(),
            size: 100,
            timeout: Some(Duration::from_secs(30)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: Buffer = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_default_timeout() {
        assert_eq!(default_timeout(), Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_config_with_custom_timeout() {
        let config = Buffer {
            name: "custom_buffer".to_string(),
            size: 75,
            timeout: Some(Duration::from_secs(10)),
            partition_key: None,
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        };

        assert_eq!(config.timeout, Some(Duration::from_secs(10)));
        assert_eq!(config.size, 75);
    }

    #[test]
    fn test_config_with_partition_key() {
        let config = Buffer {
            name: "partitioned_buffer".to_string(),
            size: 75,
            timeout: Some(Duration::from_secs(30)),
            partition_key: Some("{{event.data.program_id}}.{{event.data.country}}".to_string()),
            flush_on_completion: false,
            depends_on: None,
            retry: None,
        };

        assert_eq!(
            config.partition_key,
            Some("{{event.data.program_id}}.{{event.data.country}}".to_string())
        );
        assert_eq!(config.size, 75);
    }
}
