//! Buffer processor for accumulating events into batches.
//!
//! Collects individual events and emits them as batches based on configurable size
//! and timeout triggers. This enables efficient batch processing for downstream tasks
//! such as file writes, API calls, or columnar format conversions.

use crate::config::ConfigExt;
use crate::event::{
    new_completion_channel, Event, EventBuilder, EventData, EventExt, SharedCompletionTx,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Instant};
use tracing::{error, Instrument};

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
    /// Timeout elapsed since last flush.
    Timeout,
    /// Shutdown signal received.
    Shutdown,
    /// An incoming event carried a completion signal and the buffer
    /// was configured with `flush_on_completion`.
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
    config: Arc<super::config::Processor>,
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
    /// This function spawns a background task to perform the flush asynchronously,
    /// allowing the buffer to continue receiving events without blocking.
    ///
    /// # Arguments
    /// * `buffer` - Vector of accumulated events to flush.
    /// * `reason` - The reason this flush was triggered.
    /// * `partition_key` - Optional key that was used to group these events.
    /// * `meta` - Optional meta from the first event in the buffer to preserve.
    /// * `upstream_completions` - Deduped completion channels from every distinct
    ///   source event absorbed into this batch. The buffer attaches a single
    ///   completion to the flushed event downstream; once that completion fires,
    ///   it is fanned back out to every absorbed upstream so every source gets
    ///   acked. When the buffer is itself a leaf, all upstreams are signalled
    ///   directly.
    fn flush_buffer(
        &self,
        buffer: Vec<Value>,
        reason: FlushReason,
        partition_key: Option<String>,
        meta: Option<Map<String, Value>>,
        upstream_completions: Vec<SharedCompletionTx>,
    ) {
        if buffer.is_empty() {
            return;
        }

        let tx = self.tx.clone();
        let config_name = self.config.name.clone();
        let task_id = self.task_id;
        let task_type = self.task_type;
        let leaf_count = self.task_context.leaf_count;

        // Spawn flush as background task to avoid blocking event receives.
        tokio::spawn(
            async move {
                let batch_size = buffer.len();
                let flush_data = FlushData {
                    batch: buffer,
                    batch_size,
                    flush_reason: reason,
                    partition_key,
                };

                let flush_result = match serde_json::to_value(flush_data) {
                    Ok(v) => v,
                    Err(e) => {
                        error!(error = %e, "Failed to serialize flush data");
                        return;
                    }
                };

                let mut event_builder = EventBuilder::new()
                    .data(EventData::Json(flush_result))
                    .subject(config_name)
                    .task_id(task_id)
                    .task_type(task_type);

                // Preserve meta if it exists.
                if let Some(meta) = meta {
                    event_builder = event_builder.meta(meta);
                }

                let mut event = match event_builder.build() {
                    Ok(e) => e,
                    Err(e) => {
                        error!(error = %e, "Failed to build flush event");
                        return;
                    }
                };

                match tx {
                    Some(_) => {
                        event.completion_tx =
                            spawn_completion_proxy(upstream_completions, leaf_count);
                    }
                    None => {
                        // Leaf task: signal every absorbed upstream directly.
                        let payload = event.data_as_json().ok();
                        for arc in &upstream_completions {
                            arc.signal_completion(payload.clone());
                        }
                    }
                }

                if let Err(e) = event.send_with_logging(tx.as_ref()).await {
                    error!(error = %e, "Failed to send flush event");
                }
            }
            .instrument(tracing::Span::current()),
        );
    }

    /// Processes the main event loop with buffer accumulation and flush triggers.
    ///
    /// Uses `tokio::select!` to handle three concurrent conditions:
    /// 1. Receiving new events and accumulating them in the buffer(s)
    /// 2. Timeout expiration to flush partial batches
    /// 3. Shutdown signal to flush remaining events
    ///
    /// When partition_key is configured, maintains separate buffers per key with independent
    /// size and timeout tracking. Otherwise uses a single buffer.
    async fn process_events(&mut self) -> Result<(), Error> {
        let timeout_duration = self.config.timeout.unwrap_or(Duration::from_secs(30));

        // If partition_key is configured, use HashMap for keyed buffers.
        if self.config.partition_key.is_some() {
            self.process_events_keyed(timeout_duration).await
        } else {
            self.process_events_single(timeout_duration).await
        }
    }

    /// Processes events with a single buffer (no keying).
    async fn process_events_single(&mut self, timeout_duration: Duration) -> Result<(), Error> {
        let mut buffer: Vec<Value> = Vec::with_capacity(self.config.size);
        let mut buffer_meta: Option<Map<String, Value>> = None;
        // Distinct upstream completion handles absorbed by the current batch.
        // Each source event carries its own `CompletionState`; concurrent
        // sources (webhooks, NATS, MCP) produce distinct Arcs. The buffer must
        // ack every one of them when the flush completes — keeping only the
        // last would silently drop N-1 source acks per flush.
        let mut buffer_completions: Vec<SharedCompletionTx> = Vec::new();
        let mut last_flush = Instant::now();

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                return Ok(());
            }

            // Calculate remaining time until next timeout flush.
            let time_until_flush = timeout_duration
                .checked_sub(last_flush.elapsed())
                .unwrap_or(Duration::ZERO);

            tokio::select! {
                // Receive and buffer incoming events.
                result = self.rx.recv() => {
                    match result {
                        Some(event) => {
                            // Capture meta from first event in buffer.
                            if buffer.is_empty() {
                                buffer_meta = event.meta.clone();
                            }

                            // Extract JSON data from event.
                            let json_data = match event.data {
                                EventData::Json(data) => data,
                                EventData::ArrowRecordBatch(_) => {
                                    return Err(Error::ExpectedJsonGotArrowRecordBatch);
                                }
                                EventData::Avro(_) => {
                                    return Err(Error::ExpectedJsonGotAvro);
                                }
                            };

                            buffer.push(json_data);

                            // Sources that emit a known-finite batch attach
                            // a completion handle to the final event in the
                            // batch. When `flush_on_completion` is set, that
                            // is also the cue to flush the buffer right
                            // away instead of waiting for `size`/`timeout`.
                            let trigger_completion_flush =
                                self.config.flush_on_completion && event.completion_tx.is_some();

                            if let Some(tx) = event.completion_tx {
                                absorb_completion(&mut buffer_completions, tx);
                            }

                            // Flush if buffer reached size limit OR the
                            // upstream source signalled completion.
                            let reached_size = buffer.len() >= self.config.size;
                            if reached_size || trigger_completion_flush {
                                let flush_buffer = std::mem::replace(&mut buffer, Vec::with_capacity(self.config.size));
                                let flush_meta = buffer_meta.take();
                                let flush_completions = std::mem::take(&mut buffer_completions);
                                let reason = if reached_size {
                                    FlushReason::Size
                                } else {
                                    FlushReason::Completion
                                };
                                self.flush_buffer(flush_buffer, reason, None, flush_meta, flush_completions);
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            // Channel closed, flush remaining events and exit.
                            if !buffer.is_empty() {
                                self.flush_buffer(buffer, FlushReason::Shutdown, None, buffer_meta, buffer_completions);
                            }
                            return Ok(());
                        }
                    }
                }

                // Timeout trigger to flush partial batches.
                _ = sleep(time_until_flush), if !buffer.is_empty() => {
                    let flush_buffer = std::mem::replace(&mut buffer, Vec::with_capacity(self.config.size));
                    let flush_meta = buffer_meta.take();
                    let flush_completions = std::mem::take(&mut buffer_completions);
                    self.flush_buffer(flush_buffer, FlushReason::Timeout, None, flush_meta, flush_completions);
                    last_flush = Instant::now();
                }
            }
        }
    }

    /// Processes events with keyed buffers (separate buffer per key).
    async fn process_events_keyed(&mut self, timeout_duration: Duration) -> Result<(), Error> {
        // Pre-allocate capacity for typical partition count (e.g., 16 partitions).
        let mut buffers: HashMap<String, Vec<Value>> = HashMap::with_capacity(16);
        let mut buffer_metas: HashMap<String, Map<String, Value>> = HashMap::with_capacity(16);
        // Per-key list of distinct upstream completions to ack on flush. See
        // `process_events_single` for why we keep all of them rather than
        // the last.
        let mut buffer_completions: HashMap<String, Vec<SharedCompletionTx>> =
            HashMap::with_capacity(16);
        let mut last_flush_times: HashMap<String, Instant> = HashMap::with_capacity(16);

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                return Ok(());
            }

            // Find the earliest timeout across all active keyed buffers.
            // Only calculated once per loop iteration, not on every event.
            let min_time_until_flush = if buffers.is_empty() {
                timeout_duration
            } else {
                last_flush_times
                    .values()
                    .filter_map(|last_flush| timeout_duration.checked_sub(last_flush.elapsed()))
                    .min()
                    .unwrap_or(Duration::ZERO)
            };

            tokio::select! {
                // Receive and buffer incoming events.
                result = self.rx.recv() => {
                    match result {
                        Some(mut event) => {
                            // Render the config with event data to get the rendered buffer key.
                            let event_value = serde_json::value::Value::try_from(&event)
                                .map_err(|source| Error::EventBuilder { source })?;
                            let rendered_config = self.config.render(&event_value)
                                .map_err(|source| Error::Render { source })?;

                            // Extract the rendered key from the config.
                            let rendered_key = rendered_config.partition_key.ok_or(Error::MissingPartitionKey)?;

                            // Extract JSON data from event (after rendering to avoid unnecessary clone).
                            let json_data = match event.data {
                                EventData::Json(data) => data,
                                EventData::ArrowRecordBatch(_) => {
                                    return Err(Error::ExpectedJsonGotArrowRecordBatch);
                                }
                                EventData::Avro(_) => {
                                    return Err(Error::ExpectedJsonGotAvro);
                                }
                            };

                            let completion_tx = event.completion_tx.take();
                            // Sources that emit a known-finite batch attach
                            // a completion handle to the final event in the
                            // batch. When `flush_on_completion` is set, that
                            // is also the cue to flush the buffer right
                            // away instead of waiting for `size`/`timeout`.
                            let trigger_completion_flush =
                                self.config.flush_on_completion && completion_tx.is_some();

                            // Get or create buffer for this key with pre-allocated capacity.
                            use std::collections::hash_map::Entry;
                            let buffer_size = self.config.size;
                            let flushed_key = match buffers.entry(rendered_key.clone()) {
                                Entry::Vacant(vacant) => {
                                    let key = vacant.key().clone();
                                    let mut buffer = Vec::with_capacity(buffer_size);
                                    buffer.push(json_data);
                                    vacant.insert(buffer);
                                    // Capture meta from first event in this keyed buffer.
                                    if let Some(meta) = event.meta {
                                        buffer_metas.insert(key.clone(), meta);
                                    }
                                    if let Some(tx) = completion_tx {
                                        buffer_completions.insert(key.clone(), vec![tx]);
                                    }
                                    last_flush_times.insert(key.clone(), Instant::now());
                                    if trigger_completion_flush {
                                        Some(key)
                                    } else {
                                        None
                                    }
                                }
                                Entry::Occupied(mut occupied) => {
                                    // Get key before any mutable borrows.
                                    let key = occupied.key().clone();

                                    let buffer = occupied.get_mut();
                                    buffer.push(json_data);

                                    if let Some(tx) = completion_tx {
                                        let absorbed = buffer_completions
                                            .entry(key.clone())
                                            .or_default();
                                        absorb_completion(absorbed, tx);
                                    }

                                    // Flush if this key's buffer reached size limit.
                                    if buffer.len() >= buffer_size {
                                        let buffer_to_flush = occupied.remove();
                                        let buffer_meta = buffer_metas.remove(&key);
                                        let buffer_completion = buffer_completions.remove(&key).unwrap_or_default();
                                        self.flush_buffer(buffer_to_flush, FlushReason::Size, Some(key.clone()), buffer_meta, buffer_completion);
                                        last_flush_times.insert(key.clone(), Instant::now());
                                        None
                                    } else if trigger_completion_flush {
                                        Some(key)
                                    } else {
                                        None
                                    }
                                }
                            };

                            if let Some(key) = flushed_key {
                                if let Some(buffer_to_flush) = buffers.remove(&key) {
                                    let buffer_meta = buffer_metas.remove(&key);
                                    let buffer_completion =
                                        buffer_completions.remove(&key).unwrap_or_default();
                                    self.flush_buffer(
                                        buffer_to_flush,
                                        FlushReason::Completion,
                                        Some(key.clone()),
                                        buffer_meta,
                                        buffer_completion,
                                    );
                                    last_flush_times.insert(key, Instant::now());
                                }
                            }
                        }
                        None => {
                            // Channel closed, flush all remaining buffers and exit.
                            for (key, buffer) in buffers {
                                if !buffer.is_empty() {
                                    let buffer_meta = buffer_metas.remove(&key);
                                    let buffer_completion = buffer_completions.remove(&key).unwrap_or_default();
                                    self.flush_buffer(buffer, FlushReason::Shutdown, Some(key), buffer_meta, buffer_completion);
                                }
                            }
                            return Ok(());
                        }
                    }
                }

                // Timeout trigger to flush buffers that have exceeded timeout.
                _ = sleep(min_time_until_flush), if !buffers.is_empty() => {
                    let now = Instant::now();
                    let mut keys_to_flush = Vec::new();

                    // Find all keys with active buffers whose timeout has been exceeded.
                    for (key, last_flush) in &last_flush_times {
                        if now.duration_since(*last_flush) >= timeout_duration && buffers.contains_key(key) {
                            keys_to_flush.push(key.clone());
                        }
                    }

                    // Flush timed-out buffers.
                    for key in keys_to_flush {
                        if let Some(buffer) = buffers.remove(&key) {
                            if !buffer.is_empty() {
                                let buffer_meta = buffer_metas.remove(&key);
                                let buffer_completion = buffer_completions.remove(&key).unwrap_or_default();
                                self.flush_buffer(buffer, FlushReason::Timeout, Some(key.clone()), buffer_meta, buffer_completion);
                            }
                        }
                        last_flush_times.insert(key, now);
                    }
                }
            }
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

        // Run the main event processing loop with buffer accumulation.
        if let Err(e) = self.process_events().await {
            error!(error = %e, "Failed to process events");
        }

        Ok(())
    }
}

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Buffer processor configuration (required for build).
    config: Option<Arc<super::config::Processor>>,
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

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
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
        let config = Arc::new(super::super::config::Processor {
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

        let config = Arc::new(super::super::config::Processor {
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

        let config = Arc::new(super::super::config::Processor {
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

        let config = Arc::new(super::super::config::Processor {
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

        let config = Arc::new(super::super::config::Processor {
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

        let config = Arc::new(super::super::config::Processor {
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

    #[tokio::test]
    async fn flush_on_completion_waits_for_last_event_in_batch() {
        use crate::event::EventData;
        use serde_json::json;

        let config = Arc::new(super::super::config::Processor {
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

        let config = Arc::new(super::super::config::Processor {
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
}
