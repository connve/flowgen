//! Buffer processor for accumulating events into batches.
//!
//! Collects individual events and emits them as batches based on configurable size
//! and timeout triggers. This enables efficient batch processing for downstream tasks
//! such as file writes, API calls, or columnar format conversions.

use crate::config::ConfigExt;
use crate::event::{Event, EventBuilder, EventData, SenderExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Instant};
use tracing::error;

/// Errors that can occur during buffer processing operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Sending event to channel failed: {source}")]
    SendMessage {
        #[source]
        source: crate::event::Error,
    },
    #[error("Processor event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("Expected JSON event data, got ArrowRecordBatch")]
    ExpectedJsonGotArrowRecordBatch,
    #[error("Expected JSON event data, got Avro")]
    ExpectedJsonGotAvro,
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Failed to render buffer key template: {source}")]
    Render {
        #[source]
        source: crate::config::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
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
    _task_context: Arc<crate::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl Processor {
    /// Flushes the buffer by emitting a single event containing all buffered events.
    ///
    /// # Arguments
    /// * `buffer` - Vector of accumulated events to flush.
    /// * `reason` - The reason this flush was triggered.
    /// * `partition_key` - Optional key that was used to group these events.
    ///
    /// # Returns
    /// Result containing () on success or Error on failure.
    async fn flush_buffer(
        &self,
        buffer: Vec<Value>,
        reason: FlushReason,
        partition_key: Option<String>,
    ) -> Result<(), Error> {
        if buffer.is_empty() {
            return Ok(());
        }

        let batch_size = buffer.len();
        let flush_data = FlushData {
            batch: buffer,
            batch_size,
            flush_reason: reason,
            partition_key,
        };

        let flush_result = serde_json::to_value(flush_data).map_err(|e| Error::EventBuilder {
            source: crate::event::Error::SerdeJson { source: e },
        })?;

        let event = EventBuilder::new()
            .data(EventData::Json(flush_result))
            .subject(self.config.name.clone())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        if let Some(ref tx) = self.tx {
            tx.send_with_logging(event)
                .await
                .map_err(|source| Error::SendMessage { source })?;
        }

        Ok(())
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

        // If partition_key is configured, use HashMap for keyed buffers
        if self.config.partition_key.is_some() {
            self.process_events_keyed(timeout_duration).await
        } else {
            self.process_events_single(timeout_duration).await
        }
    }

    /// Processes events with a single buffer (no keying).
    async fn process_events_single(&mut self, timeout_duration: Duration) -> Result<(), Error> {
        let mut buffer: Vec<Value> = Vec::with_capacity(self.config.size);
        let mut last_flush = Instant::now();

        loop {
            // Calculate remaining time until next timeout flush.
            let time_until_flush = timeout_duration
                .checked_sub(last_flush.elapsed())
                .unwrap_or(Duration::ZERO);

            tokio::select! {
                // Receive and buffer incoming events.
                result = self.rx.recv() => {
                    match result {
                        Some(event) => {
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

                            // Flush if buffer reached size limit.
                            if buffer.len() >= self.config.size {
                                self.flush_buffer(buffer.clone(), FlushReason::Size, None).await?;
                                buffer.clear();
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            // Channel closed, flush remaining events and exit.
                            if !buffer.is_empty() {
                                self.flush_buffer(buffer, FlushReason::Shutdown, None).await?;
                            }
                            return Ok(());
                        }
                    }
                }

                // Timeout trigger to flush partial batches.
                _ = sleep(time_until_flush), if !buffer.is_empty() => {
                    self.flush_buffer(buffer.clone(), FlushReason::Timeout, None).await?;
                    buffer.clear();
                    last_flush = Instant::now();
                }
            }
        }
    }

    /// Processes events with keyed buffers (separate buffer per key).
    async fn process_events_keyed(&mut self, timeout_duration: Duration) -> Result<(), Error> {
        let mut buffers: HashMap<String, Vec<Value>> = HashMap::new();
        let mut last_flush_times: HashMap<String, Instant> = HashMap::new();

        loop {
            // Find the earliest timeout across all active keyed buffers.
            let min_time_until_flush = buffers
                .keys()
                .filter_map(|key| {
                    last_flush_times
                        .get(key)
                        .and_then(|last_flush| timeout_duration.checked_sub(last_flush.elapsed()))
                })
                .min()
                .unwrap_or(timeout_duration);

            tokio::select! {
                // Receive and buffer incoming events.
                result = self.rx.recv() => {
                    match result {
                        Some(event) => {
                            // Extract JSON data from event.
                            let json_data = match &event.data {
                                EventData::Json(data) => data.clone(),
                                EventData::ArrowRecordBatch(_) => {
                                    return Err(Error::ExpectedJsonGotArrowRecordBatch);
                                }
                                EventData::Avro(_) => {
                                    return Err(Error::ExpectedJsonGotAvro);
                                }
                            };

                            // Render the config with event data to get the rendered buffer key.
                            let event_value = serde_json::value::Value::try_from(&event)
                                .map_err(|source| Error::EventBuilder { source })?;
                            let rendered_config = self.config.render(&event_value)
                                .map_err(|source| Error::Render { source })?;

                            // Extract the rendered key from the config.
                            let rendered_key = rendered_config.partition_key
                                .ok_or_else(|| Error::MissingRequiredAttribute("partition_key".to_string()))?;

                            // Get or create buffer for this key.
                            let is_new_buffer = !buffers.contains_key(&rendered_key);
                            let buffer = buffers.entry(rendered_key.clone()).or_default();
                            buffer.push(json_data);

                            // Initialize last flush time for new buffers.
                            if is_new_buffer {
                                last_flush_times.insert(rendered_key.clone(), Instant::now());
                            }

                            // Flush if this key's buffer reached size limit.
                            if buffer.len() >= self.config.size {
                                if let Some(buffer_to_flush) = buffers.remove(&rendered_key) {
                                    self.flush_buffer(buffer_to_flush, FlushReason::Size, Some(rendered_key.clone())).await?;
                                }
                                last_flush_times.insert(rendered_key, Instant::now());
                            }
                        }
                        None => {
                            // Channel closed, flush all remaining buffers and exit.
                            for (key, buffer) in buffers {
                                if !buffer.is_empty() {
                                    self.flush_buffer(buffer, FlushReason::Shutdown, Some(key)).await?;
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
                    for key in buffers.keys() {
                        if let Some(last_flush) = last_flush_times.get(key) {
                            if now.duration_since(*last_flush) >= timeout_duration {
                                keys_to_flush.push(key.clone());
                            }
                        }
                    }

                    // Flush timed-out buffers.
                    for key in keys_to_flush {
                        if let Some(buffer) = buffers.remove(&key) {
                            if !buffer.is_empty() {
                                self.flush_buffer(buffer, FlushReason::Timeout, Some(key.clone())).await?;
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

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        // Initialize (no-op for buffer processor).
        match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!(
                    "{}",
                    Error::RetryExhausted {
                        source: Box::new(e)
                    }
                );
                return Ok(());
            }
        };

        // Run the main event processing loop with buffer accumulation.
        if let Err(e) = self.process_events().await {
            error!("{}", e);
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
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self.tx,
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
    use serde_json::Map;
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Buffer Test".to_string()),
        );
        let task_manager = Arc::new(crate::task::manager::TaskManagerBuilder::new().build());
        Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
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
            Error::MissingRequiredAttribute(_)
        ));
    }
}
