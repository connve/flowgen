//! Loop processor for iterating over JSON arrays.
//!
//! Processes events containing JSON arrays and emits individual events
//! for each array element, enabling fan-out processing patterns.

use crate::event::{Event, EventBuilder, EventData, EventExt, SharedCompletionTx};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{error, Instrument};

/// Receiver for a per-event completion signal carrying an optional JSON value.
type PerEventCompletionRx =
    oneshot::Receiver<Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>>;

/// Errors that can occur during loop processing operations.
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
    #[error("Expected array at key '{key}', got: {got}")]
    ExpectedArray { key: String, got: String },
    #[error("Key '{}' not found in JSON object", _0)]
    KeyNotFound(String),
    #[error("Expected JSON event data, got ArrowRecordBatch")]
    ExpectedJsonGotArrowRecordBatch,
    #[error("Expected JSON event data, got Avro")]
    ExpectedJsonGotAvro,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Coordinates completion signaling across all events emitted for a single
/// iterated array.
///
/// Each emitted event gets its own per-event `SharedCompletionTx`. The
/// associated receivers are joined into a "fan-in" task which awaits all of
/// them and only then forwards a single completion signal to the upstream
/// source. This is required because downstream processors (storage_write,
/// http_request, script, etc.) spawn a `tokio::task` per incoming event —
/// attaching the upstream completion to only the last emitted event would
/// signal completion while the other emissions are still in flight, which
/// leads to premature NATS acks and silent data loss.
///
/// If any per-event completion reports an error, the first such error wins
/// and is forwarded upstream. Missing per-event signals (handler dropped
/// the sender without completing) are treated as success to avoid
/// deadlocking the source on buggy downstream handlers.
fn spawn_fan_in_completion(
    upstream: SharedCompletionTx,
    per_event_receivers: Vec<PerEventCompletionRx>,
) {
    tokio::spawn(async move {
        let total = per_event_receivers.len();
        let remaining = Arc::new(AtomicUsize::new(total));
        let first_error: Arc<Mutex<Option<Box<dyn std::error::Error + Send + Sync>>>> =
            Arc::new(Mutex::new(None));
        let (done_tx, done_rx) = oneshot::channel::<()>();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));

        for rx in per_event_receivers {
            let remaining = Arc::clone(&remaining);
            let first_error = Arc::clone(&first_error);
            let done_tx = Arc::clone(&done_tx);
            tokio::spawn(async move {
                let outcome = rx.await;
                if let Ok(Err(e)) = outcome {
                    if let Ok(mut guard) = first_error.lock() {
                        if guard.is_none() {
                            *guard = Some(e);
                        }
                    }
                }
                if remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                    if let Ok(mut guard) = done_tx.lock() {
                        if let Some(tx) = guard.take() {
                            tx.send(()).ok();
                        }
                    }
                }
            });
        }

        // Wait for all per-event completions to report in.
        done_rx.await.ok();

        // Forward the aggregated result to the upstream completion signal.
        if let Ok(mut guard) = upstream.lock() {
            if let Some(tx) = guard.take() {
                let result = match first_error.lock().ok().and_then(|mut g| g.take()) {
                    Some(err) => Err(err),
                    None => Ok(None),
                };
                tx.send(result).ok();
            }
        }
    });
}

/// Handles individual event processing by iterating over JSON arrays.
pub struct EventHandler {
    /// Loop processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events (optional if this is the last task).
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes an event by iterating over a JSON array and emitting individual events.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();
        crate::event::with_event_context(&Arc::clone(&event), async move {
            let json_data = match &event.data {
                EventData::Json(data) => data,
                EventData::ArrowRecordBatch(_) => {
                    return Err(Error::ExpectedJsonGotArrowRecordBatch)
                }
                EventData::Avro(_) => return Err(Error::ExpectedJsonGotAvro),
            };

            let array = match &self.config.iterate_key {
                Some(key) => {
                    let value = json_data
                        .get(key)
                        .ok_or_else(|| Error::KeyNotFound(key.clone()))?;
                    match value {
                        Value::Array(arr) => arr.clone(),
                        _ => {
                            return Err(Error::ExpectedArray {
                                key: key.clone(),
                                got: format!("{value:?}"),
                            })
                        }
                    }
                }
                None => match json_data {
                    Value::Array(arr) => arr.clone(),
                    _ => {
                        return Err(Error::ExpectedArray {
                            key: "root".to_string(),
                            got: format!("{json_data:?}"),
                        })
                    }
                },
            };

            let array_len = array.len();

            // Handle empty arrays by signaling completion immediately.
            if array_len == 0 {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    if let Ok(mut guard) = arc.lock() {
                        if let Some(tx) = guard.take() {
                            tx.send(Ok(event.data_as_json().ok())).ok();
                        }
                    }
                }
                return Ok(());
            }

            // If this is the final task in the flow there is nothing
            // downstream to wait for — the emitted events go nowhere. Signal
            // upstream immediately so the source is not blocked.
            if self.tx.is_none() {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    if let Ok(mut guard) = arc.lock() {
                        if let Some(tx) = guard.take() {
                            tx.send(Ok(None)).ok();
                        }
                    }
                }
                return Ok(());
            }

            // For intermediate iterate tasks, attach a per-event completion
            // sender to every emitted event and fan-in their signals so the
            // upstream source only sees completion after ALL events finish.
            // See spawn_fan_in_completion for details.
            let mut per_event_receivers = Vec::with_capacity(array_len);

            for element in array.iter() {
                if self.task_context.cancellation_token.is_cancelled() {
                    return Ok(());
                }

                let mut e = EventBuilder::new()
                    .data(EventData::Json(element.clone()))
                    .subject(self.config.name.to_owned())
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|source| Error::EventBuilder { source })?;

                let (per_tx, per_rx) = oneshot::channel();
                e.completion_tx = Some(Arc::new(Mutex::new(Some(per_tx))));
                per_event_receivers.push(per_rx);

                e.send_with_logging(self.tx.as_ref())
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }

            if let Some(upstream) = completion_tx_arc.as_ref() {
                spawn_fan_in_completion(Arc::clone(upstream), per_event_receivers);
            }

            Ok(())
        })
        .await
    }
}

/// Loop processor that iterates over JSON arrays.
#[derive(Debug)]
pub struct Processor {
    /// Loop processor configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events (optional if this is the last task).
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

#[async_trait::async_trait]
impl crate::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the loop processor.
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize iterate processor");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        error!(error = %e, "Failed to iterate event");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Iterate failed after all retry attempts.");
                                // Emit error event downstream for error handling.
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                None => return Ok(()),
            }
        }
    }
}

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Loop processor configuration (required for build).
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
    use serde_json::{json, Map};
    use tokio::sync::mpsc;

    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Loop Test".to_string()),
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
    async fn test_processor_builder() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
        });
        let (tx, rx) = mpsc::channel(100);

        // Success case.
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

        // Error case - missing config.
        let (tx2, rx2) = mpsc::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx2)
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_event_handler_iterate_root_array() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!([{"id": 1}, {"id": 2}, {"id": 3}])),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let mut count = 0;
        while let Some(output_event) = rx.recv().await {
            match output_event.data {
                EventData::Json(value) => {
                    assert!(value.get("id").is_some());
                    count += 1;
                }
                _ => panic!("Expected JSON data"),
            }
            if count == 3 {
                break;
            }
        }

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_event_handler_iterate_nested_array() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: Some("items".to_string()),
            depends_on: None,
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({
                "items": [{"name": "a"}, {"name": "b"}],
                "total": 2
            })),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let mut count = 0;
        while let Some(output_event) = rx.recv().await {
            match output_event.data {
                EventData::Json(value) => {
                    assert!(value.get("name").is_some());
                    count += 1;
                }
                _ => panic!("Expected JSON data"),
            }
            if count == 2 {
                break;
            }
        }

        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_event_handler_key_not_found() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: Some("missing".to_string()),
            depends_on: None,
            retry: None,
        });

        let (tx, _rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"items": [1, 2, 3]})),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        let result = event_handler.handle(input_event).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::KeyNotFound(_)));
    }

    /// Verifies the upstream completion signal is only fired AFTER every
    /// emitted per-event completion reports in. Previously iterate attached
    /// completion_tx to only the last element, leading to premature source
    /// acks while other downstream tasks were still processing.
    #[tokio::test]
    async fn test_event_handler_fans_in_completion_across_all_events() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let (upstream_tx, upstream_rx) = oneshot::channel();
        let upstream_shared: SharedCompletionTx = Arc::new(Mutex::new(Some(upstream_tx)));

        let input_event = Event {
            data: EventData::Json(json!([{"id": 1}, {"id": 2}, {"id": 3}])),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: Some(Arc::clone(&upstream_shared)),
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let mut per_event_completions = Vec::new();
        while per_event_completions.len() < 3 {
            let event = rx.recv().await.expect("expected emitted event");
            per_event_completions.push(
                event
                    .completion_tx
                    .expect("per-event completion_tx attached"),
            );
        }

        // Upstream must still be pending — nothing acked yet.
        let pending = tokio::time::timeout(std::time::Duration::from_millis(50), async {
            let arc = Arc::clone(&upstream_shared);
            loop {
                let is_done = arc
                    .lock()
                    .ok()
                    .and_then(|g| g.as_ref().map(|_| ()))
                    .is_none();
                if is_done {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            pending.is_err(),
            "upstream completed before any per-event signal"
        );

        // Signal two of three — upstream still pending.
        for arc in per_event_completions.iter().take(2) {
            if let Ok(mut guard) = arc.lock() {
                if let Some(tx) = guard.take() {
                    tx.send(Ok(None)).unwrap();
                }
            }
        }
        let still_pending = tokio::time::timeout(std::time::Duration::from_millis(50), async {
            let arc = Arc::clone(&upstream_shared);
            loop {
                let is_done = arc
                    .lock()
                    .ok()
                    .and_then(|g| g.as_ref().map(|_| ()))
                    .is_none();
                if is_done {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            still_pending.is_err(),
            "upstream completed before final per-event signal"
        );

        // Signal the last one — upstream should now fire.
        if let Ok(mut guard) = per_event_completions[2].lock() {
            if let Some(tx) = guard.take() {
                tx.send(Ok(None)).unwrap();
            }
        }

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), upstream_rx)
            .await
            .expect("upstream completion timed out")
            .expect("upstream receiver dropped");
        assert!(
            result.is_ok(),
            "expected Ok aggregate completion, got {result:?}"
        );
    }

    /// Verifies that when a processor is the last in a flow (no downstream
    /// `tx`) iterate signals upstream completion immediately rather than
    /// waiting indefinitely for per-event signals that will never fire.
    #[tokio::test]
    async fn test_event_handler_signals_immediately_when_no_downstream() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
        });

        let event_handler = EventHandler {
            config,
            tx: None,
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let (upstream_tx, upstream_rx) = oneshot::channel();
        let upstream_shared: SharedCompletionTx = Arc::new(Mutex::new(Some(upstream_tx)));

        let input_event = Event {
            data: EventData::Json(json!([{"id": 1}, {"id": 2}])),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: Some(upstream_shared),
        };

        event_handler.handle(input_event).await.unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_millis(100), upstream_rx)
            .await
            .expect("upstream completion should fire immediately")
            .expect("upstream receiver dropped");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_event_handler_expected_array_error() {
        let config = Arc::new(super::super::config::Processor {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
        });

        let (tx, _rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"not": "an array"})),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        let result = event_handler.handle(input_event).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ExpectedArray { .. }));
    }
}
