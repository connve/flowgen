//! The `iterate` task: emits one event per element of a JSON array,
//! enabling fan-out processing patterns.

use crate::event::{
    new_completion_channel, CompletionRx, Event, EventBuilder, EventData, EventExt,
    SharedCompletionTx,
};
use crate::task::TaskConfig;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

/// Meta key with the number of elements in the array an event came from.
pub const ITERATE_COUNT_META: &str = "iterate_count";

/// Meta key with the identity of the array an event came from, unique per fanned-out array.
pub const ITERATE_ID_META: &str = "iterate_id";

/// Configuration for loop processor tasks that iterate over JSON arrays.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Iterate {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Optional key to extract array from JSON object.
    /// If None, assumes the root element is an array.
    /// If Some("key"), extracts the array from data["key"].
    pub iterate_key: Option<String>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<crate::retry::RetryConfig>,
    /// Events handled at once; defaults to the number of CPUs.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

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
    #[error("Failed to convert event data to JSON: {source}")]
    EventConversion {
        #[source]
        source: crate::event::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Coordinates completion signalling across all events emitted for a single
/// iterated array.
///
/// Each emitted event carries its own per-element completion channel. The
/// associated receivers are joined into a fan-in task which awaits all of
/// them and only then signals the upstream source. This is required because
/// downstream processors spawn a `tokio::task` per incoming event —
/// attaching the upstream completion to only the last emitted event would
/// signal completion while other elements are still in flight, leading to
/// premature acknowledgements and silent data loss.
///
/// The upstream `CompletionState` was sized at flow build time to expect one
/// signal per leaf reachable from iterate's subtree. Once every element
/// finishes, the fan-in emits exactly that many upstream signals so the
/// original source's leaf-count contract is satisfied.
///
/// The source gets the payload of the last element to finish with one.
/// Elements that finish without a payload or with an error don't change it,
/// so per-element errors aren't passed on.
fn spawn_fan_in_completion(
    upstream: SharedCompletionTx,
    per_event_receivers: Vec<CompletionRx>,
    upstream_leaf_share: usize,
) {
    tokio::spawn(async move {
        let mut pending: FuturesUnordered<CompletionRx> = per_event_receivers.into_iter().collect();
        let mut payload = None;
        while let Some(result) = pending.next().await {
            if let Ok(Ok(Some(value))) = result {
                payload = Some(value);
            }
        }

        // Emit one upstream signal per leaf in iterate's subtree. The source
        // sized its completion channel for the full leaf set; collapsing the
        // entire subtree into a single signal would leave the source waiting
        // for the rest forever.
        for _ in 0..upstream_leaf_share.max(1) {
            upstream.signal_completion(payload.clone());
        }
    });
}

/// Handles individual event processing by iterating over JSON arrays.
pub struct EventHandler {
    /// Loop processor configuration settings.
    config: Arc<Iterate>,
    /// Channel sender for processed events (optional if this is the last task).
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl crate::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    /// Processes an event by iterating over a JSON array and emitting individual events.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();
        crate::event::with_event_context(&Arc::clone(&event), async move {
            // Convert Arrow / Avro inputs to JSON so iterate can walk the rows
            // uniformly — Arrow batches become JSON arrays of row objects,
            // Avro records decode to their JSON shape. Pure-JSON inputs go
            // through the same path; the inner clone is the price for one
            // unified codepath.
            let json_data =
                Value::try_from(&event.data).map_err(|source| Error::EventConversion { source })?;

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
                None => match &json_data {
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

            // Handle empty arrays by signalling completion immediately. The
            // upstream channel was sized for every leaf reachable from
            // iterate's subtree, so emit one signal per leaf to satisfy that
            // contract even though no downstream work runs.
            if array_len == 0 {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    let upstream_leaf_share = self.task_context.leaf_count.max(1);
                    let payload = event.data_as_json().ok();
                    for _ in 0..upstream_leaf_share {
                        arc.signal_completion(payload.clone());
                    }
                }
                return Ok(());
            }

            // If iterate is the final task in the flow, the emitted events
            // have nowhere to go. Iterate is itself the only leaf in its
            // subtree (leaf_count == 1), so a single signal completes the
            // upstream contract.
            if self.tx.is_none() {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(None);
                }
                return Ok(());
            }

            // For intermediate iterate tasks, attach a per-element completion
            // channel to every emitted event and fan-in their signals so the
            // upstream source sees completion only after every element
            // finishes. Each per-element channel is sized to the number of
            // leaves reachable downstream of iterate so multi-leaf subgraphs
            // are handled correctly. See spawn_fan_in_completion for details.
            let downstream_leaves = self.task_context.leaf_count.max(1);
            let mut per_event_receivers = Vec::with_capacity(array_len);
            let array_id = uuid::Uuid::now_v7().to_string();

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

                let meta = e.meta.get_or_insert_with(Default::default);
                meta.insert(ITERATE_COUNT_META.to_string(), Value::from(array_len));
                meta.insert(ITERATE_ID_META.to_string(), Value::from(array_id.as_str()));
                let (per_state, per_rx) = new_completion_channel(downstream_leaves);
                e.completion_tx = Some(per_state);
                per_event_receivers.push(per_rx);

                e.send_with_logging(self.tx.as_ref())
                    .context("array_length", array_len)
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }

            if let Some(upstream) = completion_tx_arc.as_ref() {
                spawn_fan_in_completion(
                    Arc::clone(upstream),
                    per_event_receivers,
                    downstream_leaves,
                );
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
    config: Arc<Iterate>,
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
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize iterate processor");
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

        crate::task::runner::process_events(
            self.rx,
            event_handler,
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
    }
}

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Loop processor configuration (required for build).
    config: Option<Arc<Iterate>>,
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

    pub fn config(mut self, config: Arc<Iterate>) -> Self {
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
    use crate::task::runner::Handler;
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
        let config = Arc::new(Iterate {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
            max_in_flight: None,
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
        let config = Arc::new(Iterate {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
            max_in_flight: None,
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
            session: None,
            call_stack: Vec::new(),
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
        let config = Arc::new(Iterate {
            name: "test".to_string(),
            iterate_key: Some("items".to_string()),
            depends_on: None,
            retry: None,
            max_in_flight: None,
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
            session: None,
            call_stack: Vec::new(),
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
        let config = Arc::new(Iterate {
            name: "test".to_string(),
            iterate_key: Some("missing".to_string()),
            depends_on: None,
            retry: None,
            max_in_flight: None,
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
            session: None,
            call_stack: Vec::new(),
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
        let config = Arc::new(Iterate {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
            max_in_flight: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let (upstream_state, mut upstream_rx) = new_completion_channel(1);

        let input_event = Event {
            data: EventData::Json(json!([{"id": 1}, {"id": 2}, {"id": 3}])),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: Some(Arc::clone(&upstream_state)),
            session: None,
            call_stack: Vec::new(),
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

        // Upstream must still be pending — no per-event signals yet.
        let pending =
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut upstream_rx).await;
        assert!(
            pending.is_err(),
            "upstream completed before any per-event signal"
        );

        // Signal two of three — upstream still pending.
        for arc in per_event_completions.iter().take(2) {
            arc.signal_completion(None);
        }
        let still_pending =
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut upstream_rx).await;
        assert!(
            still_pending.is_err(),
            "upstream completed before final per-event signal"
        );

        // Signal the last one — upstream should now fire.
        per_event_completions[2].signal_completion(None);

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), upstream_rx)
            .await
            .expect("upstream completion timed out")
            .expect("upstream receiver dropped");
        assert!(
            result.is_ok(),
            "expected Ok aggregate completion, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_event_handler_passes_the_last_result_to_the_source() {
        let config = Arc::new(Iterate {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
            max_in_flight: None,
        });
        let (tx, mut rx) = mpsc::channel(100);
        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };
        let (upstream_state, upstream_rx) = new_completion_channel(1);
        let input_event = Event {
            data: EventData::Json(json!([{"id": 1}, {"id": 2}])),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: Some(upstream_state),
            session: None,
            call_stack: Vec::new(),
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });
        let first = rx.recv().await.unwrap();
        assert_eq!(first.meta.as_ref().unwrap()[ITERATE_COUNT_META], json!(2));
        let first = first.completion_tx.unwrap();
        let second = rx.recv().await.unwrap().completion_tx.unwrap();
        first.signal_completion(Some(json!({"n": 1})));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        second.signal_completion(Some(json!({"n": 2})));

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), upstream_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap(), Some(json!({"n": 2})));
    }

    #[tokio::test]
    async fn an_element_finishing_without_a_payload_keeps_the_last_payload_for_the_source() {
        let (tx, mut rx) = mpsc::channel(100);
        let event_handler = EventHandler {
            config: Arc::new(Iterate::default()),
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };
        let (upstream_state, upstream_rx) = new_completion_channel(1);
        let mut input_event = EventBuilder::new()
            .data(EventData::Json(json!([1, 2])))
            .subject("input.subject".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();
        input_event.completion_tx = Some(upstream_state);

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });
        let first = rx.recv().await.unwrap().completion_tx.unwrap();
        let second = rx.recv().await.unwrap().completion_tx.unwrap();
        first.signal_completion(Some(json!({"n": 1})));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        second.signal_completion(None);

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), upstream_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap(), Some(json!({"n": 1})));
    }

    #[tokio::test]
    async fn every_element_of_an_array_carries_the_array_id_and_each_array_gets_its_own() {
        let (tx, mut rx) = mpsc::channel(100);
        let event_handler = EventHandler {
            config: Arc::new(Iterate::default()),
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };
        let array = |data: Value| {
            EventBuilder::new()
                .data(EventData::Json(data))
                .subject("input.subject".to_string())
                .task_id(0)
                .task_type("test")
                .build()
                .unwrap()
        };

        event_handler.handle(array(json!([1, 2]))).await.unwrap();
        event_handler.handle(array(json!([3]))).await.unwrap();

        let mut ids = Vec::new();
        for _ in 0..3 {
            let meta = rx.recv().await.unwrap().meta.unwrap();
            ids.push(meta[ITERATE_ID_META].as_str().unwrap().to_string());
        }
        assert_eq!(ids[0], ids[1]);
        assert_ne!(ids[1], ids[2]);
    }

    /// Verifies that when a processor is the last in a flow (no downstream
    /// `tx`) iterate signals upstream completion immediately rather than
    /// waiting indefinitely for per-event signals that will never fire.
    #[tokio::test]
    async fn test_event_handler_signals_immediately_when_no_downstream() {
        let config = Arc::new(Iterate {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
            max_in_flight: None,
        });

        let event_handler = EventHandler {
            config,
            tx: None,
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let (upstream_state, upstream_rx) = new_completion_channel(1);

        let input_event = Event {
            data: EventData::Json(json!([{"id": 1}, {"id": 2}])),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: Some(upstream_state),
            session: None,
            call_stack: Vec::new(),
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
        let config = Arc::new(Iterate {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
            max_in_flight: None,
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
            session: None,
            call_stack: Vec::new(),
        };

        let result = event_handler.handle(input_event).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ExpectedArray { .. }));
    }

    /// Iterate accepts Arrow record batches by converting them to a JSON
    /// array of row objects and walking the rows. This unblocks the common
    /// ETL pattern where an upstream task (BigQuery storage read, MSSQL
    /// query) emits Arrow and a downstream `iterate` walks the result set.
    #[tokio::test]
    async fn test_event_handler_iterate_arrow_record_batch() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let config = Arc::new(Iterate {
            name: "test".to_string(),
            iterate_key: None,
            depends_on: None,
            retry: None,
            max_in_flight: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let id_array = Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_array)]).unwrap();

        let input_event = Event {
            data: EventData::ArrowRecordBatch(batch),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
            session: None,
            call_stack: Vec::new(),
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        let mut ids = Vec::new();
        while let Some(output_event) = rx.recv().await {
            match output_event.data {
                EventData::Json(value) => {
                    let id = value
                        .get("id")
                        .and_then(|v| v.as_i64())
                        .expect("row should carry an id");
                    ids.push(id);
                }
                _ => panic!("Expected JSON data"),
            }
            if ids.len() == 3 {
                break;
            }
        }

        assert_eq!(ids, vec![1, 2, 3]);
    }
}
