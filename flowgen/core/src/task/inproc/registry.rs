//! Flows callable in-process, keyed by flow identity.

use crate::event::{new_completion_channel, Event, EventBuilder, EventData};
use dashmap::DashMap;
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tracing::info;

/// Failure of an in-process flow call.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum CallError {
    #[error("No flow with an inproc_endpoint runs as '{flow}' on this pod")]
    NotFound { flow: String },
    #[error("The flow '{caller}' is not allowed to call '{flow}'; see the callers of its inproc_endpoint")]
    NotAllowed { caller: String, flow: String },
    #[error("The flow '{flow}' cannot call itself")]
    Recursive { flow: String },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("The flow '{flow}' stopped accepting calls")]
    Closed { flow: String },
    #[error("The flow failed: {reason}")]
    Failed { reason: String },
    #[error("The flow did not finish within {timeout:?}")]
    Timeout { timeout: Duration },
    #[error("The flow stopped before finishing")]
    Dropped,
}

/// An `inproc_endpoint` registered by a running flow.
#[derive(Clone, Debug)]
pub struct Registration {
    /// Channel into the flow, after its `inproc_endpoint`.
    pub tx: Sender<Event>,
    /// Name of the registering task, used as the event subject.
    pub task_name: String,
    /// Index of the registering task in its flow.
    pub task_id: usize,
    /// Type of the registering task.
    pub task_type: &'static str,
    /// Leaves that must finish before a call completes.
    pub leaf_count: usize,
    /// How long a call may take, from handing over its event to the flow
    /// finishing. Unbounded when unset.
    pub ack_timeout: Option<Duration>,
    /// Flows allowed to call: each entry admits the flow of that identity and
    /// every flow in it as a folder, and an empty entry admits every flow.
    pub callers: Vec<String>,
}

/// Callers a flow accepts by default: flows in its own top-level folder, or
/// every flow when it is not in a folder.
pub fn default_callers(flow: &str) -> Vec<String> {
    match flow.split_once('/') {
        Some((folder, _)) => vec![format!("{folder}/")],
        None => vec![String::new()],
    }
}

/// Flows callable in-process. One per process, shared by every flow.
#[derive(Debug, Default)]
pub struct InprocRegistry {
    table: DashMap<String, Arc<Registration>>,
}

impl InprocRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `flow`, replacing a registration left by an earlier run of the same flow.
    pub fn register(&self, flow: String, registration: Registration) {
        info!(flow = %flow, "Registering inproc endpoint");
        self.table.insert(flow, Arc::new(registration));
    }

    /// Removes `flow`'s registration when the flow stops.
    pub fn deregister_flow(&self, flow: &str) {
        self.table.remove(flow);
    }

    /// Runs `flow` with `data` and `meta` and returns the result of its last
    /// task. `caller` is the calling flow's identity, `None` for flowgen itself.
    pub async fn call(
        &self,
        caller: Option<&str>,
        flow: &str,
        data: Value,
        meta: Map<String, Value>,
    ) -> Result<Option<Value>, CallError> {
        if caller == Some(flow) {
            return Err(CallError::Recursive {
                flow: flow.to_string(),
            });
        }
        let registration = match self.table.get(flow) {
            Some(entry) => Arc::clone(entry.value()),
            None => {
                return Err(CallError::NotFound {
                    flow: flow.to_string(),
                })
            }
        };
        if let Some(caller) = caller {
            if !registration
                .callers
                .iter()
                .any(|entry| crate::identity::within(caller, entry))
            {
                return Err(CallError::NotAllowed {
                    caller: caller.to_string(),
                    flow: flow.to_string(),
                });
            }
        }
        let (completion_tx, completion_rx) = new_completion_channel(registration.leaf_count);
        let event = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(registration.task_name.clone())
            .task_id(registration.task_id)
            .task_type(registration.task_type)
            .completion_tx(completion_tx)
            .meta_merge(meta)
            .build()
            .map_err(|source| CallError::EventBuilder { source })?;
        let delivery = async {
            if registration.tx.send(event).await.is_err() {
                return Err(CallError::Closed {
                    flow: flow.to_string(),
                });
            }
            match completion_rx.await {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(reason)) => Err(CallError::Failed {
                    reason: reason.to_string(),
                }),
                Err(_) => Err(CallError::Dropped),
            }
        };
        match registration.ack_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, delivery).await {
                Ok(outcome) => outcome,
                Err(_) => Err(CallError::Timeout { timeout }),
            },
            None => delivery.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn registration(tx: Sender<Event>, ack_timeout: Option<Duration>) -> Registration {
        Registration {
            tx,
            task_name: "on_call".to_string(),
            task_id: 0,
            task_type: "inproc_endpoint",
            leaf_count: 1,
            ack_timeout,
            callers: default_callers("a/b"),
        }
    }

    #[tokio::test]
    async fn only_allowed_callers_reach_the_flow() {
        let registry = InprocRegistry::new();
        let (tx, mut rx) = mpsc::channel(4);
        registry.register("a/b".to_string(), registration(tx, None));
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                event.completion_tx.unwrap().signal_completion(None);
            }
        });

        let same_folder = registry
            .call(Some("a/c"), "a/b", Value::Null, Map::new())
            .await;
        assert!(same_folder.is_ok(), "{same_folder:?}");
        let flowgen = registry.call(None, "a/b", Value::Null, Map::new()).await;
        assert!(flowgen.is_ok(), "{flowgen:?}");
        let other_folder = registry
            .call(Some("x/y"), "a/b", Value::Null, Map::new())
            .await;
        assert!(
            matches!(other_folder, Err(CallError::NotAllowed { caller, .. }) if caller == "x/y")
        );
    }

    fn answer_every_call(mut rx: mpsc::Receiver<Event>) {
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                event.completion_tx.unwrap().signal_completion(None);
            }
        });
    }

    #[tokio::test]
    async fn callers_admit_whole_identity_segments_only() {
        let registry = InprocRegistry::new();
        let (tx, rx) = mpsc::channel(4);
        registry.register(
            "system/publish".to_string(),
            Registration {
                callers: vec!["system".to_string(), "platform/".to_string()],
                ..registration(tx, None)
            },
        );
        answer_every_call(rx);
        let call = |caller: &'static str| {
            registry.call(Some(caller), "system/publish", Value::Null, Map::new())
        };

        assert!(call("system/sync").await.is_ok());
        assert!(call("system").await.is_ok());
        assert!(call("platform/a/b").await.is_ok());
        assert!(matches!(
            call("systemx/sync").await,
            Err(CallError::NotAllowed { .. })
        ));
        assert!(matches!(
            call("platformer/a").await,
            Err(CallError::NotAllowed { .. })
        ));
    }

    #[tokio::test]
    async fn an_empty_callers_entry_admits_every_flow() {
        let registry = InprocRegistry::new();
        let (tx, rx) = mpsc::channel(4);
        registry.register(
            "a/b".to_string(),
            Registration {
                callers: vec![String::new()],
                ..registration(tx, None)
            },
        );
        answer_every_call(rx);

        let result = registry
            .call(Some("x/y"), "a/b", Value::Null, Map::new())
            .await;
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn a_flow_cannot_call_itself() {
        let registry = InprocRegistry::new();
        let (tx, _rx) = mpsc::channel(1);
        registry.register(
            "a/b".to_string(),
            registration(tx, Some(Duration::from_millis(10))),
        );

        let result = registry
            .call(Some("a/b"), "a/b", Value::Null, Map::new())
            .await;
        assert!(matches!(result, Err(CallError::Recursive { flow }) if flow == "a/b"));
    }

    #[tokio::test]
    async fn a_flow_that_does_not_take_the_call_times_out() {
        let registry = InprocRegistry::new();
        let (tx, _rx) = mpsc::channel(1);
        let pending = EventBuilder::new()
            .data(EventData::Json(Value::Null))
            .subject("pending".to_string())
            .task_id(0)
            .task_type("inproc_endpoint")
            .build()
            .unwrap();
        tx.send(pending).await.unwrap();
        registry.register(
            "a/b".to_string(),
            registration(tx, Some(Duration::from_millis(10))),
        );

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            registry.call(None, "a/b", Value::Null, Map::new()),
        )
        .await;
        assert!(
            matches!(result, Ok(Err(CallError::Timeout { .. }))),
            "{result:?}"
        );
    }

    #[test]
    fn a_flow_outside_a_folder_accepts_every_caller() {
        assert_eq!(
            default_callers("system/publish"),
            vec!["system/".to_string()]
        );
        assert_eq!(default_callers("publish"), vec![String::new()]);
    }

    #[tokio::test]
    async fn a_call_returns_the_result_of_the_flow() {
        let registry = InprocRegistry::new();
        let (tx, mut rx) = mpsc::channel(1);
        registry.register("a/b".to_string(), registration(tx, None));
        tokio::spawn(async move {
            let event = rx.recv().await.unwrap();
            let data = event.data_as_json().unwrap();
            let completion = event.completion_tx.unwrap();
            completion.signal_completion(Some(serde_json::json!({"echo": data})));
        });
        let result = registry
            .call(None, "a/b", serde_json::json!({"x": 1}), Map::new())
            .await
            .unwrap();
        assert_eq!(result, Some(serde_json::json!({"echo": {"x": 1}})));
    }

    #[tokio::test]
    async fn a_failed_flow_is_reported() {
        let registry = InprocRegistry::new();
        let (tx, mut rx) = mpsc::channel(1);
        registry.register("a/b".to_string(), registration(tx, None));
        tokio::spawn(async move {
            let event = rx.recv().await.unwrap();
            event
                .completion_tx
                .unwrap()
                .signal_completion_with_error("conflict".to_string());
        });
        let result = registry.call(None, "a/b", Value::Null, Map::new()).await;
        assert!(matches!(result, Err(CallError::Failed { reason }) if reason == "conflict"));
    }

    #[tokio::test]
    async fn an_unknown_or_stopped_flow_is_not_found() {
        let registry = InprocRegistry::new();
        let (tx, _rx) = mpsc::channel(1);
        registry.register("a/b".to_string(), registration(tx, None));
        registry.deregister_flow("a/b");
        let result = registry.call(None, "a/b", Value::Null, Map::new()).await;
        assert!(matches!(result, Err(CallError::NotFound { flow }) if flow == "a/b"));
    }

    #[tokio::test]
    async fn a_slow_flow_times_out() {
        let registry = InprocRegistry::new();
        let (tx, _rx) = mpsc::channel(1);
        registry.register(
            "a/b".to_string(),
            registration(tx, Some(Duration::from_millis(10))),
        );
        let result = registry.call(None, "a/b", Value::Null, Map::new()).await;
        assert!(matches!(result, Err(CallError::Timeout { .. })));
    }
}
