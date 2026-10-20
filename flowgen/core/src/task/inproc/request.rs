//! `inproc_request`: calls a flow with an `inproc_endpoint` and emits its result.

use super::registry::CallError;
use crate::config::ConfigExt;
use crate::event::{Event, EventBuilder, EventData, EventExt};
use crate::task::TaskConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

/// Processor that calls a flow with an `inproc_endpoint` and emits its result.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// The unique name of the task.
    pub name: String,
    /// Identity of the flow to call, e.g. `system/publish_workspace`.
    pub flow: String,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<crate::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for Request {}

/// Errors from the in-process request processor.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Call(#[from] CallError),
    #[error("Event data is not JSON: {source}")]
    NotJson {
        #[source]
        source: crate::event::Error,
    },
    #[error("Failed to render config: {source}")]
    RenderConfig {
        #[source]
        source: crate::config::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("Error sending event: {source}")]
    SendMessage {
        #[source]
        source: crate::event::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
}

impl Error {
    /// Whether calling again cannot help. Only a call whose event never reached
    /// the flow is retried, since any other retry would run the flow again.
    fn is_permanent(&self) -> bool {
        !matches!(
            self,
            Error::Call(CallError::NotFound { .. } | CallError::Closed { .. })
        )
    }
}

/// Calls the configured flow for each event.
pub struct EventHandler {
    config: Arc<Request>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<crate::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl crate::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    fn is_retryable(&self, error: &Error) -> bool {
        !error.is_permanent()
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if let Some(error) = event.error.clone() {
            event.forward_failure(self.tx.as_ref(), error).await;
            return Ok(());
        }
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }
        let config = self
            .config
            .render_for_event(&event)
            .map_err(|source| Error::RenderConfig { source })?;
        let data = match event.data_as_json() {
            Ok(data) => data,
            Err(source) => return Err(Error::NotJson { source }),
        };
        let meta = match &event.meta {
            Some(meta) => meta.clone(),
            None => serde_json::Map::new(),
        };
        let mut callers = event.call_stack.clone();
        callers.push(self.task_context.flow.identity().to_string());
        let result = self
            .task_context
            .inproc
            .call(&callers, &config.flow, data, meta, event.session.clone())
            .await?;

        let mut builder = EventBuilder::new()
            .data(EventData::Json(match result {
                Some(result) => result,
                None => serde_json::Value::Null,
            }))
            .subject(self.config.name.clone())
            .task_id(self.task_id)
            .task_type(self.task_type);
        if let Some(meta) = &event.meta {
            builder = builder.meta_merge(meta.clone());
        }
        if let Some(session) = &event.session {
            builder = builder.session(Arc::clone(session));
        }
        builder.call_stack = event.call_stack.clone();
        let mut e = builder
            .build()
            .map_err(|source| Error::EventBuilder { source })?;
        match self.tx {
            None => {
                if let Some(arc) = event.completion_tx.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
            Some(_) => e.completion_tx = event.completion_tx.clone(),
        }
        e.send_with_logging(self.tx.as_ref())
            .context("flow", &config.flow)
            .await
            .map_err(|source| Error::SendMessage { source })
    }
}

/// In-process request processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Request>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<crate::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        Ok(EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let handler = Arc::new(self.init().await?);

        crate::task::runner::process_events(
            self.rx,
            handler,
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
    }
}

/// Builder for the in-process request processor.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<Request>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: Option<&'static str>,
    task_context: Option<Arc<crate::task::context::TaskContext>>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<Request>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
        self.task_context = Some(task_context);
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
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_error() -> crate::event::Error {
        crate::event::Error::MissingBuilderAttribute("data".to_string())
    }

    #[test]
    fn only_a_call_that_never_reached_the_flow_is_retried() {
        let flow = || "a/b".to_string();
        let retried = [
            Error::Call(CallError::NotFound { flow: flow() }),
            Error::Call(CallError::Closed { flow: flow() }),
        ];
        let permanent = [
            Error::Call(CallError::Dropped),
            Error::Call(CallError::Recursive { flow: flow() }),
            Error::Call(CallError::Failed {
                reason: "conflict".to_string(),
            }),
            Error::Call(CallError::Timeout {
                timeout: std::time::Duration::from_secs(1),
            }),
            Error::SendMessage {
                source: event_error(),
            },
            Error::EventBuilder {
                source: event_error(),
            },
        ];

        for error in &retried {
            assert!(!error.is_permanent(), "{error}");
        }
        for error in &permanent {
            assert!(error.is_permanent(), "{error}");
        }
    }
}
