//! `inproc_request`: calls a flow with an `inproc_endpoint` and emits its result.

use super::registry::CallError;
use crate::config::ConfigExt;
use crate::event::{Event, EventBuilder, EventData, EventExt};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

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
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

impl Error {
    /// Whether calling again cannot help: the flow ran and failed, or may still be running.
    fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::Call(
                CallError::Failed { .. } | CallError::Timeout { .. } | CallError::NotAllowed { .. }
            ) | Error::NotJson { .. }
                | Error::RenderConfig { .. }
        )
    }
}

/// Calls the configured flow for each event.
pub struct EventHandler {
    config: Arc<super::config::Request>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<crate::task::context::TaskContext>,
}

impl EventHandler {
    #[tracing::instrument(skip(self, event), name = "task.handle")]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }
        let event_value =
            serde_json::Value::try_from(&event).map_err(|source| Error::EventBuilder { source })?;
        let config = self
            .config
            .render(&event_value)
            .map_err(|source| Error::RenderConfig { source })?;
        let data = match event.data_as_json() {
            Ok(data) => data,
            Err(source) => return Err(Error::NotJson { source }),
        };
        let meta = match &event.meta {
            Some(meta) => meta.clone(),
            None => serde_json::Map::new(),
        };
        let result = self
            .task_context
            .inproc
            .call(
                Some(self.task_context.flow.identity()),
                &config.flow,
                data,
                meta,
            )
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
    config: Arc<super::config::Request>,
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
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let handler = Arc::new(self.init().await?);

        let mut handlers = Vec::new();
        while let Some(event) = self.rx.recv().await {
            let handler = Arc::clone(&handler);
            let retry_strategy = retry_config.strategy();
            let handle = tokio::spawn(
                async move {
                    if let Some(error) = event.error.clone() {
                        event.forward_failure(handler.tx.as_ref(), error).await;
                        return;
                    }
                    let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                        handler.handle(event.clone()).await.map_err(|e| {
                            error!(error = %e, "Inproc request failed");
                            match e.is_permanent() {
                                true => tokio_retry::RetryError::permanent(e),
                                false => tokio_retry::RetryError::transient(e),
                            }
                        })
                    })
                    .await;
                    if let Err(e) = result {
                        event
                            .forward_failure(handler.tx.as_ref(), e.to_string())
                            .await;
                    }
                }
                .instrument(tracing::Span::current()),
            );
            handlers.push(handle);
            handlers.retain(|h| !h.is_finished());
        }
        futures_util::future::join_all(handlers).await;
        Ok(())
    }
}

/// Builder for the in-process request processor.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Request>>,
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

    pub fn config(mut self, config: Arc<super::config::Request>) -> Self {
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
