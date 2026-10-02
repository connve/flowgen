//! OCI push processor: releases the files of each event as an artifact.

use super::client::{ArtifactFile, Push};
use super::config::Processor as ProcessorConfig;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors from the OCI push processor.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Push(#[from] super::client::Error),
    #[error("Event data must hold a `files` list of {{path, content}}: {source}")]
    Input {
        #[source]
        source: serde_json::Error,
    },
    #[error("Event data is not JSON: {source}")]
    NotJson {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to render config: {source}")]
    RenderConfig {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error sending event: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

impl Error {
    /// Whether retrying the same event cannot succeed.
    fn is_permanent(&self) -> bool {
        match self {
            Error::Push(source) => matches!(
                source,
                super::client::Error::NoTags | super::client::Error::InvalidReference { .. }
            ),
            Error::Input { .. } | Error::NotJson { .. } | Error::RenderConfig { .. } => true,
            _ => false,
        }
    }
}

#[derive(Deserialize)]
struct InputFile {
    path: String,
    content: String,
}

#[derive(Deserialize)]
struct Input {
    files: Vec<InputFile>,
}

/// Event handler for OCI push operations.
pub struct EventHandler {
    config: Arc<ProcessorConfig>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Pushes the event's files and emits the data with the digest added.
    #[tracing::instrument(skip(self, event), name = "task.handle")]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }
        let event = Arc::new(event);
        let completion_tx = event.completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            let event_value = serde_json::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config: ProcessorConfig = self
                .config
                .render(&event_value)
                .map_err(|source| Error::RenderConfig { source })?;
            let mut data = match event.data_as_json() {
                Ok(data) => data,
                Err(source) => return Err(Error::NotJson { source }),
            };
            let input: Input =
                serde_json::from_value(data.clone()).map_err(|source| Error::Input { source })?;

            let files: Vec<ArtifactFile> = input
                .files
                .into_iter()
                .map(|file| ArtifactFile {
                    path: file.path,
                    content: file.content.into_bytes(),
                })
                .collect();
            let push = Push {
                repository: config.repository.clone(),
                credentials_path: config.credentials_path.clone(),
            };
            let pushed = push.push(&files, &config.tags).await?;

            if let Some(fields) = data.as_object_mut() {
                fields.remove("files");
                fields.insert("digest".to_string(), pushed.digest.clone().into());
                fields.insert("references".to_string(), pushed.references.clone().into());
            }
            let mut e = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(self.config.name.clone())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;
            match self.tx {
                None => {
                    if let Some(arc) = completion_tx.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => e.completion_tx = completion_tx.clone(),
            }
            e.send_with_logging(self.tx.as_ref())
                .context("digest", &pushed.digest)
                .await
                .map_err(|source| Error::SendMessage { source })
        })
        .await
    }
}

/// OCI push processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<ProcessorConfig>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
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
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);
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
                            error!(error = %e, "OCI push failed");
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

/// Builder for the OCI push processor.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<ProcessorConfig>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<ProcessorConfig>) -> Self {
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

    pub fn task_context(mut self, ctx: Arc<flowgen_core::task::context::TaskContext>) -> Self {
        self.task_context = Some(ctx);
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
