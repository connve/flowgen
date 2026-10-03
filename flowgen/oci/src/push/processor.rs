//! OCI push processor: releases the files of each event as an artifact.

use super::client::{ArtifactFile, Push, Pushed};
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
    #[error("No tags configured: `tags` must list at least one tag")]
    NoTags,
    #[error("Event data must be a JSON object")]
    NotObject,
    #[error("Event data has no `files` list")]
    MissingFiles,
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
            Error::Push(source) => source.is_permanent(),
            Error::NoTags
            | Error::NotObject
            | Error::MissingFiles
            | Error::Input { .. }
            | Error::NotJson { .. }
            | Error::RenderConfig { .. } => true,
            _ => false,
        }
    }
}

#[derive(Deserialize)]
struct InputFile {
    path: String,
    content: String,
}

/// Event handler for OCI push operations.
pub struct EventHandler {
    config: Arc<ProcessorConfig>,
    pushing: tokio::sync::Mutex<()>,
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
            let ProcessorConfig {
                repository,
                tags,
                credentials_path,
                ..
            } = self
                .config
                .render(&event_value)
                .map_err(|source| Error::RenderConfig { source })?;
            let mut fields = match event.data_as_json() {
                Ok(serde_json::Value::Object(fields)) => fields,
                Ok(_) => return Err(Error::NotObject),
                Err(source) => return Err(Error::NotJson { source }),
            };
            let files = match fields.remove("files") {
                Some(files) => files,
                None => return Err(Error::MissingFiles),
            };
            let files: Vec<InputFile> =
                serde_json::from_value(files).map_err(|source| Error::Input { source })?;
            let files: Vec<ArtifactFile> = files
                .into_iter()
                .map(|InputFile { path, content }| ArtifactFile {
                    path,
                    content: content.into_bytes(),
                })
                .collect();
            let push = Push {
                repository,
                credentials_path,
            };
            let Pushed { digest, references } = {
                let _pushing = self.pushing.lock().await;
                push.push(files, &tags).await?
            };

            fields.insert("digest".to_string(), digest.clone().into());
            fields.insert("references".to_string(), references.into());
            let mut e = EventBuilder::new()
                .data(EventData::Json(serde_json::Value::Object(fields)))
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
                .context("digest", digest)
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
        if self.config.tags.is_empty() {
            return Err(Error::NoTags);
        }
        Ok(EventHandler {
            config: Arc::clone(&self.config),
            pushing: tokio::sync::Mutex::new(()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use flowgen_core::task::runner::Runner;

    fn task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("a".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    fn config(tags: &[&str]) -> ProcessorConfig {
        ProcessorConfig {
            name: "release".to_string(),
            repository: "127.0.0.1:9/a".to_string(),
            tags: tags.iter().map(|tag| tag.to_string()).collect(),
            credentials_path: None,
            depends_on: None,
            retry: None,
        }
    }

    async fn processor(tags: &[&str]) -> Processor {
        let (_, rx) = tokio::sync::mpsc::channel(1);
        ProcessorBuilder::new()
            .config(Arc::new(config(tags)))
            .receiver(rx)
            .task_type("oci_push")
            .task_context(task_context())
            .build()
            .await
            .unwrap()
    }

    fn event(data: EventData) -> Event {
        EventBuilder::new()
            .data(data)
            .subject("a".to_string())
            .task_id(0)
            .task_type("a")
            .build()
            .unwrap()
    }

    fn json(data: &str) -> Event {
        event(EventData::Json(serde_json::from_str(data).unwrap()))
    }

    async fn handle(data: Event) -> Error {
        let handler = processor(&["ok"]).await.init().await.unwrap();
        handler.handle(data).await.unwrap_err()
    }

    #[tokio::test]
    async fn a_config_without_tags_is_rejected_at_init_as_permanent() {
        let result = processor(&[]).await.init().await;
        let error = result.err().unwrap();
        assert!(matches!(error, Error::NoTags));
        assert!(error.is_permanent());
    }

    #[tokio::test]
    async fn event_data_that_is_not_an_object_is_rejected_as_permanent() {
        for data in [
            json(r#"[[{"path": "a.txt", "content": "a"}]]"#),
            json(r#""a""#),
            event(EventData::Bytes(bytes::Bytes::from_static(b"a"))),
        ] {
            let error = handle(data).await;
            assert!(matches!(error, Error::NotObject), "{error}");
            assert!(error.is_permanent());
        }
    }

    #[tokio::test]
    async fn event_data_without_files_is_rejected_as_permanent() {
        let error = handle(json(r#"{"commit": "a"}"#)).await;
        assert!(matches!(error, Error::MissingFiles), "{error}");
        assert!(error.is_permanent());
    }

    #[tokio::test]
    async fn files_that_are_not_path_and_content_pairs_are_rejected_as_permanent() {
        let error = handle(json(r#"{"files": [{"path": "a.txt"}]}"#)).await;
        assert!(matches!(error, Error::Input { .. }), "{error}");
        assert!(error.is_permanent());
    }

    #[tokio::test]
    async fn invalid_file_paths_are_rejected_before_any_push_as_permanent() {
        let error = handle(json(r#"{"files": [{"path": "../a.txt", "content": "a"}]}"#)).await;
        assert!(
            matches!(
                error,
                Error::Push(super::super::client::Error::InvalidPath { .. })
            ),
            "{error}"
        );
        assert!(error.is_permanent());
    }

    #[tokio::test]
    async fn a_tag_that_renders_invalid_is_rejected_before_any_push_as_permanent() {
        let (_, rx) = tokio::sync::mpsc::channel(1);
        let handler = ProcessorBuilder::new()
            .config(Arc::new(config(&["ok", "{{event.data.version}}"])))
            .receiver(rx)
            .task_type("oci_push")
            .task_context(task_context())
            .build()
            .await
            .unwrap()
            .init()
            .await
            .unwrap();
        let error = handler
            .handle(json(
                r#"{"version": "not valid!", "files": [{"path": "a.txt", "content": "a"}]}"#,
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::Push(super::super::client::Error::InvalidReference { .. })
            ),
            "{error}"
        );
        assert!(error.is_permanent());
    }
}
