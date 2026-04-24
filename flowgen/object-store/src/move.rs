use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::{client::Client, task::runner::Runner};
use object_store::ObjectStoreExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};
use tracing::{error, Instrument};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Object store error: {source}")]
    ObjectStore {
        #[source]
        source: object_store::Error,
    },
    #[error("Object store client error: {source}")]
    ObjectStoreClient {
        #[source]
        source: super::client::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Could not initialize object store context")]
    NoObjectStoreContext,
    #[error("Invalid URL format: {source}")]
    ParseUrl {
        #[source]
        source: url::ParseError,
    },
    #[error("Invalid source_files format after template rendering: expected array, got {value}")]
    InvalidSourceFiles { value: String },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Object store move operation requires a source path or source_files.")]
    MissingSource,
    #[error("Object store move operation requires a destination.")]
    MissingDestination,
}

/// Result of a move operation containing source, destination, and file count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveResult {
    /// Source path pattern for files that were moved.
    pub source: String,
    /// Destination path where files were moved to.
    pub destination: String,
    /// Number of files successfully moved.
    pub files_moved: usize,
}

/// Handles processing of individual events by moving files in object storage.
pub struct EventHandler {
    /// Move configuration settings.
    config: Arc<super::config::Processor>,
    /// Object store client for moving files.
    client: Arc<Mutex<super::client::Client>>,
    /// Channel sender for processed events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes an event and moves files in the configured object store.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();
        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config to support templates inside configuration.
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Parse destination path.
            let destination = config
                .destination
                .as_ref()
                .ok_or(Error::MissingDestination)?;
            let destination_str = destination.to_string_lossy();
            let destination_url =
                url::Url::parse(&destination_str).map_err(|source| Error::ParseUrl { source })?;
            let destination_base = object_store::path::Path::from(destination_url.path());

            let mut client_guard = self.client.lock().await;
            let mut files_moved = 0;
            let using_explicit_files = config.source_files.is_some();

            // Determine files to move from either explicit list or wildcard pattern.
            let files_to_move: Vec<object_store::path::Path> = if let Some(ref source_files_value) =
                config.source_files
            {
                // Extract Vec<String> from Value after template rendering.
                let source_files: Vec<String> = match source_files_value {
                    serde_json::Value::Array(arr) => arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect(),
                    _ => {
                        return Err(Error::InvalidSourceFiles {
                            value: source_files_value.to_string(),
                        });
                    }
                };

                // Parse explicit file URIs to extract object store paths.
                source_files
                    .iter()
                    .filter_map(|uri| {
                        url::Url::parse(uri).ok().map(|url| {
                            let path = url.path().trim_start_matches('/');
                            object_store::path::Path::from(path)
                        })
                    })
                    .collect()
            } else {
                // List files matching wildcard source pattern.
                let source_path = config.source.as_ref().ok_or(Error::MissingSource)?;
                let source_str = source_path.to_string_lossy();
                let source_url =
                    url::Url::parse(&source_str).map_err(|source| Error::ParseUrl { source })?;

                // Strip wildcard patterns from source path.
                let source_url_path = source_url.path();
                let source_prefix = match source_url_path.split_once('*') {
                    Some((before, _)) => before.trim_end_matches('/'),
                    None => source_url_path,
                };

                let source_path = object_store::path::Path::from(source_prefix);

                // Use list_with_delimiter to avoid recursing into subdirectories.
                let mut list_result = {
                    let context = client_guard
                        .context
                        .as_ref()
                        .ok_or_else(|| Error::NoObjectStoreContext)?;
                    context
                        .object_store
                        .list_with_delimiter(Some(&source_path))
                        .await
                };

                // Retries once on authentication failure after reconnecting.
                if let Err(ref e) = list_result {
                    if super::client::Client::is_auth_error(e) {
                        client_guard
                            .reconnect()
                            .await
                            .map_err(|source| Error::ObjectStoreClient { source })?;

                        let context = client_guard
                            .context
                            .as_ref()
                            .ok_or_else(|| Error::NoObjectStoreContext)?;
                        list_result = context
                            .object_store
                            .list_with_delimiter(Some(&source_path))
                            .await;
                    }
                }

                let list_result = list_result.map_err(|source| Error::ObjectStore { source })?;
                list_result
                    .objects
                    .into_iter()
                    .map(|meta| meta.location)
                    .collect()
            };

            // Move each file.
            for source_location in files_to_move {
                let filename = source_location
                    .filename()
                    .unwrap_or_else(|| source_location.as_ref());
                let destination_location = destination_base.clone().join(filename);

                // Copy file
                let mut copy_result = {
                    let context = client_guard
                        .context
                        .as_ref()
                        .ok_or_else(|| Error::NoObjectStoreContext)?;
                    context
                        .object_store
                        .copy(&source_location, &destination_location)
                        .await
                };

                // Retries once on authentication failure after reconnecting.
                if let Err(ref e) = copy_result {
                    if super::client::Client::is_auth_error(e) {
                        client_guard
                            .reconnect()
                            .await
                            .map_err(|source| Error::ObjectStoreClient { source })?;

                        let context = client_guard
                            .context
                            .as_ref()
                            .ok_or_else(|| Error::NoObjectStoreContext)?;
                        copy_result = context
                            .object_store
                            .copy(&source_location, &destination_location)
                            .await;
                    }
                }

                // Handle copy result: if file already exists, it was moved in a previous attempt.
                // In this case, proceed to delete the source to complete the move operation.
                match copy_result {
                    Ok(_) => {}
                    Err(object_store::Error::AlreadyExists { .. }) => {}
                    Err(e) => return Err(Error::ObjectStore { source: e }),
                }

                // Delete source file
                let mut delete_result = {
                    let context = client_guard
                        .context
                        .as_ref()
                        .ok_or_else(|| Error::NoObjectStoreContext)?;
                    context.object_store.delete(&source_location).await
                };

                // Retries once on authentication failure after reconnecting.
                if let Err(ref e) = delete_result {
                    if super::client::Client::is_auth_error(e) {
                        client_guard
                            .reconnect()
                            .await
                            .map_err(|source| Error::ObjectStoreClient { source })?;

                        let context = client_guard
                            .context
                            .as_ref()
                            .ok_or_else(|| Error::NoObjectStoreContext)?;
                        delete_result = context.object_store.delete(&source_location).await;
                    }
                }

                delete_result.map_err(|source| Error::ObjectStore { source })?;

                files_moved += 1;
            }

            let move_result = MoveResult {
                source: if using_explicit_files {
                    format!("[{files_moved} files]")
                } else {
                    config
                        .source
                        .as_ref()
                        .ok_or(Error::MissingSource)?
                        .to_string_lossy()
                        .to_string()
                },
                destination: destination_str.to_string(),
                files_moved,
            };

            let event_data =
                EventData::Json(serde_json::to_value(&move_result).map_err(|source| {
                    Error::EventBuilder {
                        source: flowgen_core::event::Error::SerdeJson { source },
                    }
                })?);

            let mut e = EventBuilder::new()
                .subject(self.config.name.to_owned())
                .data(event_data)
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            // Signal completion or pass through to next task.
            match self.tx {
                None => {
                    // Final task, signal completion.
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        if let Ok(mut guard) = arc.lock() {
                            if let Some(tx) = guard.take() {
                                tx.send(Ok(e.data_as_json().ok())).ok();
                            }
                        }
                    }
                }
                Some(_) => {
                    // Pass through completion_tx to next task.
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

/// Object store move processor that processes events from a receiver.
#[derive(Debug)]
pub struct MoveProcessor {
    /// Move configuration settings.
    config: Arc<super::config::Processor>,
    /// Receiver for incoming events.
    rx: Receiver<Event>,
    /// Channel sender for processed events
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl Runner for MoveProcessor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the mover by establishing object store client connection.
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        // Extract bucket/container path for client initialization.
        // Glob characters cause invalid percent-encoding, so strip wildcards from source pattern.
        // When using explicit source_files, derive bucket from destination since both must share the same container.
        let client_path = if init_config.source_files.is_some() {
            init_config.destination.ok_or(Error::MissingDestination)?
        } else {
            let source = init_config.source.ok_or(Error::MissingSource)?;
            let path_str = source.to_string_lossy();
            let stripped = match path_str.split_once('*') {
                Some((before, _)) => before.trim_end_matches('/'),
                None => &path_str,
            };
            std::path::PathBuf::from(stripped)
        };

        let mut client_builder = super::client::ClientBuilder::new().path(client_path);

        if let Some(options) = &self.config.client_options {
            client_builder = client_builder.options(options.clone());
        }
        if let Some(credentials_path) = &self.config.credentials_path {
            client_builder = client_builder.credentials_path(credentials_path.clone());
        }

        let client = Arc::new(Mutex::new(
            client_builder
                .build()
                .map_err(|source| Error::ObjectStoreClient { source })?
                .connect()
                .await
                .map_err(|source| Error::ObjectStoreClient { source })?,
        ));

        let event_handler = EventHandler {
            client,
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize mover");
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

        // Process incoming events, filtering by task ID.
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
                                        error!(error = %e, "Failed to move objects");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Move failed after all retry attempts");
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

/// Builder pattern for constructing MoveProcessor instances.
#[derive(Default)]
pub struct MoveProcessorBuilder {
    /// Move configuration settings.
    config: Option<Arc<super::config::Processor>>,
    /// Receiver for incoming events.
    rx: Option<Receiver<Event>>,
    /// Event channel sender
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl MoveProcessorBuilder {
    pub fn new() -> MoveProcessorBuilder {
        MoveProcessorBuilder {
            ..Default::default()
        }
    }

    /// Sets the mover configuration.
    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the current task identifier.
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// Builds the MoveProcessor instance, validating required fields.
    pub async fn build(self) -> Result<MoveProcessor, Error> {
        Ok(MoveProcessor {
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

    #[test]
    fn test_move_result_serialization() {
        let result = MoveResult {
            source: "gs://bucket/source/*.parquet".to_string(),
            destination: "gs://bucket/archive/".to_string(),
            files_moved: 5,
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["source"], "gs://bucket/source/*.parquet");
        assert_eq!(json["destination"], "gs://bucket/archive/");
        assert_eq!(json["files_moved"], 5);
    }

    #[test]
    fn test_move_result_deserialization() {
        let json = serde_json::json!({
            "source": "s3://my-bucket/incoming/*.csv",
            "destination": "s3://my-bucket/processed/",
            "files_moved": 10
        });

        let result: MoveResult = serde_json::from_value(json).unwrap();
        assert_eq!(result.source, "s3://my-bucket/incoming/*.csv");
        assert_eq!(result.destination, "s3://my-bucket/processed/");
        assert_eq!(result.files_moved, 10);
    }

    #[test]
    fn test_move_result_zero_files() {
        let result = MoveResult {
            source: "gs://bucket/empty/*.txt".to_string(),
            destination: "gs://bucket/dest/".to_string(),
            files_moved: 0,
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["files_moved"], 0);

        let deserialized: MoveResult = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.files_moved, 0);
    }

    #[test]
    fn test_error_variants() {
        let err = Error::NoObjectStoreContext;
        assert!(matches!(err, Error::NoObjectStoreContext));

        let err = Error::MissingBuilderAttribute("config".to_string());
        assert!(matches!(err, Error::MissingBuilderAttribute(ref s) if s == "config"));

        let err = Error::MissingBuilderAttribute("receiver".to_string());
        assert!(matches!(err, Error::MissingBuilderAttribute(ref s) if s == "receiver"));
    }
}
