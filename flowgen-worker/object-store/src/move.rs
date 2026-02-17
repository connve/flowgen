use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::{client::Client, task::runner::Runner};
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
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
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
    config: Arc<super::config::MoveProcessor>,
    /// Object store client for moving files.
    client: Arc<Mutex<super::client::Client>>,
    /// Channel sender for processed events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event and moves files in the configured object store.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        let event = Arc::new(event);
        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let mut client_guard = self.client.lock().await;
            let context = client_guard
                .context
                .as_mut()
                .ok_or_else(|| Error::NoObjectStoreContext)?;

            // Render config to support templates inside configuration.
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Parse source and destination paths.
            let source_str = config.source.to_string_lossy();
            let destination_str = config.destination.to_string_lossy();

            let source_url =
                url::Url::parse(&source_str).map_err(|source| Error::ParseUrl { source })?;
            let destination_url =
                url::Url::parse(&destination_str).map_err(|source| Error::ParseUrl { source })?;

            // Strip wildcard patterns from source path.
            let source_url_path = source_url.path();
            let source_prefix = if source_url_path.contains('*') {
                source_url_path
                    .split('*')
                    .next()
                    .unwrap_or(source_url_path)
                    .trim_end_matches('/')
            } else {
                source_url_path
            };

            let source_path = object_store::path::Path::from(source_prefix);
            let destination_base = object_store::path::Path::from(destination_url.path());

            // Use list_with_delimiter to avoid recursing into subdirectories.
            let list_result = context
                .object_store
                .list_with_delimiter(Some(&source_path))
                .await
                .map_err(|source| Error::ObjectStore { source })?;

            let mut files_moved = 0;

            for meta in list_result.objects {
                let source_location = &meta.location;

                let filename = source_location
                    .filename()
                    .unwrap_or_else(|| source_location.as_ref());
                let destination_location = destination_base.child(filename);

                context
                    .object_store
                    .copy(source_location, &destination_location)
                    .await
                    .map_err(|source| Error::ObjectStore { source })?;

                context
                    .object_store
                    .delete(source_location)
                    .await
                    .map_err(|source| Error::ObjectStore { source })?;

                files_moved += 1;
            }

            let move_result = MoveResult {
                source: source_str.to_string(),
                destination: destination_str.to_string(),
                files_moved,
            };

            let event_data =
                EventData::Json(serde_json::to_value(&move_result).map_err(|source| {
                    Error::EventBuilder {
                        source: flowgen_core::event::Error::SerdeJson { source },
                    }
                })?);

            let e = EventBuilder::new()
                .subject(self.config.name.to_owned())
                .data(event_data)
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

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
    config: Arc<super::config::MoveProcessor>,
    /// Receiver for incoming events.
    rx: Receiver<Event>,
    /// Channel sender for processed events
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl Runner for MoveProcessor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the mover by establishing object store client connection.
    async fn init(&self) -> Result<EventHandler, Error> {
        // Build object store client with conditional configuration.
        // For move operations, we use the source path for client initialization
        let mut client_builder =
            super::client::ClientBuilder::new().path(self.config.source.clone());

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
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

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
                error!(error = %e, "Mover failed after all retry attempts");
                return Ok(());
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
    config: Option<Arc<super::config::MoveProcessor>>,
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
    pub fn config(mut self, config: Arc<super::config::MoveProcessor>) -> Self {
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
            _task_context: self
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
