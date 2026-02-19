use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::{client::Client, task::runner::Runner};
use object_store::ObjectMeta;
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

/// Result of a list operation containing file paths and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResult {
    /// Path pattern that was listed.
    pub path: String,
    /// List of files found matching the path pattern.
    pub files: Vec<FileInfo>,
}

/// Information about a single file in object storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    /// Full path or location of the file.
    pub location: String,
    /// Last modified timestamp in ISO 8601 format.
    pub last_modified: String,
    /// File size in bytes.
    pub size: u64,
    /// E-Tag for the file if available.
    pub e_tag: Option<String>,
}

impl From<ObjectMeta> for FileInfo {
    fn from(meta: ObjectMeta) -> Self {
        FileInfo {
            location: meta.location.to_string(),
            last_modified: meta.last_modified.to_rfc3339(),
            size: meta.size,
            e_tag: meta.e_tag,
        }
    }
}

/// Handles processing of individual events by listing files in object storage.
pub struct EventHandler {
    /// List configuration settings.
    config: Arc<super::config::ListProcessor>,
    /// Object store client for listing files.
    client: Arc<Mutex<super::client::Client>>,
    /// Channel sender for processed events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event and lists files from the configured object store.
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

            // Parse the rendered path to extract directory prefix.
            // Strip wildcard patterns if present.
            let config_path_str = config.path.to_string_lossy();
            let url =
                url::Url::parse(&config_path_str).map_err(|source| Error::ParseUrl { source })?;

            let url_path = url.path();
            let prefix = if url_path.contains('*') {
                url_path
                    .split('*')
                    .next()
                    .unwrap_or(url_path)
                    .trim_end_matches('/')
            } else {
                url_path
            };

            let path = object_store::path::Path::from(prefix);

            // Use list_with_delimiter to avoid recursing into subdirectories.
            let list_result = context
                .object_store
                .list_with_delimiter(Some(&path))
                .await
                .map_err(|source| Error::ObjectStore { source })?;

            let files: Vec<FileInfo> = list_result
                .objects
                .into_iter()
                .map(FileInfo::from)
                .collect();

            let num_files = files.len();
            let list_result = ListResult {
                path: config_path_str.to_string(),
                files,
            };

            let event_data =
                EventData::Json(serde_json::to_value(&list_result).map_err(|source| {
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
                .context("num_files", num_files)
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

/// Object store list processor that processes events from a receiver.
#[derive(Debug)]
pub struct ListProcessor {
    /// List configuration settings.
    config: Arc<super::config::ListProcessor>,
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
impl Runner for ListProcessor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the lister by establishing object store client connection.
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        // Strip any glob pattern before building the client — the URL passed to
        // parse_url_opts must be valid, and glob characters cause percent-encoding
        // that breaks bucket/container name extraction in the object store backend.
        let client_path = {
            let path_str = init_config.path.to_string_lossy();
            let stripped = path_str
                .split('*')
                .next()
                .unwrap_or(&path_str)
                .trim_end_matches('/');
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
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize lister");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(error = %e, "Lister failed after all retry attempts");
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
                                        error!(error = %e, "Failed to list objects");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "List failed after all retry attempts");
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

/// Builder pattern for constructing ListProcessor instances.
#[derive(Default)]
pub struct ListProcessorBuilder {
    /// List configuration settings.
    config: Option<Arc<super::config::ListProcessor>>,
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

impl ListProcessorBuilder {
    pub fn new() -> ListProcessorBuilder {
        ListProcessorBuilder {
            ..Default::default()
        }
    }

    /// Sets the lister configuration.
    pub fn config(mut self, config: Arc<super::config::ListProcessor>) -> Self {
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

    /// Builds the ListProcessor instance, validating required fields.
    pub async fn build(self) -> Result<ListProcessor, Error> {
        Ok(ListProcessor {
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
    use chrono::{TimeZone, Utc};
    use object_store::path::Path;

    #[test]
    fn test_file_info_from_object_meta() {
        let last_modified = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 45).unwrap();
        let meta = ObjectMeta {
            location: Path::from("test/path/file.txt"),
            last_modified,
            size: 1024,
            e_tag: Some("abc123".to_string()),
            version: Some("v1".to_string()),
        };

        let file_info = FileInfo::from(meta);

        assert_eq!(file_info.location, "test/path/file.txt");
        assert_eq!(file_info.last_modified, "2024-01-15T10:30:45+00:00");
        assert_eq!(file_info.size, 1024);
        assert_eq!(file_info.e_tag, Some("abc123".to_string()));
    }

    #[test]
    fn test_file_info_from_object_meta_without_etag() {
        let last_modified = Utc.with_ymd_and_hms(2024, 2, 20, 15, 45, 30).unwrap();
        let meta = ObjectMeta {
            location: Path::from("bucket/data.parquet"),
            last_modified,
            size: 2048,
            e_tag: None,
            version: None,
        };

        let file_info = FileInfo::from(meta);

        assert_eq!(file_info.location, "bucket/data.parquet");
        assert_eq!(file_info.size, 2048);
        assert_eq!(file_info.e_tag, None);
    }

    #[test]
    fn test_list_result_serialization() {
        let files = vec![
            FileInfo {
                location: "gs://bucket/file1.txt".to_string(),
                last_modified: "2024-01-15T10:30:45+00:00".to_string(),
                size: 100,
                e_tag: Some("tag1".to_string()),
            },
            FileInfo {
                location: "gs://bucket/file2.txt".to_string(),
                last_modified: "2024-01-15T11:30:45+00:00".to_string(),
                size: 200,
                e_tag: None,
            },
        ];

        let result = ListResult {
            path: "gs://bucket/*.txt".to_string(),
            files: files.clone(),
        };

        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["path"], "gs://bucket/*.txt");
        assert_eq!(json["files"].as_array().unwrap().len(), 2);
        assert_eq!(json["files"][0]["location"], "gs://bucket/file1.txt");
        assert_eq!(json["files"][0]["size"], 100);
        assert_eq!(json["files"][1]["e_tag"], serde_json::Value::Null);
    }

    #[test]
    fn test_list_result_deserialization() {
        let json = serde_json::json!({
            "path": "gs://bucket/*.parquet",
            "files": [
                {
                    "location": "gs://bucket/data.parquet",
                    "last_modified": "2024-01-15T10:30:45+00:00",
                    "size": 1024,
                    "e_tag": "etag123"
                }
            ]
        });

        let result: ListResult = serde_json::from_value(json).unwrap();
        assert_eq!(result.path, "gs://bucket/*.parquet");
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0].location, "gs://bucket/data.parquet");
        assert_eq!(result.files[0].size, 1024);
        assert_eq!(result.files[0].e_tag, Some("etag123".to_string()));
    }

    #[test]
    fn test_error_variants() {
        let err = Error::NoObjectStoreContext;
        assert!(matches!(err, Error::NoObjectStoreContext));

        let err = Error::MissingBuilderAttribute("config".to_string());
        assert!(matches!(err, Error::MissingBuilderAttribute(ref s) if s == "config"));

        let err = Error::MissingBuilderAttribute("task_id".to_string());
        assert!(matches!(err, Error::MissingBuilderAttribute(ref s) if s == "task_id"));
    }
}
