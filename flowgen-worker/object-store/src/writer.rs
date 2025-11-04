use super::config::{DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION};
use bytes::Bytes;
use chrono::{DateTime, Datelike, Utc};
use flowgen_core::buffer::ToWriter;
use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, SenderExt};
use object_store::PutPayload;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{error, Instrument};

/// Status of an object store write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WriteStatus {
    /// Object was successfully written.
    Success,
    /// Write operation failed.
    Failed,
}

/// Result of a write operation to object storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResult {
    /// Status of the operation.
    pub status: WriteStatus,
    /// Path where the object was written.
    pub path: String,
    /// ETag of the uploaded object.
    pub e_tag: Option<String>,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Sending event to channel failed with error: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error("Writer event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("IO operation failed with error: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    #[error("Arrow operation failed with error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Avro operation failed with error: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    #[error("JSON serialization/deserialization failed with error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Object store operation failed with error: {source}")]
    ObjectStore {
        #[source]
        source: object_store::Error,
    },
    #[error("Object store client failed with error: {source}")]
    ObjectStoreClient {
        #[source]
        source: super::client::Error,
    },
    #[error("Configuration template rendering failed with error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Host coordination failed with error: {source}")]
    Host {
        #[source]
        source: flowgen_core::host::Error,
    },
    #[error("Invalid URL format with error: {source}")]
    ParseUrl {
        #[source]
        source: url::ParseError,
    },
    #[error("Could not initialize object store context")]
    NoObjectStoreContext,
    #[error("Missing required builder attribute: {0}")]
    MissingRequiredAttribute(String),
}

/// Handles processing of individual events by writing them to object storage.
pub struct EventHandler {
    /// Writer configuration settings.
    config: Arc<super::config::Writer>,
    /// Object store client for writing data.
    client: Arc<Mutex<super::client::Client>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Channel sender for response events.
    tx: tokio::sync::broadcast::Sender<Event>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event and writes it to the configured object store.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        let mut client_guard = self.client.lock().await;
        let context = client_guard
            .context
            .as_mut()
            .ok_or_else(|| Error::NoObjectStoreContext)?;

        // Render config with to support templates inside configuration.
        let event_value = serde_json::value::Value::try_from(&event)
            .map_err(|source| Error::EventBuilder { source })?;
        let config = self
            .config
            .render(&event_value)
            .map_err(|source| Error::ConfigRender { source })?;

        // Parse the rendered path to extract just the path part (not the URL scheme/bucket)
        let config_path_str = config.path.to_string_lossy();
        let url = url::Url::parse(&config_path_str).map_err(|source| Error::ParseUrl { source })?;
        let mut path = object_store::path::Path::from(url.path());

        let cd = Utc::now();
        if let Some(hive_options) = &self.config.hive_partition_options {
            if hive_options.enabled {
                for partition_key in &hive_options.partition_keys {
                    match partition_key {
                        crate::config::HiveParitionKeys::EventDate => {
                            let date_partition = self.format_date_partition(&cd);
                            // Split the date partition by '/' and add each part as a child
                            for part in date_partition.split('/') {
                                path = path.child(part);
                            }
                        }
                    }
                }
            }
        }

        let timestamp = cd.timestamp_micros();
        let filename = match event.id {
            Some(id) => id,
            _none => timestamp.to_string(),
        };

        let extension = match &event.data {
            flowgen_core::event::EventData::ArrowRecordBatch(_) => DEFAULT_CSV_EXTENSION,
            flowgen_core::event::EventData::Avro(_) => DEFAULT_AVRO_EXTENSION,
            flowgen_core::event::EventData::Json(_) => DEFAULT_JSON_EXTENSION,
        };

        // Transform the event data to writer.
        let mut writer = Vec::new();
        event
            .data
            .to_writer(&mut writer)
            .map_err(|source| Error::EventBuilder { source })?;

        let object_path = path.child(format!("{filename}.{extension}"));

        // Upload processed data to object store.
        let payload = PutPayload::from_bytes(Bytes::from(writer));
        let put_result = context
            .object_store
            .put(&object_path, payload)
            .await
            .map_err(|e| Error::ObjectStore { source: e })?;

        let result = WriteResult {
            status: WriteStatus::Success,
            path: object_path.to_string(),
            e_tag: put_result.e_tag.clone(),
        };

        // Build and send event.
        let data = serde_json::to_value(&result).map_err(|e| Error::SerdeJson { source: e })?;
        let mut e = EventBuilder::new()
            .subject(self.config.name.to_owned())
            .data(EventData::Json(data))
            .task_id(self.task_id)
            .task_type(self.task_type);

        if let Some(e_tag) = put_result.e_tag {
            e = e.id(e_tag);
        };

        let e = e.build().map_err(|source| Error::EventBuilder { source })?;

        self.tx
            .send_with_logging(e)
            .map_err(|source| Error::SendMessage { source })?;

        Ok(())
    }

    /// Formats date into Hive partition format (year=YYYY/month=MM/day=DD).
    fn format_date_partition(&self, date: &DateTime<Utc>) -> String {
        format!(
            "year={}/month={}/day={}",
            date.year(),
            date.month(),
            date.day()
        )
    }
}

/// Object store writer that processes events from a broadcast receiver.
#[derive(Debug)]
pub struct Writer {
    /// Writer configuration settings.
    config: Arc<super::config::Writer>,
    /// Broadcast receiver for incoming events.
    rx: Receiver<Event>,
    /// Channel sender for response events.
    tx: tokio::sync::broadcast::Sender<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the writer by establishing object store client connection.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Building and connecting the object store client with credentials
    async fn init(&self) -> Result<EventHandler, Error> {
        // Build object store client with conditional configuration.
        let mut client_builder = super::client::ClientBuilder::new().path(self.config.path.clone());

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
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        // Initialize runner task.
        let event_handler = match self.init().await {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!("{}", e);
                return Ok(());
            }
        };

        // Process incoming events, filtering by task ID.
        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    tokio::spawn(
                        async move {
                            if let Err(err) = event_handler.handle(event).await {
                                error!("{}", err);
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

/// Builder pattern for constructing Writer instances.
#[derive(Default)]
pub struct WriterBuilder {
    /// Writer configuration settings.
    config: Option<Arc<super::config::Writer>>,
    /// Broadcast receiver for incoming events.
    rx: Option<Receiver<Event>>,
    /// Channel sender for response events.
    tx: Option<tokio::sync::broadcast::Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl WriterBuilder {
    pub fn new() -> WriterBuilder {
        WriterBuilder {
            ..Default::default()
        }
    }

    /// Sets the writer configuration.
    pub fn config(mut self, config: Arc<super::config::Writer>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender.
    pub fn sender(mut self, sender: tokio::sync::broadcast::Sender<Event>) -> Self {
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

    /// Builds the Writer instance, validating required fields.
    pub async fn build(self) -> Result<Writer, Error> {
        Ok(Writer {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            task_id: self.task_id,
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value};
    use std::path::PathBuf;
    use tokio::sync::broadcast;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(flowgen_core::task::manager::TaskManagerBuilder::new().build());
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_writer_builder() {
        let config = Arc::new(crate::config::Writer {
            name: "test_writer".to_string(),
            path: PathBuf::from("s3://bucket/path/"),
            credentials_path: None,
            client_options: None,
            hive_partition_options: None,
        });
        let (tx, rx) = broadcast::channel::<Event>(10);

        // Success case.
        let writer = WriterBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(writer.is_ok());

        // Error case - missing config.
        let (_tx2, rx2) = broadcast::channel::<Event>(10);
        let result = WriterBuilder::new()
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingRequiredAttribute(_)
        ));
    }
}
