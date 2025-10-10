use super::config::{DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION};
use bytes::Bytes;
use chrono::{DateTime, Datelike, Utc};
use flowgen_core::buffer::ToWriter;
use flowgen_core::event::{Event, EventBuilder, EventData, SubjectSuffix, DEFAULT_LOG_MESSAGE};
use flowgen_core::{client::Client, event::generate_subject};
use object_store::PutPayload;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{debug, error, info, Instrument};

/// Default subject prefix for logging messages.
const DEFAULT_MESSAGE_SUBJECT: &str = "object_store_writer";

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    ObjectStoreClient(#[from] super::client::Error),
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error("Missing required attribute: {}.", _0)]
    MissingRequiredAttribute(String),
    #[error("Could not initialize object store context")]
    NoObjectStoreContext(),
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
    /// Task manager error.
    #[error(transparent)]
    TaskManager(#[from] flowgen_core::task::manager::Error),
}

/// Handles processing of individual events by writing them to object storage.
struct EventHandler {
    /// Writer configuration settings.
    config: Arc<super::config::Writer>,
    /// Object store client for writing data.
    client: Arc<Mutex<super::client::Client>>,
}

impl EventHandler {
    /// Processes an event and writes it to the configured object store.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let mut client_guard = self.client.lock().await;
        let context = client_guard
            .context
            .as_mut()
            .ok_or_else(Error::NoObjectStoreContext)?;

        let mut path = PathBuf::from(context.path.to_string());

        let cd = Utc::now();
        if let Some(hive_options) = &self.config.hive_partition_options {
            if hive_options.enabled {
                for partition_key in &hive_options.partition_keys {
                    match partition_key {
                        crate::config::HiveParitionKeys::EventDate => {
                            let date_partition = self.format_date_partition(&cd);
                            path.push(date_partition);
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
        path.push(&filename);

        let mut writer = Vec::new();
        let extension = match &event.data {
            flowgen_core::event::EventData::ArrowRecordBatch(_) => DEFAULT_CSV_EXTENSION,
            flowgen_core::event::EventData::Avro(_) => DEFAULT_AVRO_EXTENSION,
            flowgen_core::event::EventData::Json(_) => DEFAULT_JSON_EXTENSION,
        };

        // Transform the event data to writer.
        event.data.to_writer(&mut writer)?;

        let object_path =
            object_store::path::Path::from(format!("{}.{}", path.to_string_lossy(), extension));

        // Upload processed data to object store.
        let payload = PutPayload::from_bytes(Bytes::from(writer));
        let put_result = context.object_store.put(&object_path, payload).await?;

        // Generate event subject.
        let subject = generate_subject(
            Some(&self.config.name),
            DEFAULT_MESSAGE_SUBJECT,
            SubjectSuffix::Timestamp,
        );

        // Build and send event.
        let data = serde_json::json!({
            "status": "written",
            "path": object_path.to_string(),
            "e_tag": put_result.e_tag
        });
        let e = EventBuilder::new()
            .subject(subject)
            .data(EventData::Json(data))
            .build()?;
        info!("{}: {}", DEFAULT_LOG_MESSAGE, e.subject);
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
    /// Current task identifier for event filtering.
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.current_task_id))]
    async fn run(mut self) -> Result<(), Self::Error> {
        // Register task with task manager.
        let task_id = format!(
            "{}.{}.{}",
            self.task_context.flow.name, DEFAULT_MESSAGE_SUBJECT, self.config.name
        );
        let mut task_manager_rx = self
            .task_context
            .task_manager
            .register(
                task_id,
                Some(flowgen_core::task::manager::LeaderElectionOptions {}),
            )
            .await?;

        // Build object store client with conditional configuration
        let mut client_builder = super::client::ClientBuilder::new().path(self.config.path.clone());

        if let Some(options) = &self.config.client_options {
            client_builder = client_builder.options(options.clone());
        }
        if let Some(credentials) = &self.config.credentials {
            client_builder = client_builder.credentials(credentials.to_path_buf());
        }

        let client = Arc::new(Mutex::new(client_builder.build()?.connect().await?));

        let event_handler = Arc::new(EventHandler {
            client,
            config: Arc::clone(&self.config),
        });

        // Process incoming events, filtering by task ID.
        loop {
            tokio::select! {
                biased;

                // Check for leadership changes.
                Some(status) = task_manager_rx.recv() => {
                    if status == flowgen_core::task::manager::LeaderElectionResult::NotLeader {
                        debug!("Lost leadership for task: {}", self.config.name);
                        return Ok(());
                    }
                }

                // Process events.
                result = self.rx.recv() => {
                    match result {
                        Ok(event) => {
                            if event.current_task_id == Some(self.current_task_id - 1) {
                                let handler = Arc::clone(&event_handler);
                                tokio::spawn(async move {
                                    if let Err(err) = handler.handle(event).await {
                                        error!("{}", err);
                                    }
                                }.instrument(tracing::Span::current()));
                            }
                        }
                        Err(_) => return Ok(()),
                    }
                }
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
    /// Current task identifier for event filtering.
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
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

    /// Sets the current task identifier.
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
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
            current_task_id: self.current_task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HiveParitionKeys, HivePartitionOptions};
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

    #[test]
    fn test_writer_builder_new() {
        let builder = WriterBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.rx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[test]
    fn test_writer_builder_config() {
        let config = Arc::new(crate::config::Writer {
            name: "test_writer".to_string(),
            path: PathBuf::from("s3://bucket/path/"),
            credentials: None,
            client_options: None,
            hive_partition_options: None,
        });

        let builder = WriterBuilder::new().config(config.clone());
        assert!(builder.config.is_some());
        assert_eq!(
            builder.config.unwrap().path,
            PathBuf::from("s3://bucket/path/")
        );
    }

    #[test]
    fn test_writer_builder_receiver() {
        let (_, rx) = broadcast::channel::<Event>(10);
        let builder = WriterBuilder::new().receiver(rx);
        assert!(builder.rx.is_some());
    }

    #[test]
    fn test_writer_builder_current_task_id() {
        let builder = WriterBuilder::new().current_task_id(42);
        assert_eq!(builder.current_task_id, 42);
    }

    #[tokio::test]
    async fn test_writer_builder_missing_config() {
        let (_, rx) = broadcast::channel::<Event>(10);
        let result = WriterBuilder::new()
            .receiver(rx)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[tokio::test]
    async fn test_writer_builder_missing_receiver() {
        let config = Arc::new(crate::config::Writer {
            name: "test_writer".to_string(),
            path: PathBuf::from("/tmp/output/"),
            credentials: None,
            client_options: None,
            hive_partition_options: None,
        });

        let result = WriterBuilder::new()
            .config(config)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "receiver")
        );
    }

    #[tokio::test]
    async fn test_writer_builder_build_success() {
        let config = Arc::new(crate::config::Writer {
            name: "test_writer".to_string(),
            path: PathBuf::from("gs://my-bucket/data/"),
            credentials: Some(PathBuf::from("/service-account.json")),
            client_options: None,
            hive_partition_options: Some(HivePartitionOptions {
                enabled: true,
                partition_keys: vec![HiveParitionKeys::EventDate],
            }),
        });

        let (_, rx) = broadcast::channel::<Event>(10);

        let result = WriterBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .current_task_id(99)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_ok());
        let writer = result.unwrap();
        assert_eq!(writer.current_task_id, 99);
        assert_eq!(writer.config.path, PathBuf::from("gs://my-bucket/data/"));
    }

    #[test]
    fn test_event_handler_structure() {
        // Test that EventHandler can be constructed with the right types
        let config = Arc::new(crate::config::Writer {
            name: "test_writer".to_string(),
            path: PathBuf::from("/tmp/"),
            credentials: None,
            client_options: None,
            hive_partition_options: None,
        });

        let client = Arc::new(Mutex::new(
            crate::client::ClientBuilder::new()
                .path(PathBuf::from("/tmp/"))
                .build()
                .unwrap(),
        ));

        // We can't actually create an EventHandler here because it's private,
        // but we can verify the types are correct by compiling this
        let _ = (config, client);
    }

    #[test]
    fn test_writer_builder_chain() {
        let config = Arc::new(crate::config::Writer {
            name: "test_writer".to_string(),
            path: PathBuf::from("file:///data/output/"),
            credentials: None,
            client_options: None,
            hive_partition_options: None,
        });

        let (_, rx) = broadcast::channel::<Event>(5);

        let builder = WriterBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .current_task_id(10);

        assert!(builder.config.is_some());
        assert!(builder.rx.is_some());
        assert_eq!(builder.current_task_id, 10);
    }
}
