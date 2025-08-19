use super::config::{DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION};
use apache_avro::from_avro_datum;
use bytes::Bytes;
use chrono::{DateTime, Datelike, Utc};
use flowgen_core::connect::client::Client;
use flowgen_core::event::Event;
use object_store::PutPayload;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{event, Level};

/// Default subject prefix for logging messages.
const DEFAULT_MESSAGE_SUBJECT: &str = "object_store.writer.out";

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
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("could not initialize object store context")]
    NoObjectStoreContext(),
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
    async fn handle(self, event: Event) -> Result<(), Error> {
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
        let object_path = match &event.data {
            flowgen_core::event::EventData::ArrowRecordBatch(data) => {
                arrow::csv::WriterBuilder::new()
                    .with_header(true)
                    .build(&mut writer)
                    .write(data)?;
                object_store::path::Path::from(format!(
                    "{}.{}",
                    path.to_string_lossy(),
                    DEFAULT_CSV_EXTENSION
                ))
            }
            flowgen_core::event::EventData::Avro(data) => {
                let schema = apache_avro::Schema::parse_str(&data.schema)?;
                let value = from_avro_datum(&schema, &mut &data.raw_bytes[..], None)?;

                let mut writer = apache_avro::Writer::new(&schema, &mut writer);
                writer.append(value)?;
                writer.flush()?;
                object_store::path::Path::from(format!(
                    "{}.{}",
                    path.to_string_lossy(),
                    DEFAULT_AVRO_EXTENSION
                ))
            }
            flowgen_core::event::EventData::Json(data) => {
                serde_json::to_writer_pretty(&mut writer, data)?;
                object_store::path::Path::from(format!(
                    "{}.{}",
                    path.to_string_lossy(),
                    DEFAULT_JSON_EXTENSION
                ))
            }
        };
        // Upload processed data to object store.
        let payload = PutPayload::from_bytes(Bytes::from(writer));
        context.object_store.put(&object_path, payload).await?;

        let subject = format!("{DEFAULT_MESSAGE_SUBJECT}.{filename}");
        event!(Level::INFO, "Event processed: {}", subject);
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
pub struct Writer {
    /// Writer configuration settings.
    config: Arc<super::config::Writer>,
    /// Broadcast receiver for incoming events.
    rx: Receiver<Event>,
    /// Current task identifier for event filtering.
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;

    async fn run(mut self) -> Result<(), Self::Error> {
        // Build object store client with conditional configuration
        let mut client_builder = super::client::ClientBuilder::new().path(self.config.path.clone());

        if let Some(options) = &self.config.client_options {
            client_builder = client_builder.options(options.clone());
        }
        if let Some(credentials) = &self.config.credentials {
            client_builder = client_builder.credentials(credentials.to_path_buf());
        }

        let client = Arc::new(Mutex::new(client_builder.build()?.connect().await?));

        // Process incoming events, filtering by task ID.
        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let client = Arc::clone(&client);
                let config = Arc::clone(&self.config);
                let event_handler = EventHandler { client, config };
                tokio::spawn(async move {
                    if let Err(err) = event_handler.handle(event).await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
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
        })
    }
}
