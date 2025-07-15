//! Asynchronous file writing with timestamped output files.
//!
//! Receives events containing Arrow RecordBatch or Avro data and writes them
//! to CSV files with automatically generated timestamps.
use apache_avro::from_avro_datum;
use chrono::Utc;
use flowgen_core::stream::event::Event;
use std::{fs::File, sync::Arc};
use tokio::sync::broadcast::Receiver;
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "file.writer";

/// File writing errors.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
    #[error("no filename in provided path")]
    EmptyFileName(),
    #[error("no value in provided str")]
    EmptyStr(),
}

/// Event-driven file writer with timestamped output.
pub struct Writer {
    config: Arc<super::config::Writer>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

/// Processes individual file write operations.
struct EventHandler {
    config: Arc<super::config::Writer>,
}

impl EventHandler {
    /// Writes event data to timestamped CSV file.
    async fn handle(self, event: Event) -> Result<(), Error> {
        let file_stem = self
            .config
            .path
            .file_stem()
            .ok_or_else(Error::EmptyFileName)?
            .to_str()
            .ok_or_else(Error::EmptyStr)?;

        let file_ext = self
            .config
            .path
            .extension()
            .ok_or_else(Error::EmptyFileName)?
            .to_str()
            .ok_or_else(Error::EmptyStr)?;

        let timestamp = Utc::now().timestamp_micros();
        let filename = format!("{file_stem}.{timestamp}.{file_ext}");
        let file = File::create(filename).map_err(Error::IO)?;

        match &event.data {
            flowgen_core::stream::event::EventData::ArrowRecordBatch(data) => {
                arrow::csv::WriterBuilder::new()
                    .with_header(true)
                    .build(file)
                    .write(data)
                    .map_err(Error::Arrow)?;
            }
            flowgen_core::stream::event::EventData::Avro(data) => {
                let schema = apache_avro::Schema::parse_str(&data.schema).map_err(Error::Avro)?;
                let value = from_avro_datum(&schema, &mut &data.raw_bytes[..], None)
                    .map_err(Error::Avro)?;
                let mut writer = apache_avro::Writer::new(&schema, file);
                writer.append(value).map_err(Error::Avro)?;
                writer.flush().map_err(Error::Avro)?;
            }
        }

        let subject = format!("{DEFAULT_MESSAGE_SUBJECT}.{file_stem}.{timestamp}.{file_ext}");
        event!(Level::INFO, "event processed: {}", subject);
        Ok(())
    }
}
impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;

    /// Processes incoming events and spawns write tasks.
    async fn run(mut self) -> Result<(), Self::Error> {
        while let Ok(event) = self.rx.recv().await {
            // Process events from previous task only.
            if event.current_task_id == Some(self.current_task_id - 1) {
                let config = Arc::clone(&self.config);
                let event_handler = EventHandler { config };
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

/// Builder for Writer instances.
#[derive(Default)]
pub struct WriterBuilder {
    config: Option<Arc<super::config::Writer>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl WriterBuilder {
    pub fn new() -> WriterBuilder {
        WriterBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Writer>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

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
