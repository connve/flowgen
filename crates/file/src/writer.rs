use chrono::Utc;
use flowgen_core::stream::event::Event;
use std::{fs::File, sync::Arc};
use tokio::sync::broadcast::Receiver;
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "file.writer";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error opening/creating file")]
    IO(#[source] std::io::Error),
    #[error("error deserializing data into binary format")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
    #[error("no filename in provided path")]
    EmptyFileName(),
    #[error("no value in provided str")]
    EmptyStr(),
}

pub struct Writer {
    config: Arc<super::config::Writer>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

/// Handles file writing and event processing.
struct EventHandler {
    /// Subscriber configuration.
    config: Arc<super::config::Writer>,
}

impl EventHandler {
    async fn run(self, event: Event) -> Result<(), Error> {
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
        let filename = format!("{}.{}.{}", file_stem, timestamp, file_ext);

        let file = File::create(filename).map_err(Error::IO)?;

        arrow::csv::WriterBuilder::new()
            .with_header(true)
            .build(file)
            .write(&event.data)
            .map_err(Error::Arrow)?;

        let subject = format!(
            "{}.{}.{}.{}",
            DEFAULT_MESSAGE_SUBJECT, file_stem, timestamp, file_ext
        );
        event!(Level::INFO, "event processed: {}", subject);

        Ok(())
    }
}
impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;
    async fn run(mut self) -> Result<(), Self::Error> {
        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let config = Arc::clone(&self.config);
                let event_handler = EventHandler { config };
                // Spawn a new asynchronous task to handle the event processing.
                // This allows the main loop to continue receiving new events
                // while existing events are being written concurrently.
                tokio::spawn(async move {
                    // Process the events and in case of error log it.
                    if let Err(err) = event_handler.run(event).await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
    }
}

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
