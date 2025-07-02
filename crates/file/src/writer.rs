//! # File Writer Module
//!
//! This module provides asynchronous file writing capabilities with event-driven processing.
//! It receives events containing RecordBatch data and writes them to CSV files with timestamped filenames.
//!
//! ## Core Components:
//! - `Writer`: The main runner struct that implements `flowgen_core::task::runner::Runner`.
//!   It listens for `Event`s on a channel and spawns `EventHandler` tasks for file writing.
//! - `EventHandler`: Processes individual events, performing the actual write operation
//!   to CSV files with timestamped filenames based on the provided configuration.
//! - `WriterBuilder`: A builder pattern implementation for constructing `Writer` instances.
//! - `Error`: An enum defining all possible errors that can occur within this module.
//!
//! ## Workflow:
//! 1. A `Writer` instance is created using `WriterBuilder`, configured with necessary
//!    parameters like output file path and an event channel receiver.
//! 2. The `Writer::run` method is called (typically by the `flowgen_core` task execution framework).
//! 3. `run` enters a loop, receiving `Event`s from the channel.
//! 4. For each valid event (matching `current_task_id`), it spawns a new asynchronous task
//!    using `tokio::spawn` that runs `EventHandler::run`.
//! 5. `EventHandler::run` creates a timestamped filename, writes the RecordBatch data to a CSV file,
//!    and logs the successful operation.
//! 6. Errors during file operations or event processing are logged using the `tracing` crate.

use chrono::Utc;
use flowgen_core::stream::event::Event;
use std::{fs::File, sync::Arc};
use tokio::sync::broadcast::Receiver;
use tracing::{event, Level};

/// Default subject prefix for file writer events.
const DEFAULT_MESSAGE_SUBJECT: &str = "file.writer";

/// Errors that can occur during the file writing process.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Error originating from I/O operations (file creation, writing, etc.).
    #[error("error opening/creating file")]
    IO(#[source] std::io::Error),
    /// Error originating from the Apache Arrow crate (CSV writing, data serialization).
    #[error("error deserializing data into binary format")]
    Arrow(#[source] arrow::error::ArrowError),
    /// An expected attribute or configuration value was missing.
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
    /// Could not extract a filename from the configured file path.
    #[error("no filename in provided path")]
    EmptyFileName(),
    /// An expected string value was empty (e.g., filename conversion).
    #[error("no value in provided str")]
    EmptyStr(),
}

/// The main asynchronous runner for writing events to CSV files.
///
/// Implements the `flowgen_core::task::runner::Runner` trait. It listens for
/// incoming `Event`s on a broadcast channel receiver (`rx`) and spawns
/// `EventHandler` tasks to perform the actual file writing operations.
/// Each event is written to a timestamped CSV file.
pub struct Writer {
    /// Thread-safe reference to the writer configuration settings.
    config: Arc<super::config::Writer>,
    /// Receiver end of the broadcast channel for incoming events.
    rx: Receiver<Event>,
    /// The ID assigned to this writer task, used for filtering events.
    current_task_id: usize,
}

/// Handles the processing logic for a single file writing operation.
///
/// An `EventHandler` instance is created for each event that needs to be written to a file.
/// It holds a reference to the writer configuration necessary to perform the file writing operation.
struct EventHandler {
    /// Thread-safe reference to the writer configuration settings.
    config: Arc<super::config::Writer>,
}

impl EventHandler {
    /// Processes a single event by writing its data to a timestamped CSV file.
    ///
    /// This method extracts the filename components from the configuration,
    /// generates a unique timestamped filename, creates the CSV file,
    /// and writes the event's RecordBatch data to it.
    ///
    /// # Arguments
    /// * `event` - The `Event` containing the RecordBatch data to be written.
    ///
    /// # Returns
    /// * `Ok(())` if the event was processed and written successfully.
    /// * `Err(Error)` if any error occurred during processing or writing.
    async fn run(self, event: Event) -> Result<(), Error> {
        // Extract filename stem and extension from the configured path.
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

        // Generate a unique timestamped filename.
        let timestamp = Utc::now().timestamp_micros();
        let filename = format!("{}.{}.{}", file_stem, timestamp, file_ext);

        // Create the output file and write the RecordBatch data as CSV.
        let file = File::create(filename).map_err(Error::IO)?;

        arrow::csv::WriterBuilder::new()
            .with_header(true)
            .build(file)
            .write(&event.data)
            .map_err(Error::Arrow)?;

        // Generate event subject and log successful processing.
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
    
    /// Executes the main loop of the writer task.
    ///
    /// 1. Enters a loop, receiving `Event`s from the `rx` channel.
    /// 2. Filters events based on `event.current_task_id` to ensure it processes
    ///    events intended for the previous task in the pipeline (`current_task_id - 1`).
    /// 3. For valid events, clones shared resources (`config`) and spawns an
    ///    `EventHandler` task via `tokio::spawn` to process the file writing.
    /// 4. Logs errors encountered during event processing.
    ///
    /// # Returns
    /// * `Ok(())` if the loop terminates gracefully (e.g., channel closes).
    /// * `Err(Self::Error)` if an unrecoverable error occurs.
    async fn run(mut self) -> Result<(), Self::Error> {
        // Process incoming events and spawn file writing tasks.
        while let Ok(event) = self.rx.recv().await {
            // Process events where `current_task_id` matches the *previous* task's ID.
            // This assumes tasks are chained, and this writer processes output from task `N-1`.
            if event.current_task_id == Some(self.current_task_id - 1) {
                // Setup thread-safe reference to the configuration.
                let config = Arc::clone(&self.config);
                let event_handler = EventHandler { config };
                
                // Spawn a new asynchronous task to handle the file writing.
                // This allows the main loop to continue receiving new events
                // while existing events are being written concurrently.
                tokio::spawn(async move {
                    // Process the file writing and log any errors that occur.
                    if let Err(err) = event_handler.run(event).await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
    }
}

/// Builder for creating [`Writer`] instances.
///
/// Provides an API for setting the required configuration and
/// channel receiver before constructing the `Writer`.
#[derive(Default)]
pub struct WriterBuilder {
    /// Optional writer configuration containing file path and processing settings.
    config: Option<Arc<super::config::Writer>>,
    /// Optional event receiver for incoming events to be written.
    rx: Option<Receiver<Event>>,
    /// Task identifier for event processing and filtering.
    current_task_id: usize,
}

impl WriterBuilder {
    /// Creates a new `WriterBuilder` with default values.
    pub fn new() -> WriterBuilder {
        WriterBuilder {
            ..Default::default()
        }
    }

    /// Sets the writer configuration.
    ///
    /// # Arguments
    /// * `config` - An `Arc` containing the `super::config::Writer` configuration.
    pub fn config(mut self, config: Arc<super::config::Writer>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the broadcast channel receiver for incoming events.
    ///
    /// # Arguments
    /// * `receiver` - The `Receiver<Event>` end of the broadcast channel.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the ID for this writer task.
    ///
    /// This ID is used to filter incoming events in the `Writer::run` method,
    /// typically processing events whose `current_task_id` matches `this_task_id - 1`.
    ///
    /// # Arguments
    /// * `current_task_id` - The unique identifier assigned to this task instance.
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    /// Builds the `Writer` instance.
    ///
    /// Consumes the builder and returns a `Writer` if all required fields have been set.
    ///
    /// # Returns
    /// * `Ok(Writer)` if construction is successful.
    /// * `Err(Error::MissingRequiredAttribute)` if any required field was not provided.
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
