//! # Delta Lake Writer Module.
//!
//! This module provides the core functionality for writing data streams asynchronously
//! into a Delta Lake table. It acts as a task runner within the `flowgen_core` framework.
//!
//! ## Core Components:
//! - `Writer`: The main runner struct that implements `flowgen_core::task::runner::Runner`.
//!   It listens for `Event`s on a channel, manages a `Client` connection to Delta Lake,
//!   and spawns `EventHandler` tasks.
//! - `EventHandler`: Processes individual `Event`s, performing the actual write
//!   (Append or Merge) operation to the Delta table based on the provided configuration.
//! - `WriterBuilder`: A builder pattern implementation for constructing `Writer` instances.
//! - `Error`: An enum defining all possible errors that can occur within this module.
//!
//! ## Workflow:
//! 1. A `Writer` instance is created using `WriterBuilder`, configured with necessary
//!    parameters like Delta table path, credentials, write operation, and an event channel receiver.
//! 2. The `Writer::run` method is called (typically by the `flowgen_core` task execution framework).
//! 3. `run` establishes a connection to the Delta Lake table using `super::client::Client`.
//! 4. It enters a loop, receiving `Event`s from the channel.
//! 5. For each valid event (matching `current_task_id`), it spawns a new asynchronous task
//!    using `tokio::spawn` that runs `EventHandler::process`.
//! 6. `EventHandler::process` acquires a lock on the shared `Client`, prepares the Delta Lake
//!    operation (Append or Merge using DataFusion), executes it, and logs the outcome.
//! 7. Errors during client connection or event processing are logged using the `tracing` crate.

use crate::{event::EventExt, schema::SchemaExt};
use chrono::Utc;
use deltalake::{
    arrow::datatypes::Schema,
    datafusion::{
        common::Column,
        datasource::MemTable,
        prelude::{Expr, SessionContext},
    },
    kernel::{StructField, StructType},
    parquet::{
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    },
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps,
};
use flowgen_core::{cache::Cache, connect::client::Client as FlowgenClientTrait, event::Event};
use std::sync::Arc;
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{error, event, Level};

/// Base subject used for logging successful event processing messages.
const DEFAULT_MESSAGE_SUBJECT: &str = "deltalake.writer";
/// Default alias for the target DeltaTable in MERGE operations.
const DEFAULT_TARGET_ALIAS: &str = "target";
/// Default alias for the source data (in-memory table) in MERGE operations.
const DEFAULT_SOURCE_ALIAS: &str = "source";

/// Errors that can occur during the Delta Lake writing process.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Error originating from the schema extension crate.
    #[error(transparent)]
    Schema(#[from] super::schema::Error),
    /// Error originating when converting String to Serde JSON.
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    /// Error originating when converting String from Utf8.
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),
    /// Error originating from the event extension crate.
    #[error(transparent)]
    Event(#[from] super::event::Error),
    /// Error originating from the event extension crate.
    #[error("cache errors")]
    Cache(),
    /// Error originating from the Apache Arrow crate.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// Error originating from the Parquet library used by Delta Lake.
    #[error(transparent)]
    Parquet(#[from] deltalake::parquet::errors::ParquetError),
    /// Error originating from the Delta Lake table operations.
    #[error(transparent)]
    DeltaTable(#[from] deltalake::DeltaTableError),
    /// Error occurring when joining Tokio tasks.
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    /// Error originating from the Delta Lake client setup or connection (`super::client`).
    #[error(transparent)]
    Client(#[from] super::client::Error),
    /// Error originating from the DataFusion query engine, used for MERGE operations.
    #[error(transparent)]
    DataFusion(#[from] deltalake::datafusion::error::DataFusionError),
    /// An expected attribute or configuration value was missing.
    #[error("missing required event attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// The required `path` configuration for the Delta table was not provided.
    #[error("missing required config value path")]
    MissingPath(),
    /// Internal error: The DeltaTable reference was unexpectedly missing.
    #[error("missing required value DeltaTable")]
    MissingDeltaTable(),
    /// Could not extract a filename from the configured Delta table path.
    #[error("no filename in provided path")]
    EmptyFileName(),
    /// An expected string value was empty (e.g., filename conversion).
    #[error("no value in provided str")]
    EmptyStr(),
}

/// Handles the processing logic for a single incoming `Event`.
///
/// An `EventHandler` instance is created for each event that needs to be written.
/// It holds references to the shared `Client` and `Writer` configuration
/// necessary to perform the write operation.
struct EventHandler {
    /// Thread-safe reference to the Delta Lake client.
    client: Arc<Mutex<super::client::Client>>,
    /// Thread-safe reference to the writer configuration.
    config: Arc<super::config::Writer>,
}

impl EventHandler {
    /// Processes a single event by writing its data to the configured Delta Lake table.
    ///
    /// This method acquires a lock on the shared client, determines the operation
    /// (Append or Merge) based on the configuration, executes the corresponding
    /// Delta Lake operation, and logs the result. It consumes the `EventHandler` instance.
    ///
    /// # Arguments
    /// * `event` - The `Event` containing the data (`RecordBatch`) to be written.
    ///
    /// # Returns
    /// * `Ok(())` if the event was processed and written successfully.
    /// * `Err(Error)` if any error occurred during processing or writing.
    async fn run(self, mut event: Event) -> Result<(), Error> {
        // Lock the client and setup necessary components i.e. table, operations context,
        // writer properties etc.
        let mut client = self.client.lock().await;
        let table = client.table.as_mut().ok_or_else(Error::MissingDeltaTable)?;
        let ops = DeltaOps::from(table.clone());
        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(3).map_err(Error::Parquet)?,
            ))
            .build();

        // Deltatable supports Timestamp with Microseconds only.
        // We call this helper function to do event data adjustment.
        event.normalize().map_err(Error::Event)?;

        // Match on operation and Append or Merge source data (event) into the target table.
        // match self.config.operation {
        //     crate::config::Operation::Append => {
        //         let mut writer = RecordBatchWriter::for_table(table)
        //             .map_err(Error::DeltaTable)?
        //             .with_writer_properties(writer_properties);

        //         writer.write(event.data).await.map_err(Error::DeltaTable)?;
        //         writer
        //             .flush_and_commit(table)
        //             .await
        //             .map_err(Error::DeltaTable)?;
        //     }
        //     crate::config::Operation::Merge => {
        //         let ctx = SessionContext::new();
        //         let schema = event.data.schema();
        //         let provider = MemTable::try_new(schema.clone(), vec![vec![event.data]])
        //             .map_err(Error::DataFusion)?;
        //         ctx.register_table(DEFAULT_SOURCE_ALIAS, Arc::new(provider))
        //             .map_err(Error::DataFusion)?;
        //         let df = ctx
        //             .table(DEFAULT_SOURCE_ALIAS)
        //             .await
        //             .map_err(Error::DataFusion)?;

        //         let predicate = self
        //             .config
        //             .predicate
        //             .clone()
        //             .ok_or_else(|| Error::MissingRequiredAttribute("predicate".to_string()))?;

        //         let column_names: Vec<String> = schema
        //             .fields()
        //             .iter()
        //             .map(|field| field.name().clone())
        //             .collect();

        //         ops.merge(df, predicate)
        //             .with_source_alias(DEFAULT_SOURCE_ALIAS)
        //             .with_target_alias(DEFAULT_TARGET_ALIAS)
        //             .with_writer_properties(writer_properties)
        //             .when_not_matched_insert(|insert| {
        //                 column_names.iter().fold(insert, |acc, col_name| {
        //                     let qualified_col =
        //                         Expr::Column(Column::new(Some(DEFAULT_SOURCE_ALIAS), col_name));
        //                     acc.set(col_name, qualified_col)
        //                 })
        //             })?
        //             .with_streaming(true)
        //             .await
        //             .map_err(Error::DeltaTable)?;
        //     }
        // }

        // Setup logging elements and log info in case of sucessfull table operation.
        let file_stem = self
            .config
            .path
            .file_stem()
            .ok_or_else(Error::EmptyFileName)?
            .to_str()
            .ok_or_else(Error::EmptyStr)?
            .trim();

        let timestamp = Utc::now().timestamp_micros();
        let subject = format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, file_stem, timestamp);
        event!(Level::INFO, "{}", format!("event processed: {}", subject));
        Ok(())
    }
}

/// The main asynchronous runner for writing events to a Delta Lake table.
///
/// Implements the `flowgen_core::task::runner::Runner` trait. It manages
/// the connection to the Delta table and listens for incoming `Event`s on a
/// broadcast channel receiver (`rx`). For each valid event, it spawns an
/// `EventHandler` task to perform the actual write operation.
#[derive(Debug)]
pub struct Writer<T: Cache> {
    /// Shared configuration for the writer task.
    config: Arc<super::config::Writer>,
    /// Receiver end of the broadcast channel for incoming events.
    rx: Receiver<Event>,
    /// The ID assigned to this writer task, used for filtering events.
    current_task_id: usize,
    /// The cache store used to put / retrieve schema etc.
    cache: Arc<T>,
}

impl<T: Cache> flowgen_core::task::runner::Runner for Writer<T> {
    type Error = Error;

    /// Executes the main loop of the writer task.
    ///
    /// 1. Initializes the Delta Lake client (`super::client::Client`).
    /// 2. Enters a loop, receiving `Event`s from the `rx` channel.
    /// 3. Filters events based on `event.current_task_id` to ensure it processes
    ///    events intended for the previous task in the pipeline (`current_task_id - 1`).
    /// 4. For valid events, clones shared resources (`client`, `config`) and
    ///    spawns an `EventHandler` task via `tokio::spawn` to process the event.
    /// 5. Logs errors encountered during client connection or event processing.
    ///
    /// # Returns
    /// * `Ok(())` if the loop terminates gracefully (e.g., channel closes).
    /// * `Err(Error)` if client initialization fails.
    async fn run(mut self) -> Result<(), Error> {
        // Build a DeltaTable client with credentials and storage path.
        // Return thread-safe reference to use in the downstream components.
        let mut columns: Vec<StructField> = Vec::new();
        if let Some(cache_options) = &self.config.create_options.cache_options {
            if let Some(key) = &cache_options.retrieve_key {
                let schema_bytes = self.cache.get(key).await.map_err(|_| Error::Cache())?;
                let schema_str =
                    String::from_utf8(schema_bytes.to_vec()).map_err(Error::FromUtf8)?;

                let schema: Schema = serde_json::from_str(&schema_str).map_err(Error::SerdeJson)?;
                let new_schema = schema.normalize().map_err(Error::Schema)?;
                let delta_schema = StructType::try_from(&new_schema).map_err(Error::Arrow)?;

                let struct_fields: Vec<StructField> = delta_schema.fields().cloned().collect();
                columns = struct_fields;
            }
        };

        let client = match super::client::ClientBuilder::new()
            .credentials(self.config.credentials.clone())
            .path(self.config.path.clone())
            .columns(columns)
            .create_if_not_exist()
            .build()
        {
            Ok(client) => match client.connect().await {
                Ok(client) => Arc::new(Mutex::new(client)),
                Err(err) => {
                    event!(Level::ERROR, "{}", err);
                    return Err(Error::Client(err));
                }
            },
            Err(err) => {
                event!(Level::ERROR, "{}", err);
                return Err(Error::Client(err));
            }
        };

        // Receive events, validate if the event should be processed.
        while let Ok(event) = self.rx.recv().await {
            // Process events where `current_task_id` matches the *previous* task's ID.
            // This assumes tasks are chained, and this writer processes output from task `N-1`.
            if event.current_task_id == Some(self.current_task_id - 1) {
                // Setup thread-safe reference to variables and pass to EventHandler.
                let client = Arc::clone(&client);
                let config = Arc::clone(&self.config);
                let event_handler = EventHandler { client, config };

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

/// Builder for creating [`Writer`] instances.
///
/// Provides an API for setting the required configuration and
/// channel receiver before constructing the `Writer`.
#[derive(Default)]
pub struct WriterBuilder<T: Cache> {
    config: Option<Arc<super::config::Writer>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
    cache: Option<Arc<T>>,
}

impl<T: Cache> WriterBuilder<T>
where
    T: Default,
{
    /// Creates a new `WriterBuilder` with default values.
    pub fn new() -> WriterBuilder<T> {
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

    pub fn cache(mut self, cache: Arc<T>) -> Self {
        self.cache = Some(cache);
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
    /// Consumes the builder and returns a `Writer` if all required fields (`config`, `rx`)
    /// have been set.
    ///
    /// # Returns
    /// * `Ok(Writer)` if construction is successful.
    /// * `Err(Error::MissingRequiredAttribute)` if `config` or `rx` was not provided.
    pub fn build(self) -> Result<Writer<T>, Error> {
        Ok(Writer {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            cache: self
                .cache
                .ok_or_else(|| Error::MissingRequiredAttribute("cache".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
