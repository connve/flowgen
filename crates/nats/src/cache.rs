use chrono::Utc;
use flowgen_core::{connect::client::Client as FlowgenClientTrait, stream::event::Event};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
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
pub enum Error {
    /// Error occurring when joining Tokio tasks.
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    /// Error originating from the Delta Lake client setup or connection (`super::client`).
    #[error(transparent)]
    Client(#[from] super::client::Error),
    /// An expected attribute or configuration value was missing.
    #[error("missing required event attrubute")]
    // Note: "attrubute" typo exists in original code
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

#[derive(Debug, Default)]
pub struct Cache {
    credentials: PathBuf,
}

impl flowgen_core::cache::Cache for Cache {
    type Error = Error;

    async fn put(&self) -> Result<(), Self::Error>
    where
        Self: Sized,
    {
        todo!()
    }
}

#[derive(Default)]
pub struct CacheBuilder {
    credentials: Option<PathBuf>,
}

impl CacheBuilder {
    /// Creates a new `CacheBuilder` with default values.
    pub fn new() -> CacheBuilder {
        CacheBuilder {
            ..Default::default()
        }
    }

    /// Sets the broadcast channel receiver for incoming events.
    ///
    /// # Arguments
    /// * `receiver` - The `Receiver<Event>` end of the broadcast channel.
    pub fn credentials(mut self, credentials: PathBuf) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Builds the `Cache` instance.
    ///
    /// Consumes the builder and returns a `Writer` if all required fields (`config`, `rx`)
    /// have been set.
    ///
    /// # Returns
    /// * `Ok(Writer)` if construction is successful.
    /// * `Err(Error::MissingRequiredAttribute)` if `config` or `rx` was not provided.
    pub fn build(self) -> Result<Cache, Error> {
        Ok(Cache {
            credentials: self
                .credentials
                .ok_or_else(|| Error::MissingRequiredAttribute("credentials".to_string()))?,
        })
    }
}
