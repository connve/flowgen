//! NATS JetStream Key-Value store based cache implementation.
//! Includes [`Cache`], [`CacheBuilder`], and [`Error`].

use flowgen_core::connect::client::Client as FlowgenClientTrait;
use std::path::PathBuf;

/// Errors during NATS-based cache interaction.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// NATS client authentication or connection error (transparently wraps `crate::client::Error`).
    #[error(transparent)]
    NatsClientAuth(#[from] crate::client::Error),
    /// NATS KV store entry access/processing error (transparently wraps `async_nats::jetstream::kv::EntryError`).
    #[error(transparent)]
    NatsKVEntry(#[from] async_nats::jetstream::kv::EntryError),
    /// NATS KV store `put` operation error (transparently wraps `async_nats::jetstream::kv::PutError`).
    #[error(transparent)]
    NatsKVPut(#[from] async_nats::jetstream::kv::PutError),
    /// NATS KV bucket creation error (transparently wraps `async_nats::jetstream::context::CreateKeyValueError`).
    #[error(transparent)]
    NatsKVBucketCreate(#[from] async_nats::jetstream::context::CreateKeyValueError),
    /// Expected non-empty buffer from KV store was empty or missing.
    #[error("no value in provided buffer")]
    EmptyBuffer(),
    /// NATS KV store not initialized or unexpectedly `None`.
    #[error("missing required value Nats KV Store")]
    MissingNatsKVStore(),
    /// Required configuration attribute missing. Contains attribute name.
    #[error("missing required event attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// NATS JetStream context was missing or unavailable.
    #[error("missing required value Nats Jetstream Context")]
    MissingNatsJetstreamContext(),
}

/// NATS JetStream Key-Value (KV) store cache.
///
/// Interact with a NATS KV bucket. Create with [`CacheBuilder`].
#[derive(Debug, Default)]
pub struct Cache {
    /// Path to NATS credentials file.
    credentials_path: PathBuf,
    /// NATS JetStream KV store instance; `None` until `init()`.
    store: Option<async_nats::jetstream::kv::Store>,
}

impl flowgen_core::cache::Cache for Cache {
    type Error = Error;

    /// Connects to NATS and initializes the KV bucket.
    ///
    /// Consumes `self`, returns `Cache` with an active KV store connection.
    ///
    /// # Arguments
    /// * `bucket` - NATS JetStream KV bucket name.
    ///
    /// # Errors
    /// If NATS connection, authentication, or KV bucket access/creation fails.
    async fn init(mut self, bucket: &str) -> Result<Self, Self::Error> {
        // Connect to NATS.
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.credentials_path.clone())
            .build()
            .map_err(Error::NatsClientAuth)?
            .connect()
            .await
            .map_err(Error::NatsClientAuth)?;

        let jetstream = client
            .jetstream
            .ok_or(Error::MissingNatsJetstreamContext())?;

        // Get or create KV store.
        let store = match jetstream.get_key_value(bucket).await {
            Ok(store) => store,
            Err(_) => jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: bucket.to_string(),
                    history: 10,
                    ..Default::default()
                })
                .await
                .map_err(Error::NatsKVBucketCreate)?,
        };

        self.store = Some(store);
        Ok(self)
    }

    /// Puts a key-value pair into the NATS KV store.
    ///
    /// # Arguments
    /// * `key` - Key for the value.
    /// * `value` - Value to store.
    ///
    /// # Errors
    /// If store is uninitialized or NATS `put` fails.
    async fn put(&self, key: &str, value: bytes::Bytes) -> Result<(), Self::Error> {
        let store = self.store.as_ref().ok_or(Error::MissingNatsKVStore())?;
        store.put(key, value).await.map_err(Error::NatsKVPut)?;
        Ok(())
    }

    /// Retrieves a value from the NATS KV store.
    ///
    /// # Arguments
    /// * `key` - Key of the value to retrieve.
    ///
    /// # Errors
    /// If store is uninitialized, NATS `get` fails, or key not found/value empty.
    async fn get(&self, key: &str) -> Result<bytes::Bytes, Self::Error> {
        let store = self.store.as_ref().ok_or(Error::MissingNatsKVStore())?;
        // Map Ok(None) (key not found/empty) from NATS to Error::EmptyBuffer.
        let bytes = store
            .get(key)
            .await
            .map_err(Error::NatsKVEntry)?
            .ok_or(Error::EmptyBuffer())?;
        Ok(bytes)
    }
}

/// Builder for [`Cache`] instances.
///
/// Allows step-by-step `Cache` configuration.
#[derive(Default)]
pub struct CacheBuilder {
    /// Optional path to NATS credentials.
    credentials_path: Option<PathBuf>,
}

impl CacheBuilder {
    /// Creates a new, empty `CacheBuilder`.
    pub fn new() -> CacheBuilder {
        CacheBuilder {
            ..Default::default()
        }
    }

    /// Sets the NATS credentials file path.
    ///
    /// Used for NATS client authentication.
    ///
    /// # Arguments
    /// * `credentials_path` - Path to the credentials file.
    pub fn credentials_path(mut self, credentials_path: PathBuf) -> Self {
        self.credentials_path = Some(credentials_path);
        self
    }

    /// Builds the [`Cache`].
    ///
    /// Consumes builder. `Cache` is returned unconnected; call `init()` to connect.
    ///
    /// # Returns
    /// * `Ok(Cache)` on success.
    /// * `Err(Error::MissingRequiredAttribute)` if `credentials_path` is missing.
    pub fn build(self) -> Result<Cache, Error> {
        let creds_path = self
            .credentials_path
            .ok_or_else(|| Error::MissingRequiredAttribute("credentials".to_string()))?;

        Ok(Cache {
            credentials_path: creds_path,
            store: None,
        })
    }
}
