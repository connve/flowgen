//! NATS JetStream Key-Value store based cache implementation.
//! Includes [`Cache`], [`CacheBuilder`], and [`Error`].

use flowgen_core::client::Client as FlowgenClientTrait;
use std::path::PathBuf;

/// Errors during NATS-based cache interaction.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("NATS client authentication failed with error: {source}")]
    ClientAuth {
        #[source]
        source: crate::client::Error,
    },
    #[error("KV store entry access failed with error: {source}")]
    KVEntry {
        #[source]
        source: async_nats::jetstream::kv::EntryError,
    },
    #[error("KV store put operation failed with error: {source}")]
    KVPut {
        #[source]
        source: async_nats::jetstream::kv::PutError,
    },
    #[error("KV bucket creation failed with error: {source}")]
    KVBucketCreate {
        #[source]
        source: async_nats::jetstream::context::CreateKeyValueError,
    },
    #[error("No value in provided buffer")]
    EmptyBuffer,
    #[error("Missing required value KV Store")]
    MissingKVStore,
    #[error("Missing required value JetStream Context")]
    MissingJetStreamContext,
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

/// NATS JetStream Key-Value (KV) store cache.
///
/// Interact with a NATS KV bucket. Create with [`CacheBuilder`].
#[derive(Debug, Default)]
pub struct Cache {
    /// Path to NATS credentials file.
    credentials_path: PathBuf,
    /// NATS server URL. Defaults to "localhost:4222".
    url: String,
    /// NATS JetStream KV store instance; `None` until `init()`.
    store: Option<async_nats::jetstream::kv::Store>,
}

impl Cache {
    /// Connects to NATS and initializes the KV bucket.
    ///
    /// Consumes `self`, returns `Cache` with an active KV store connection.
    ///
    /// # Arguments
    /// * `bucket` - NATS JetStream KV bucket name.
    ///
    /// # Errors
    /// If NATS connection, authentication, or KV bucket access/creation fails.
    pub async fn init(mut self, bucket: &str) -> Result<Self, Error> {
        // Connect to NATS.
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.credentials_path.clone())
            .url(self.url.clone())
            .build()
            .map_err(|source| Error::ClientAuth { source })?
            .connect()
            .await
            .map_err(|source| Error::ClientAuth { source })?;

        let jetstream = client
            .jetstream
            .ok_or_else(|| Error::MissingJetStreamContext)?;

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
                .map_err(|e| Error::KVBucketCreate { source: e })?,
        };

        self.store = Some(store);
        Ok(self)
    }
}

#[async_trait::async_trait]
impl flowgen_core::cache::Cache for Cache {
    /// Puts a key-value pair into the NATS KV store.
    ///
    /// # Arguments
    /// * `key` - Key for the value.
    /// * `value` - Value to store.
    ///
    /// # Errors
    /// If store is uninitialized or NATS `put` fails.
    async fn put(&self, key: &str, value: bytes::Bytes) -> Result<(), flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Box::new(Error::MissingKVStore) as flowgen_core::cache::Error)?;
        store
            .put(key, value)
            .await
            .map_err(|e| Box::new(Error::KVPut { source: e }) as flowgen_core::cache::Error)?;
        Ok(())
    }

    /// Retrieves a value from the NATS KV store.
    ///
    /// # Arguments
    /// * `key` - Key of the value to retrieve.
    ///
    /// # Errors
    /// If store is uninitialized, NATS `get` fails, or key not found/value empty.
    async fn get(&self, key: &str) -> Result<bytes::Bytes, flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Box::new(Error::MissingKVStore) as flowgen_core::cache::Error)?;
        // Map Ok(None) (key not found/empty) from NATS to Error::EmptyBuffer.
        let bytes = store
            .get(key)
            .await
            .map_err(|e| Box::new(Error::KVEntry { source: e }) as flowgen_core::cache::Error)?
            .ok_or_else(|| Box::new(Error::EmptyBuffer) as flowgen_core::cache::Error)?;
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
    /// NATS server URL. Defaults to DEFAULT_NATS_URL if not set.
    url: Option<String>,
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
    /// * `path` - Path to the credentials file.
    pub fn credentials_path(mut self, path: PathBuf) -> Self {
        self.credentials_path = Some(path);
        self
    }

    /// Sets the NATS server URL.
    ///
    /// # Arguments
    /// * `url` - NATS server URL (e.g., "nats://localhost:4222" or "localhost:4222").
    pub fn url(mut self, url: String) -> Self {
        self.url = Some(url);
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
        Ok(Cache {
            credentials_path: self
                .credentials_path
                .ok_or_else(|| Error::MissingRequiredAttribute("credentials_path".to_string()))?,
            url: self
                .url
                .unwrap_or_else(|| crate::client::DEFAULT_NATS_URL.to_string()),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_cache_builder_new() {
        let builder = CacheBuilder::new();
        assert!(builder.credentials_path.is_none());
    }

    #[test]
    fn test_cache_builder_credentials_path() {
        let path = PathBuf::from("/path/to/creds.jwt");
        let builder = CacheBuilder::new().credentials_path(path.clone());
        assert_eq!(builder.credentials_path, Some(path));
    }

    #[test]
    fn test_cache_builder_build_missing_credentials() {
        let result = CacheBuilder::new().build();
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "credentials_path")
        );
    }

    #[test]
    fn test_cache_builder_build_success() {
        let path = PathBuf::from("/valid/path/creds.jwt");
        let result = CacheBuilder::new().credentials_path(path.clone()).build();

        assert!(result.is_ok());
        let cache = result.unwrap();
        assert_eq!(cache.credentials_path, path);
        assert!(cache.store.is_none());
    }

    #[test]
    fn test_cache_builder_chain() {
        let path = PathBuf::from("/chain/test/creds.jwt");
        let cache = CacheBuilder::new()
            .credentials_path(path.clone())
            .build()
            .unwrap();

        assert_eq!(cache.credentials_path, path);
    }

    #[test]
    fn test_cache_default() {
        let cache = Cache::default();
        assert_eq!(cache.credentials_path, PathBuf::new());
        assert!(cache.store.is_none());
    }

    #[test]
    fn test_cache_structure() {
        // Test that Cache can be constructed and has the expected structure
        let path = PathBuf::from("/test/creds.jwt");
        let cache = Cache {
            credentials_path: path.clone(),
            ..Default::default()
        };

        assert_eq!(cache.credentials_path, path);
        assert!(cache.store.is_none());
    }

    // Note: We cannot easily test the async methods (init, put, get) without
    // a real NATS server connection, but we can test the builder pattern
    // and error types which cover the main functionality
}
