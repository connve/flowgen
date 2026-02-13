//! NATS JetStream Key-Value store cache implementation.

use flowgen_core::client::Client as FlowgenClientTrait;
use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("NATS client error: {source}")]
    ClientAuth {
        #[source]
        source: crate::client::Error,
    },
    #[error("KV entry access error: {source}")]
    KVEntry {
        #[source]
        source: async_nats::jetstream::kv::EntryError,
    },
    #[error("KV put error: {source}")]
    KVPut {
        #[source]
        source: async_nats::jetstream::kv::PutError,
    },
    #[error("KV delete error: {source}")]
    KVDelete {
        #[source]
        source: async_nats::jetstream::kv::UpdateError,
    },
    #[error("KV bucket creation error: {source}")]
    KVBucketCreate {
        #[source]
        source: async_nats::jetstream::context::CreateKeyValueError,
    },
    #[error("Missing required value KV Store")]
    MissingKVStore,
    #[error("Missing required value JetStream Context")]
    MissingJetStreamContext,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
}

/// NATS JetStream Key-Value store cache.
#[derive(Debug, Default)]
pub struct Cache {
    credentials_path: PathBuf,
    url: String,
    store: Option<async_nats::jetstream::kv::Store>,
}

impl Cache {
    /// Connects to NATS and initializes the KV bucket.
    pub async fn init(mut self, bucket: &str) -> Result<Self, Error> {
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

    async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>, flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Box::new(Error::MissingKVStore) as flowgen_core::cache::Error)?;
        store
            .get(key)
            .await
            .map_err(|e| Box::new(Error::KVEntry { source: e }) as flowgen_core::cache::Error)
    }

    async fn delete(&self, key: &str) -> Result<(), flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Box::new(Error::MissingKVStore) as flowgen_core::cache::Error)?;
        store
            .delete(key)
            .await
            .map_err(|e| Box::new(Error::KVDelete { source: e }) as flowgen_core::cache::Error)?;
        Ok(())
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
    /// * `Err(Error::MissingBuilderAttribute)` if `credentials_path` is missing.
    pub fn build(self) -> Result<Cache, Error> {
        Ok(Cache {
            credentials_path: self
                .credentials_path
                .ok_or_else(|| Error::MissingBuilderAttribute("credentials_path".to_string()))?,
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
            matches!(result.unwrap_err(), Error::MissingBuilderAttribute(attr) if attr == "credentials_path")
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
        let path = PathBuf::from("/test/creds.jwt");
        let cache = Cache {
            credentials_path: path.clone(),
            ..Default::default()
        };

        assert_eq!(cache.credentials_path, path);
        assert!(cache.store.is_none());
    }
}
