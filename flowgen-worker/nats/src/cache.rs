//! NATS JetStream Key-Value store cache implementation.

use flowgen_core::client::Client as FlowgenClientTrait;
use std::path::PathBuf;
use std::time::Duration;

/// Number of historical entries retained per key in the KV bucket.
const DEFAULT_HISTORY: i64 = 10;

/// Default TTL for KV delete and purge tombstone markers.
///
/// This only applies to tombstones left behind after a key is deleted,
/// not to the actual values (those use the per-put TTL passed to `put`).
/// Setting this to any `Some(Duration)` enables per-message TTL on the
/// underlying JetStream stream (`allow_message_ttl`), which makes per-key
/// TTLs work at all. One hour is plenty for flowgen's usage — we do not
/// have watchers that need to observe delete events after long disconnects.
const DEFAULT_TOMBSTONE_TTL: Duration = Duration::from_secs(3600);

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
    #[error("JetStream publish error: {source}")]
    JetStreamPublish {
        #[source]
        source: async_nats::jetstream::context::PublishError,
    },
    #[error("JetStream publish acknowledgment error: {source}")]
    JetStreamPublishAck {
        #[source]
        source: async_nats::error::Error<async_nats::jetstream::context::PublishErrorKind>,
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
    history: Option<i64>,
    tombstone_ttl: Option<Duration>,
    store: Option<async_nats::jetstream::kv::Store>,
    jetstream: Option<async_nats::jetstream::Context>,
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
            Err(_) => {
                let history = self.history.unwrap_or(DEFAULT_HISTORY);

                // Try creating with limit_markers (enables per-key TTL, requires NATS 2.11+).
                // Falls back to creating without if the server rejects it.
                let with_ttl = jetstream
                    .create_key_value(async_nats::jetstream::kv::Config {
                        bucket: bucket.to_string(),
                        history,
                        limit_markers: Some(self.tombstone_ttl.unwrap_or(DEFAULT_TOMBSTONE_TTL)),
                        ..Default::default()
                    })
                    .await;

                match with_ttl {
                    Ok(store) => store,
                    Err(_) => {
                        tracing::warn!(
                            "NATS server does not support limit_markers (requires 2.11+), \
                             creating KV bucket without per-key TTL support."
                        );
                        jetstream
                            .create_key_value(async_nats::jetstream::kv::Config {
                                bucket: bucket.to_string(),
                                history,
                                ..Default::default()
                            })
                            .await
                            .map_err(|e| Error::KVBucketCreate { source: e })?
                    }
                }
            }
        };

        self.store = Some(store);
        self.jetstream = Some(jetstream);
        Ok(self)
    }
}

#[async_trait::async_trait]
impl flowgen_core::cache::Cache for Cache {
    async fn put(
        &self,
        key: &str,
        value: bytes::Bytes,
        ttl_secs: Option<u64>,
    ) -> Result<(), flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;

        let jetstream = self
            .jetstream
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;

        let subject = format!("{}{}", &store.prefix, key);

        let mut headers = async_nats::HeaderMap::new();
        if let Some(ttl) = ttl_secs {
            headers.insert(
                async_nats::header::NATS_MESSAGE_TTL,
                async_nats::HeaderValue::from(ttl),
            );
        }

        jetstream
            .publish_with_headers(subject, headers, value)
            .await
            .map_err(|source| {
                flowgen_core::cache::CacheError::PutFailed(Box::new(Error::JetStreamPublish {
                    source,
                }))
            })?
            .await
            .map_err(|source| {
                flowgen_core::cache::CacheError::PutFailed(Box::new(Error::JetStreamPublishAck {
                    source,
                }))
            })?;

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>, flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;
        store.get(key).await.map_err(|e| {
            flowgen_core::cache::CacheError::GetFailed(Box::new(Error::KVEntry { source: e }))
        })
    }

    async fn delete(&self, key: &str) -> Result<(), flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;
        store.delete(key).await.map_err(|e| {
            flowgen_core::cache::CacheError::DeleteFailed(Box::new(Error::KVDelete { source: e }))
        })?;
        Ok(())
    }

    async fn create(
        &self,
        key: &str,
        value: bytes::Bytes,
        ttl_secs: Option<u64>,
    ) -> Result<u64, flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;

        let jetstream = self
            .jetstream
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;

        let subject = format!("{}{}", &store.prefix, key);

        let mut headers = async_nats::HeaderMap::new();
        if let Some(ttl) = ttl_secs {
            headers.insert(
                async_nats::header::NATS_MESSAGE_TTL,
                async_nats::HeaderValue::from(ttl),
            );
        }
        headers.insert(
            "Nats-Expected-Last-Subject-Sequence",
            async_nats::HeaderValue::from(0u64),
        );

        let ack = jetstream
            .publish_with_headers(subject, headers, value)
            .await
            .map_err(|source| {
                flowgen_core::cache::CacheError::CreateFailed(Box::new(Error::JetStreamPublish {
                    source,
                }))
            })?
            .await
            .map_err(|source| match &source.kind() {
                async_nats::jetstream::context::PublishErrorKind::WrongLastSequence => {
                    flowgen_core::cache::CacheError::AlreadyExists
                }
                _ => flowgen_core::cache::CacheError::CreateFailed(Box::new(
                    Error::JetStreamPublishAck { source },
                )),
            })?;

        Ok(ack.sequence)
    }

    async fn update(
        &self,
        key: &str,
        value: bytes::Bytes,
        expected_revision: u64,
        ttl_secs: Option<u64>,
    ) -> Result<u64, flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;

        let jetstream = self
            .jetstream
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;

        let subject = format!("{}{}", &store.prefix, key);

        let mut headers = async_nats::HeaderMap::new();
        if let Some(ttl) = ttl_secs {
            headers.insert(
                async_nats::header::NATS_MESSAGE_TTL,
                async_nats::HeaderValue::from(ttl),
            );
        }
        headers.insert(
            "Nats-Expected-Last-Subject-Sequence",
            async_nats::HeaderValue::from(expected_revision),
        );

        let ack = jetstream
            .publish_with_headers(subject, headers, value)
            .await
            .map_err(|source| {
                flowgen_core::cache::CacheError::UpdateFailed(Box::new(Error::JetStreamPublish {
                    source,
                }))
            })?
            .await
            .map_err(|source| match &source.kind() {
                async_nats::jetstream::context::PublishErrorKind::WrongLastSequence => {
                    // For revision mismatch, we return 0 as actual since NATS doesn't provide
                    // the actual sequence in the error. Getting the actual revision would require
                    // an additional round-trip to NATS, which could fail or return stale data.
                    // The expected revision is sufficient for debugging most cases.
                    flowgen_core::cache::CacheError::RevisionMismatch {
                        expected: expected_revision,
                        actual: 0,
                    }
                }
                async_nats::jetstream::context::PublishErrorKind::StreamNotFound => {
                    flowgen_core::cache::CacheError::NotFound
                }
                _ => flowgen_core::cache::CacheError::UpdateFailed(Box::new(
                    Error::JetStreamPublishAck { source },
                )),
            })?;

        Ok(ack.sequence)
    }

    async fn get_with_revision(
        &self,
        key: &str,
    ) -> Result<Option<(bytes::Bytes, u64)>, flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;

        match store.entry(key).await {
            Ok(Some(entry)) if !entry.value.is_empty() => {
                let revision = entry.revision;
                let value = entry.value;
                Ok(Some((value, revision)))
            }
            Ok(Some(_)) | Ok(None) => Ok(None),
            Err(e) => Err(flowgen_core::cache::CacheError::GetFailed(Box::new(
                Error::KVEntry { source: e },
            ))),
        }
    }

    async fn delete_with_revision(
        &self,
        key: &str,
        expected_revision: u64,
    ) -> Result<(), flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;

        store
            .delete_expect_revision(key, Some(expected_revision))
            .await
            .map_err(|e| {
                flowgen_core::cache::CacheError::DeleteFailed(Box::new(Error::KVDelete {
                    source: e,
                }))
            })
    }

    async fn get_revision(&self, key: &str) -> Result<Option<u64>, flowgen_core::cache::Error> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)?;

        match store.entry(key).await {
            Ok(Some(entry)) => Ok(Some(entry.revision)),
            Ok(None) => Ok(None),
            Err(e) => Err(flowgen_core::cache::CacheError::GetFailed(Box::new(
                Error::KVEntry { source: e },
            ))),
        }
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
    /// Number of historical entries retained per key. Defaults to DEFAULT_HISTORY.
    history: Option<i64>,
    /// TTL for delete/purge tombstone markers. Defaults to DEFAULT_TOMBSTONE_TTL.
    tombstone_ttl: Option<Duration>,
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

    /// Sets the number of historical entries retained per key.
    ///
    /// Only applies when the bucket is created. Defaults to `DEFAULT_HISTORY` if unset.
    pub fn history(mut self, history: i64) -> Self {
        self.history = Some(history);
        self
    }

    /// Sets the TTL for delete/purge tombstone markers.
    ///
    /// Setting this also enables per-key TTL on cache entries. Defaults to
    /// `DEFAULT_TOMBSTONE_TTL` if unset.
    pub fn tombstone_ttl(mut self, ttl: Duration) -> Self {
        self.tombstone_ttl = Some(ttl);
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
            history: self.history,
            tombstone_ttl: self.tombstone_ttl,
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
