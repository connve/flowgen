//! NATS JetStream Key-Value store cache implementation.

use async_nats::jetstream::context::CreateKeyValueErrorKind;
use flowgen_core::client::Client as _;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

/// Number of historical entries retained per key in the KV bucket.
/// NATS server caps this at 64 for KV buckets.
const DEFAULT_HISTORY: i64 = 64;

/// Default TTL for KV delete and purge tombstone markers.
///
/// This only applies to tombstones left behind after a key is deleted,
/// not to the actual values (those use the per-put TTL passed to `put`).
/// Setting this to any `Some(Duration)` enables per-message TTL on the
/// underlying JetStream stream (`allow_message_ttl`), which makes per-key
/// TTLs work at all. One hour is plenty for flowgen's usage — we do not
/// have watchers that need to observe delete events after long disconnects.
pub(crate) const DEFAULT_TOMBSTONE_TTL: Duration = Duration::from_secs(3600);

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
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
    #[error("KV keys listing error: {source}")]
    KVKeys {
        #[source]
        source: async_nats::error::Error<async_nats::jetstream::kv::WatchErrorKind>,
    },
    #[error("KV watch subscription error: {source}")]
    KVWatch {
        #[source]
        source: async_nats::error::Error<async_nats::jetstream::kv::WatchErrorKind>,
    },
    #[error("KV watch stream error: {source}")]
    KVWatchStream {
        #[source]
        source: async_nats::error::Error<async_nats::jetstream::kv::WatcherErrorKind>,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
}

/// Whether `error` or one of its sources says the stream behind a KV bucket does not exist.
pub(crate) fn stream_missing(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if says_stream_missing(error) {
            return true;
        }
        current = error.source();
    }
    false
}

/// Whether `error` itself is "no responders" for the bucket's subjects or a
/// JetStream "stream not found".
fn says_stream_missing(error: &(dyn std::error::Error + 'static)) -> bool {
    use async_nats::jetstream::context::{
        PublishError, PublishErrorKind, RequestError, RequestErrorKind,
    };
    use async_nats::jetstream::stream::{
        ConsumerError, ConsumerErrorKind, DirectGetError, DirectGetErrorKind,
    };
    use async_nats::jetstream::ErrorCode;

    if let Some(e) = error.downcast_ref::<PublishError>() {
        return e.kind() == PublishErrorKind::StreamNotFound;
    }
    if let Some(e) = error.downcast_ref::<DirectGetError>() {
        return matches!(e.kind(), DirectGetErrorKind::ErrorResponse(status, _) if status == async_nats::StatusCode::NO_RESPONDERS);
    }
    if let Some(e) = error.downcast_ref::<RequestError>() {
        return e.kind() == RequestErrorKind::NoResponders;
    }
    if let Some(e) = error.downcast_ref::<async_nats::RequestError>() {
        return e.kind() == async_nats::RequestErrorKind::NoResponders;
    }
    if let Some(e) = error.downcast_ref::<ConsumerError>() {
        return matches!(e.kind(), ConsumerErrorKind::JetStream(source) if source.error_code() == ErrorCode::STREAM_NOT_FOUND);
    }
    match error.downcast_ref::<async_nats::jetstream::Error>() {
        Some(e) => e.error_code() == ErrorCode::STREAM_NOT_FOUND,
        None => false,
    }
}

/// NATS JetStream Key-Value store cache.
#[derive(Debug, Default)]
pub struct Cache {
    credentials_path: Option<PathBuf>,
    url: String,
    history: Option<i64>,
    tombstone_ttl: Option<Duration>,
    bucket: String,
    store: Option<async_nats::jetstream::kv::Store>,
    jetstream: Option<async_nats::jetstream::Context>,
    /// Lets one operation at a time recreate a missing bucket.
    recovery: tokio::sync::Mutex<()>,
}

impl Cache {
    /// Connects to NATS and initializes the KV bucket.
    pub async fn init(mut self, bucket: &str) -> Result<Self, Error> {
        let mut builder = crate::client::ClientBuilder::new();
        builder.url(self.url.clone());
        if let Some(path) = self.credentials_path.clone() {
            builder.credentials_path(path);
        }
        let client = builder
            .build()
            .map_err(|source| Error::ClientAuth { source })?
            .connect()
            .await
            .map_err(|source| Error::ClientAuth { source })?;

        let jetstream = client
            .jetstream
            .ok_or_else(|| Error::MissingJetStreamContext)?;

        self.bucket = bucket.to_string();
        self.store = Some(self.open_bucket(&jetstream).await?);
        self.jetstream = Some(jetstream);
        Ok(self)
    }

    /// Opens the bucket, creating it when it does not exist.
    async fn open_bucket(
        &self,
        jetstream: &async_nats::jetstream::Context,
    ) -> Result<async_nats::jetstream::kv::Store, Error> {
        if let Ok(store) = jetstream.get_key_value(&self.bucket).await {
            return Ok(store);
        }
        let history = match self.history {
            Some(history) => history,
            None => DEFAULT_HISTORY,
        };
        let tombstone_ttl = match self.tombstone_ttl {
            Some(tombstone_ttl) => tombstone_ttl,
            None => DEFAULT_TOMBSTONE_TTL,
        };
        let with_ttl = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: self.bucket.clone(),
                history,
                limit_markers: Some(tombstone_ttl),
                ..Default::default()
            })
            .await;

        match with_ttl {
            Ok(store) => Ok(store),
            Err(e) if e.kind() != CreateKeyValueErrorKind::LimitMarkersNotSupported => {
                Err(Error::KVBucketCreate { source: e })
            }
            Err(_) => {
                tracing::warn!(
                    "NATS server does not support limit_markers (requires 2.11+), \
                     creating KV bucket without per-key TTL support"
                );
                jetstream
                    .create_key_value(async_nats::jetstream::kv::Config {
                        bucket: self.bucket.clone(),
                        history,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| Error::KVBucketCreate { source: e })
            }
        }
    }

    /// Runs `op` and, if the bucket is gone, recreates it and runs `op` again.
    async fn recovering<T, F, Fut>(&self, op: F) -> Result<T, flowgen_core::cache::Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, flowgen_core::cache::Error>>,
    {
        let error = match op().await {
            Err(error) if stream_missing(&error) => error,
            result => return result,
        };
        let jetstream = self.jetstream()?;
        let _recovering = self.recovery.lock().await;
        tracing::warn!(bucket = %self.bucket, "KV bucket is missing, recreating it");
        if let Err(source) = self.open_bucket(jetstream).await {
            tracing::warn!(bucket = %self.bucket, error = %source, "Failed to recreate KV bucket");
            return Err(error);
        }
        op().await
    }

    fn store(&self) -> Result<&async_nats::jetstream::kv::Store, flowgen_core::cache::Error> {
        self.store
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)
    }

    fn jetstream(&self) -> Result<&async_nats::jetstream::Context, flowgen_core::cache::Error> {
        self.jetstream
            .as_ref()
            .ok_or_else(|| flowgen_core::cache::CacheError::StoreNotInitialized)
    }

    async fn put_once(
        &self,
        key: &str,
        value: bytes::Bytes,
        ttl_secs: Option<u64>,
    ) -> Result<(), flowgen_core::cache::Error> {
        let store = self.store()?;
        let jetstream = self.jetstream()?;

        let subject = format!("{}{}", store.prefix, key);

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

    async fn get_once(
        &self,
        key: &str,
    ) -> Result<Option<bytes::Bytes>, flowgen_core::cache::Error> {
        self.store()?.get(key).await.map_err(|e| {
            flowgen_core::cache::CacheError::GetFailed(Box::new(Error::KVEntry { source: e }))
        })
    }

    async fn delete_once(&self, key: &str) -> Result<(), flowgen_core::cache::Error> {
        self.store()?.delete(key).await.map_err(|e| {
            flowgen_core::cache::CacheError::DeleteFailed(Box::new(Error::KVDelete { source: e }))
        })
    }

    async fn create_once(
        &self,
        key: &str,
        value: bytes::Bytes,
        ttl_secs: Option<u64>,
    ) -> Result<u64, flowgen_core::cache::Error> {
        let store = self.store()?;
        let jetstream = self.jetstream()?;

        let subject = format!("{}{}", store.prefix, key);

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

    async fn update_once(
        &self,
        key: &str,
        value: bytes::Bytes,
        expected_revision: u64,
        ttl_secs: Option<u64>,
    ) -> Result<u64, flowgen_core::cache::Error> {
        let store = self.store()?;
        let jetstream = self.jetstream()?;

        let subject = format!("{}{}", store.prefix, key);

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
                _ => flowgen_core::cache::CacheError::UpdateFailed(Box::new(
                    Error::JetStreamPublishAck { source },
                )),
            })?;

        Ok(ack.sequence)
    }

    async fn get_with_revision_once(
        &self,
        key: &str,
    ) -> Result<Option<(bytes::Bytes, u64)>, flowgen_core::cache::Error> {
        match self.store()?.entry(key).await {
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

    async fn delete_with_revision_once(
        &self,
        key: &str,
        expected_revision: u64,
    ) -> Result<(), flowgen_core::cache::Error> {
        self.store()?
            .delete_expect_revision(key, Some(expected_revision))
            .await
            .map_err(|e| match e.kind() {
                async_nats::jetstream::kv::UpdateErrorKind::WrongLastRevision => {
                    flowgen_core::cache::CacheError::RevisionMismatch {
                        expected: expected_revision,
                        actual: 0,
                    }
                }
                _ => flowgen_core::cache::CacheError::DeleteFailed(Box::new(Error::KVDelete {
                    source: e,
                })),
            })
    }

    async fn get_revision_once(
        &self,
        key: &str,
    ) -> Result<Option<u64>, flowgen_core::cache::Error> {
        match self.store()?.entry(key).await {
            Ok(Some(entry)) => Ok(Some(entry.revision)),
            Ok(None) => Ok(None),
            Err(e) => Err(flowgen_core::cache::CacheError::GetFailed(Box::new(
                Error::KVEntry { source: e },
            ))),
        }
    }

    async fn list_keys_once(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, flowgen_core::cache::Error> {
        use futures_util::StreamExt;
        let mut keys = Vec::new();
        let mut key_stream = self.store()?.keys().await.map_err(|e| {
            flowgen_core::cache::CacheError::GetFailed(Box::new(Error::KVKeys { source: e }))
        })?;
        while let Some(result) = key_stream.next().await {
            if let Ok(key) = result {
                if key.starts_with(prefix) {
                    keys.push(key);
                }
            }
        }
        Ok(keys)
    }

    async fn watch_once(
        &self,
        prefix: &str,
        include_history: bool,
    ) -> Result<
        futures_util::stream::BoxStream<
            'static,
            Result<flowgen_core::cache::WatchEvent, flowgen_core::cache::Error>,
        >,
        flowgen_core::cache::Error,
    > {
        use async_nats::jetstream::kv::Operation;
        use futures_util::StreamExt;

        let store = self.store()?.clone();

        // Append the NATS wildcard so the watch covers all keys under the prefix.
        let subject = format!("{prefix}.>");
        let watcher = match include_history {
            true => store.watch_with_history(subject).await,
            false => store.watch(subject).await,
        }
        .map_err(|e| {
            flowgen_core::cache::CacheError::WatchFailed(Box::new(Error::KVWatch { source: e }))
        })?;

        let stream = watcher.map(|result| match result {
            Err(e) => Err(flowgen_core::cache::CacheError::WatchFailed(Box::new(
                Error::KVWatchStream { source: e },
            ))),
            Ok(entry) => match entry.operation {
                Operation::Put => Ok(flowgen_core::cache::WatchEvent::Put {
                    key: entry.key,
                    value: entry.value,
                }),
                Operation::Delete | Operation::Purge => {
                    Ok(flowgen_core::cache::WatchEvent::Delete { key: entry.key })
                }
            },
        });

        Ok(stream.boxed())
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
        self.recovering(|| self.put_once(key, value.clone(), ttl_secs))
            .await
    }

    async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>, flowgen_core::cache::Error> {
        self.recovering(|| self.get_once(key)).await
    }

    async fn delete(&self, key: &str) -> Result<(), flowgen_core::cache::Error> {
        self.recovering(|| self.delete_once(key)).await
    }

    async fn create(
        &self,
        key: &str,
        value: bytes::Bytes,
        ttl_secs: Option<u64>,
    ) -> Result<u64, flowgen_core::cache::Error> {
        self.recovering(|| self.create_once(key, value.clone(), ttl_secs))
            .await
    }

    async fn update(
        &self,
        key: &str,
        value: bytes::Bytes,
        expected_revision: u64,
        ttl_secs: Option<u64>,
    ) -> Result<u64, flowgen_core::cache::Error> {
        match self
            .recovering(|| self.update_once(key, value.clone(), expected_revision, ttl_secs))
            .await
        {
            Err(error) if stream_missing(&error) => Err(flowgen_core::cache::CacheError::NotFound),
            result => result,
        }
    }

    async fn get_with_revision(
        &self,
        key: &str,
    ) -> Result<Option<(bytes::Bytes, u64)>, flowgen_core::cache::Error> {
        self.recovering(|| self.get_with_revision_once(key)).await
    }

    async fn delete_with_revision(
        &self,
        key: &str,
        expected_revision: u64,
    ) -> Result<(), flowgen_core::cache::Error> {
        self.recovering(|| self.delete_with_revision_once(key, expected_revision))
            .await
    }

    async fn get_revision(&self, key: &str) -> Result<Option<u64>, flowgen_core::cache::Error> {
        self.recovering(|| self.get_revision_once(key)).await
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, flowgen_core::cache::Error> {
        self.recovering(|| self.list_keys_once(prefix)).await
    }

    /// Subscribes to key changes under the given prefix.
    ///
    /// When `include_history` is true, replays retained per-key history (up to
    /// the bucket's `history` setting) before switching to live updates —
    /// enabling consumers to reconstruct recent state on connect.
    async fn watch(
        &self,
        prefix: &str,
        include_history: bool,
    ) -> Result<
        futures_util::stream::BoxStream<
            'static,
            Result<flowgen_core::cache::WatchEvent, flowgen_core::cache::Error>,
        >,
        flowgen_core::cache::Error,
    > {
        self.recovering(|| self.watch_once(prefix, include_history))
            .await
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

    /// Builds the [`Cache`]. Cache is returned unconnected; call
    /// `init()` to connect.
    pub fn build(self) -> Result<Cache, Error> {
        Ok(Cache {
            credentials_path: self.credentials_path,
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

    #[test]
    fn error_missing_builder_attribute_display() {
        let err = Error::MissingBuilderAttribute("credentials_path".to_string());
        assert_eq!(
            err.to_string(),
            "Missing required builder attribute: credentials_path"
        );
    }

    #[test]
    fn error_missing_kv_store_display() {
        let err = Error::MissingKVStore;
        assert_eq!(err.to_string(), "Missing required value KV Store");
    }

    #[test]
    fn error_missing_jetstream_context_display() {
        let err = Error::MissingJetStreamContext;
        assert_eq!(err.to_string(), "Missing required value JetStream Context");
    }

    #[test]
    fn a_publish_to_no_stream_means_the_stream_is_missing() {
        let error =
            flowgen_core::cache::CacheError::PutFailed(Box::new(Error::JetStreamPublishAck {
                source: async_nats::jetstream::context::PublishError::new(
                    async_nats::jetstream::context::PublishErrorKind::StreamNotFound,
                ),
            }));
        assert!(stream_missing(&error));
    }

    #[test]
    fn a_revision_mismatch_does_not_mean_the_stream_is_missing() {
        let error = flowgen_core::cache::CacheError::RevisionMismatch {
            expected: 1,
            actual: 0,
        };
        assert!(!stream_missing(&error));
    }

    #[test]
    fn builder_build_without_credentials_leaves_field_none() {
        let cache = CacheBuilder::new().build().unwrap();
        assert!(cache.credentials_path.is_none());
        assert_eq!(cache.url, crate::client::DEFAULT_NATS_URL);
        assert!(cache.store.is_none());
        assert!(cache.jetstream.is_none());
    }

    #[test]
    fn builder_build_succeeds_with_credentials_path() {
        let cache = CacheBuilder::new()
            .credentials_path(PathBuf::from("/etc/nats/creds"))
            .build()
            .unwrap();
        assert_eq!(
            cache.credentials_path,
            Some(PathBuf::from("/etc/nats/creds"))
        );
        assert_eq!(cache.url, crate::client::DEFAULT_NATS_URL);
        assert!(cache.store.is_none());
        assert!(cache.jetstream.is_none());
    }

    #[test]
    fn builder_sets_url() {
        let cache = CacheBuilder::new()
            .credentials_path(PathBuf::from("/creds"))
            .url("nats://custom:4222".to_string())
            .build()
            .unwrap();
        assert_eq!(cache.url, "nats://custom:4222");
    }

    #[test]
    fn builder_sets_history() {
        let cache = CacheBuilder::new()
            .credentials_path(PathBuf::from("/creds"))
            .history(5)
            .build()
            .unwrap();
        assert_eq!(cache.history, Some(5));
    }

    #[test]
    fn builder_sets_tombstone_ttl() {
        let ttl = Duration::from_secs(7200);
        let cache = CacheBuilder::new()
            .credentials_path(PathBuf::from("/creds"))
            .tombstone_ttl(ttl)
            .build()
            .unwrap();
        assert_eq!(cache.tombstone_ttl, Some(ttl));
    }
}
