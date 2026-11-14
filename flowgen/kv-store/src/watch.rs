//! The `kv_watch` task: starts its flow with the value of a key, or of each
//! key under a prefix, whenever it changes in a redb file or a NATS bucket.

use crate::store::{self, Bucket, ConfigError};
use flowgen_core::cache::{Cache, CacheError, WatchEvent};
use flowgen_core::config::ConfigExt;
use flowgen_core::delivery::{Delivered, Delivery};
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::retry::{ReconnectBackoff, RetryConfig};
use flowgen_core::task::TaskConfig;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tracing::{error, warn};

/// Key-value watch source configuration.
///
/// # Example
///
/// ```yaml
/// - kv_watch:
///     name: on_release
///     url: "redb://.flowgen/state.redb"
///     bucket: flowgen_system
///     key: releases.latest
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct KvWatch {
    /// Task name.
    pub name: String,
    /// The store, as in `kv_store`: `redb://<path>` or a NATS server.
    pub url: String,
    /// NATS credentials file; a redb file takes none. Empty means none.
    #[serde(default, deserialize_with = "flowgen_core::config::optional_path")]
    pub credentials_path: Option<PathBuf>,
    /// Bucket, created when missing.
    pub bucket: String,
    /// The key to watch, under a folder such as `releases.`.
    #[serde(default)]
    pub key: Option<String>,
    /// Watches every key under this prefix, which ends in `.` or `/`.
    #[serde(default)]
    pub key_prefix: Option<String>,
    /// How long to wait for the flow to complete a value. Waits
    /// indefinitely when omitted.
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<Duration>,
    /// How many times a value is sent through the flow before it is skipped.
    /// Delivered until the flow completes it when omitted.
    #[serde(default)]
    pub max_deliver: Option<std::num::NonZeroU32>,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration for connecting and redelivery (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<RetryConfig>,
}

impl ConfigExt for KvWatch {}

/// What a `kv_watch` watches: one key, or the keys under a prefix.
enum Watched<'a> {
    Key(&'a str),
    Prefix(&'a str),
}

impl KvWatch {
    fn watched(&self) -> Result<Watched<'_>, ConfigError> {
        match (self.key.as_deref(), self.key_prefix.as_deref()) {
            (Some(key), None) => Ok(Watched::Key(key)),
            (None, Some(prefix)) => Ok(Watched::Prefix(prefix)),
            _ => Err(ConfigError::KeyOrPrefix),
        }
    }

    /// The store, and a key or prefix under a folder; templated values are
    /// checked once rendered.
    pub fn check_config(&self) -> Result<(), ConfigError> {
        store::backend(&self.url, self.credentials_path.as_deref())?;
        let watched = self.watched()?;
        let (text, valid) = match watched {
            Watched::Key(key) => (key, store::is_valid_key(key)),
            Watched::Prefix(prefix) => (
                prefix,
                prefix.ends_with(['.', '/'])
                    && store::is_valid_key(prefix.trim_end_matches(['.', '/'])),
            ),
        };
        match store::literal(text) {
            Some(text) if !valid || folder(text).is_none() => Err(ConfigError::NotInFolder {
                key: text.to_string(),
            }),
            _ => Ok(()),
        }
    }
}

/// The first segment of `key`, which a NATS bucket watches as `<folder>.>`.
fn folder(key: &str) -> Option<&str> {
    match key.split_once('.') {
        Some((folder, _)) if !folder.is_empty() => Some(folder),
        _ => None,
    }
}

impl Watched<'_> {
    fn matches(&self, key: &str) -> bool {
        match self {
            Watched::Key(watched) => key == *watched,
            Watched::Prefix(prefix) => key.starts_with(prefix),
        }
    }

    fn text(&self) -> &str {
        match self {
            Watched::Key(text) | Watched::Prefix(text) => text,
        }
    }
}

/// Errors from the key-value watch source.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Store(#[from] store::Error),
    #[error("Failed to read the store: {source}")]
    Cache {
        #[source]
        source: CacheError,
    },
    #[error("The watch of '{folder}' ended")]
    WatchEnded { folder: String },
    #[error("Error sending event: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to render config: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
}

impl Error {
    fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::Config(_) | Error::ConfigRender { .. } | Error::Store(store::Error::Config(_))
        )
    }
}

/// The value of a key as event data: JSON when it parses, else text, else bytes.
fn value_data(value: &[u8]) -> EventData {
    match (serde_json::from_slice(value), std::str::from_utf8(value)) {
        (Ok(json), _) => EventData::Json(json),
        (Err(_), Ok(text)) => EventData::Json(serde_json::Value::String(text.to_string())),
        (Err(_), Err(_)) => EventData::Bytes(bytes::Bytes::copy_from_slice(value)),
    }
}

/// Watches the store and delivers each changed value until the flow stops reading.
pub struct EventHandler {
    config: Arc<KvWatch>,
    store: Arc<dyn Cache>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    retry_config: RetryConfig,
    received: AtomicBool,
}

impl EventHandler {
    /// Cache key holding the digest of the last value of `key` the flow finished.
    fn cursor_key(&self, key: &str) -> String {
        format!("flow.{}.kv_watch.{key}", self.task_context.flow.id())
    }

    /// Sends the value of `key` through the flow unless it finished it already,
    /// then records it; a stop first leaves the record as it is.
    #[tracing::instrument(skip(self, value), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn offer(&self, key: &str, value: &[u8]) -> Result<Delivered, Error> {
        self.received.store(true, Ordering::Relaxed);
        let digest = format!("{:x}", sha2::Sha256::digest(value));
        let cursor_key = self.cursor_key(key);
        let cache = self.task_context.cache.as_ref();
        let finished = cache
            .get(&cursor_key)
            .await
            .map_err(|source| Error::Cache { source })?;
        if finished.as_deref() == Some(digest.as_bytes()) {
            return Ok(Delivered::Completed);
        }

        let event = EventBuilder::new()
            .subject(key.to_string())
            .id(format!("{key}-{digest}"))
            .data(value_data(value))
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;
        let delivery = Delivery {
            downstream: self.tx.is_some(),
            leaf_count: self.task_context.leaf_count,
            ack_timeout: self.config.ack_timeout,
            max_deliver: self.config.max_deliver,
            retry: &self.retry_config,
            cancellation: &self.task_context.cancellation_token,
        };
        let delivered = delivery
            .deliver(&event, |attempt| {
                attempt.send_with_logging(self.tx.as_ref())
            })
            .await
            .map_err(|source| Error::SendMessage { source })?;
        if delivered == Delivered::Stopped {
            return Ok(delivered);
        }
        if let Err(e) = cache.put(&cursor_key, digest.into(), None).await {
            error!(key, error = %e, "Failed to record the finished value");
        }
        Ok(delivered)
    }

    /// Delivers the current values, then each change, until the flow stops reading.
    async fn handle(&self) -> Result<(), Error> {
        let watched = self.config.watched()?;
        let folder = match folder(watched.text()) {
            Some(folder) => folder,
            None => {
                return Err(ConfigError::NotInFolder {
                    key: watched.text().to_string(),
                }
                .into())
            }
        };
        let stop_reading = self.task_context.stop_reading.clone();
        let mut changes = self
            .store
            .watch(folder, false)
            .await
            .map_err(|source| Error::Cache { source })?;

        let mut keys = self
            .store
            .list_keys(folder)
            .await
            .map_err(|source| Error::Cache { source })?;
        keys.retain(|key| watched.matches(key));
        keys.sort();
        for key in keys {
            if stop_reading.is_cancelled() {
                return Ok(());
            }
            let value = self
                .store
                .get(&key)
                .await
                .map_err(|source| Error::Cache { source })?;
            if let Some(value) = value {
                if self.offer(&key, &value).await? == Delivered::Stopped {
                    return Ok(());
                }
            }
        }

        loop {
            let change = tokio::select! {
                biased;
                _ = stop_reading.cancelled() => return Ok(()),
                change = changes.next() => change,
            };
            match change {
                Some(Ok(WatchEvent::Put { key, value })) if watched.matches(&key) => {
                    if self.offer(&key, &value).await? == Delivered::Stopped {
                        return Ok(());
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(source)) => return Err(Error::Cache { source }),
                None => {
                    return Err(Error::WatchEnded {
                        folder: folder.to_string(),
                    })
                }
            }
        }
    }
}

/// Key-value watch source.
#[derive(Debug)]
pub struct Watcher {
    config: Arc<KvWatch>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Watcher {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self
            .config
            .render(&serde_json::Value::Object(serde_json::Map::new()))
            .map_err(|source| Error::ConfigRender { source })?;
        config.check_config()?;
        let bucket = Bucket {
            url: &config.url,
            credentials_path: &config.credentials_path,
            name: &config.bucket,
            history: store::default_history(),
            ttl: false,
        };
        let store = store::open(bucket, &self.task_context.client_registry, self.task_type).await?;
        Ok(EventHandler {
            config: Arc::new(config),
            store,
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            retry_config: RetryConfig::merge(&self.task_context.retry, &self.config.retry),
            task_context: Arc::clone(&self.task_context),
            received: AtomicBool::new(false),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config = RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let stop_reading = self.task_context.stop_reading.clone();
        let mut reconnect_backoff = ReconnectBackoff::new(&retry_config);

        loop {
            if stop_reading.is_cancelled() {
                return Ok(());
            }
            let init = tokio_retry::Retry::spawn(
                retry_config.init_strategy(self.task_context.startup_delay),
                || async {
                    self.init().await.map_err(|e| {
                        error!(error = %e, "Failed to initialize key-value watch");
                        match e.is_permanent() {
                            true => tokio_retry::RetryError::permanent(e),
                            false => tokio_retry::RetryError::transient(e),
                        }
                    })
                },
            );
            let handler = tokio::select! {
                _ = stop_reading.cancelled() => return Ok(()),
                handler = init => handler,
            };
            let received = match handler {
                Ok(handler) => {
                    match handler.handle().await {
                        _ if stop_reading.is_cancelled() => return Ok(()),
                        Ok(()) => warn!("Key-value watch ended, watching again"),
                        Err(e) if e.is_permanent() => return Err(e),
                        Err(e) => warn!(error = %e, "Key-value watch failed, watching again"),
                    }
                    handler.received.load(Ordering::Relaxed)
                }
                Err(e) if e.is_permanent() => return Err(e),
                Err(_) => false,
            };
            let delay = reconnect_backoff.next_delay(received);
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = stop_reading.cancelled() => return Ok(()),
            }
        }
    }
}

/// Builder for the key-value watch source.
#[derive(Default)]
pub struct WatcherBuilder {
    config: Option<Arc<KvWatch>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl WatcherBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<KvWatch>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(mut self, ctx: Arc<flowgen_core::task::context::TaskContext>) -> Self {
        self.task_context = Some(ctx);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Watcher, Error> {
        Ok(Watcher {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowgen_core::cache::RedbCache;
    use flowgen_core::task::runner::Runner;
    use serde_json::json;

    fn config(fields: serde_json::Value) -> KvWatch {
        let mut config = json!({"name": "w", "url": "redb://a.redb", "bucket": "b"});
        if let (Some(config), serde_json::Value::Object(fields)) = (config.as_object_mut(), fields)
        {
            config.extend(fields);
        }
        serde_json::from_value(config).unwrap()
    }

    #[test]
    fn a_watch_needs_one_key_or_prefix_under_a_folder() {
        let check = |fields| match config(fields).check_config() {
            Ok(()) => "ok".to_string(),
            Err(error) => error.to_string(),
        };
        assert_eq!(check(json!({"key": "releases.latest"})), "ok");
        assert_eq!(check(json!({"key_prefix": "flows."})), "ok");
        assert_eq!(check(json!({"key": "{{env.KEY}}"})), "ok");
        assert_eq!(check(json!({})), ConfigError::KeyOrPrefix.to_string());
        assert_eq!(
            check(json!({"key": "a.b", "key_prefix": "a."})),
            ConfigError::KeyOrPrefix.to_string()
        );
        for (fields, key) in [
            (json!({"key": "latest"}), "latest"),
            (json!({"key_prefix": "flows"}), "flows"),
            (json!({"key": "a."}), "a."),
        ] {
            assert_eq!(
                check(fields),
                ConfigError::NotInFolder {
                    key: key.to_string()
                }
                .to_string()
            );
        }
    }

    fn context(cache: Arc<dyn Cache>) -> Arc<flowgen_core::task::context::TaskContext> {
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("f".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    async fn start(
        path: &std::path::Path,
        runtime: &Arc<dyn Cache>,
    ) -> (
        tokio::sync::mpsc::Receiver<Event>,
        Arc<flowgen_core::task::context::TaskContext>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let context = context(Arc::clone(runtime));
        let watcher = WatcherBuilder::new()
            .config(Arc::new(config(json!({
                "url": format!("redb://{}", path.display()),
                "key": "releases.latest",
            }))))
            .sender(tx)
            .task_type("kv_watch")
            .task_context(Arc::clone(&context))
            .build()
            .await
            .unwrap();
        tokio::spawn(watcher.run());
        (rx, context)
    }

    async fn next(rx: &mut tokio::sync::mpsc::Receiver<Event>) -> Event {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("A value arrives")
            .expect("The watch runs")
    }

    #[tokio::test]
    async fn the_current_value_and_each_change_start_the_flow_once() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("state.redb");
        let store = RedbCache::open(&path, "b").unwrap();
        store
            .put("releases.latest", "a".into(), None)
            .await
            .unwrap();
        store.put("releases.other", "x".into(), None).await.unwrap();
        let runtime = Arc::new(flowgen_core::cache::MemoryCache::new()) as Arc<dyn Cache>;

        let (mut rx, context) = start(&path, &runtime).await;
        let first = next(&mut rx).await;
        assert_eq!(first.subject, "releases.latest");
        assert_eq!(first.data_as_json().unwrap(), json!("a"));
        if let Some(completion) = &first.completion_tx {
            completion.signal_completion(None);
        }
        store
            .put("releases.latest", "b".into(), None)
            .await
            .unwrap();
        let second = next(&mut rx).await;
        assert_eq!(second.data_as_json().unwrap(), json!("b"));
        if let Some(completion) = &second.completion_tx {
            completion.signal_completion(None);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        context.stop_reading.cancel();
        context.cancellation_token.cancel();

        let (mut rx, context) = start(&path, &runtime).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), rx.recv())
                .await
                .is_err(),
            "A finished value is not delivered again"
        );
        store
            .put("releases.latest", "c".into(), None)
            .await
            .unwrap();
        assert_eq!(next(&mut rx).await.data_as_json().unwrap(), json!("c"));
        context.cancellation_token.cancel();
    }
}
