//! NATS JetStream Key-Value store processor.
//!
//! Provides get, put, list, and delete operations on a NATS JetStream
//! Key-Value bucket. Each operation receives an event, performs the KV
//! operation, and emits the result as a downstream event.

use flowgen_core::client::Client as FlowgenClientTrait;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

// --- Configuration ---

/// Default NATS server URL function for serde.
fn default_nats_url() -> String {
    crate::client::DEFAULT_NATS_URL.to_string()
}

/// NATS KV store processor configuration.
///
/// # Example
///
/// ```yaml
/// - nats_kv_store:
///     name: save_flow
///     credentials_path: /etc/nats/credentials.json
///     url: "{{env.NATS_URL}}"
///     bucket: flowgen_system
///     operation: put
///     key: "flows.{{event.data.path}}"
///
/// - nats_kv_store:
///     name: mirror_flows
///     url: "{{env.NATS_URL}}"
///     bucket: flowgen_system
///     operation: put
///     key_prefix: "flows."
///     prune: true
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Task name.
    pub name: String,
    /// Optional path to credentials file containing NATS
    /// authentication details.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// NATS server URL. Defaults to "localhost:4222".
    #[serde(default = "default_nats_url")]
    pub url: String,
    /// KV bucket name.
    pub bucket: String,
    /// KV operation to perform.
    pub operation: Operation,
    /// Key for get, put, and delete operations (supports templating). A `put`
    /// without `key` writes every entry in `event.data.entries`.
    #[serde(default)]
    pub key: Option<String>,
    /// Key prefix for list operations, and for the keys of `put` entries
    /// (supports templating). With `prune`, the rendered prefix must end in
    /// `.` or `/`, so `flows` cannot also prune `flows_archive.*`.
    #[serde(default)]
    pub key_prefix: Option<String>,
    /// For a `put` of entries: also delete the keys under `key_prefix` that
    /// are not among the entries.
    #[serde(default)]
    pub prune: bool,
    /// With `prune`: allow an empty `entries` list to delete every key under
    /// `key_prefix`. Off by default, so an empty source deletes nothing.
    #[serde(default)]
    pub allow_empty: bool,
    /// For `list` operations: also include each key's current value in
    /// the output under `values`. Defaults to false to keep list cheap.
    /// Use when downstream needs to compare existing content against new
    /// content (idempotency / diff against current state) — avoids a
    /// separate `get` per key. Values are decoded as UTF-8 strings; keys
    /// whose payload is not valid UTF-8 are omitted from `values` and
    /// still appear in `keys`.
    #[serde(default)]
    pub include_values: bool,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Config {}

/// KV store operations.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    /// Write `event.data.content` to `key`, or, without `key`, every entry of
    /// `event.data.entries` under `key_prefix`.
    #[default]
    Put,
    /// Read a value by key.
    Get,
    /// List keys matching a prefix.
    List,
    /// Delete a key.
    Delete,
}

/// Result of a KV get operation.
#[derive(Debug, Serialize)]
pub struct GetResult {
    /// Key that was looked up.
    pub key: String,
    /// Value content (null if not found).
    pub content: Option<String>,
    /// Whether the key was found.
    pub found: bool,
}

/// Result of a KV put operation.
#[derive(Debug, Serialize)]
pub struct PutResult {
    /// Key that was written.
    pub key: String,
    /// Revision number after the write.
    pub revision: u64,
}

/// One key to write in a `put` of entries.
#[derive(Debug, Deserialize)]
pub struct Entry {
    /// Key relative to `key_prefix`.
    pub key: String,
    /// Value to store; non-string values are stored as JSON.
    pub value: serde_json::Value,
}

/// Input of a `put` of entries.
#[derive(Debug, Deserialize)]
struct Entries {
    entries: Vec<Entry>,
}

/// Result of a `put` of entries.
#[derive(Debug, Default, Serialize)]
pub struct PutEntriesResult {
    /// Keys written because they were new or changed.
    pub put: Vec<String>,
    /// Keys deleted by `prune`.
    pub deleted: Vec<String>,
    /// Number of entries that already held their value.
    pub unchanged: usize,
}

/// Result of a KV list operation.
#[derive(Debug, Serialize)]
pub struct ListResult {
    /// Keys matching the prefix.
    pub keys: Vec<String>,
    /// Number of keys found.
    pub count: usize,
    /// Prefix that was used for filtering.
    pub prefix: String,
    /// Per-key values, present only when `include_values: true` is set
    /// on the config. Missing for normal list operations to keep the
    /// payload small. Keys whose payload is not valid UTF-8 are omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<std::collections::HashMap<String, String>>,
}

/// Result of a KV delete operation.
#[derive(Debug, Serialize)]
pub struct DeleteResult {
    /// Key that was deleted.
    pub key: String,
}

// --- Errors ---

/// Errors that can occur during KV store processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("NATS client error: {source}")]
    Client {
        #[source]
        source: crate::client::Error,
    },
    #[error("KV entry error: {source}")]
    KvEntry {
        #[source]
        source: async_nats::jetstream::kv::EntryError,
    },
    #[error("KV put error: {source}")]
    KvPut {
        #[source]
        source: async_nats::jetstream::kv::PutError,
    },
    #[error("KV bucket creation error: {source}")]
    KvBucketCreate {
        #[source]
        source: async_nats::jetstream::context::CreateKeyValueError,
    },
    #[error("KV keys listing error: {source}")]
    KvKeys {
        #[source]
        source: async_nats::error::Error<async_nats::jetstream::kv::WatchErrorKind>,
    },
    #[error("KV keys stream error: {source}")]
    KvKeyStream {
        #[source]
        source: async_nats::jetstream::kv::WatcherError,
    },
    #[error("KV delete error: {source}")]
    KvDelete {
        #[source]
        source: async_nats::jetstream::kv::UpdateError,
    },
    #[error("JetStream context not available after connect.")]
    MissingJetStream,
    #[error("Missing key in config for '{operation}' operation.")]
    MissingKey { operation: String },
    #[error("Missing content in event data for put operation.")]
    MissingContent,
    #[error("Event data is not JSON: {source}")]
    NotJson {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("A put without key needs `entries` in the event data: {source}")]
    InvalidEntries {
        #[source]
        source: serde_json::Error,
    },
    #[error("A put of entries needs key_prefix")]
    MissingKeyPrefix,
    #[error("No entries to put under '{prefix}'; prune would delete every key there, set allow_empty to allow it")]
    EmptyPrune { prefix: String },
    #[error("Prune needs a key_prefix ending in '.' or '/', got '{prefix}'")]
    UnsafePrunePrefix { prefix: String },
    #[error("Invalid entry key '{key}': KV keys use only letters, digits and '-/_=.' and cannot start or end with '.'")]
    InvalidEntryKey { key: String },
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
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
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

impl Error {
    /// Whether retrying the same event cannot succeed.
    fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::MissingKey { .. }
                | Error::MissingContent
                | Error::NotJson { .. }
                | Error::InvalidEntries { .. }
                | Error::MissingKeyPrefix
                | Error::EmptyPrune { .. }
                | Error::UnsafePrunePrefix { .. }
                | Error::InvalidEntryKey { .. }
                | Error::ConfigRender { .. }
                | Error::EventBuilder { .. }
                | Error::SerdeJson { .. }
        )
    }
}

/// A validated put of entries: the key prefix and the full key and value of each entry.
#[derive(Debug)]
struct PreparedEntries<'a> {
    prefix: &'a str,
    entries: Vec<(String, String)>,
}

/// Validates a put of entries before it touches the bucket.
fn prepare_entries(
    config: &Config,
    event_data: serde_json::Value,
) -> Result<PreparedEntries<'_>, Error> {
    let prefix = match config.key_prefix.as_deref() {
        Some(prefix) => prefix,
        None => return Err(Error::MissingKeyPrefix),
    };
    if config.prune && !prefix.ends_with(['.', '/']) {
        return Err(Error::UnsafePrunePrefix {
            prefix: prefix.to_string(),
        });
    }
    let Entries { entries } =
        serde_json::from_value(event_data).map_err(|source| Error::InvalidEntries { source })?;
    if config.prune && entries.is_empty() && !config.allow_empty {
        return Err(Error::EmptyPrune {
            prefix: prefix.to_string(),
        });
    }
    let mut prepared = Vec::with_capacity(entries.len());
    for entry in entries {
        let key = format!("{prefix}{}", entry.key);
        if !is_valid_key(&key) {
            return Err(Error::InvalidEntryKey { key });
        }
        let value = match entry.value {
            serde_json::Value::String(value) => value,
            other => other.to_string(),
        };
        prepared.push((key, value));
    }
    Ok(PreparedEntries {
        prefix,
        entries: prepared,
    })
}

/// Same rule the client enforces per write, checked up front so a bad entry
/// fails before anything is written.
fn is_valid_key(key: &str) -> bool {
    !key.is_empty()
        && !key.starts_with('.')
        && !key.ends_with('.')
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '/' | '_' | '=' | '.'))
}

/// Collects the keys under `prefix`, failing on the first stream error so no
/// caller acts on a partial listing.
async fn collect_keys<S>(mut key_stream: S, prefix: &str) -> Result<Vec<String>, Error>
where
    S: futures_util::Stream<Item = Result<String, async_nats::jetstream::kv::WatcherError>> + Unpin,
{
    let mut keys = Vec::new();
    while let Some(key) = key_stream.next().await {
        let key = key.map_err(|source| Error::KvKeyStream { source })?;
        if key.starts_with(prefix) {
            keys.push(key);
        }
    }
    Ok(keys)
}

// --- Event Handler ---

/// Event handler for KV store operations.
pub struct EventHandler {
    config: Arc<Config>,
    store: async_nats::jetstream::kv::Store,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Serializes pruning puts so two events cannot interleave their listing,
    /// writes and deletes.
    prune_lock: tokio::sync::Mutex<()>,
}

impl EventHandler {
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            let event_value = serde_json::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let rendered = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            let result = match rendered.operation {
                Operation::Put => {
                    let event_data = event
                        .data_as_json()
                        .map_err(|source| Error::NotJson { source })?;
                    match rendered.key.as_deref() {
                        Some(key) => self.handle_put(key, &event_data).await?,
                        None => self.handle_put_entries(&rendered, event_data).await?,
                    }
                }
                Operation::Get => self.handle_get(&rendered).await?,
                Operation::List => self.handle_list(&rendered).await?,
                Operation::Delete => self.handle_delete(&rendered).await?,
            };

            let mut e = EventBuilder::new()
                .data(EventData::Json(result))
                .subject(self.config.name.clone())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            match self.tx {
                None => {
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => {
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }

    async fn handle_put(
        &self,
        key: &str,
        event_data: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let value = match event_data.get("content").and_then(|c| c.as_str()) {
            Some(content) => bytes::Bytes::from(content.to_string()),
            None => return Err(Error::MissingContent),
        };

        let revision = self
            .store
            .put(key, value)
            .await
            .map_err(|source| Error::KvPut { source })?;

        let result = PutResult {
            key: key.to_string(),
            revision,
        };
        serde_json::to_value(&result).map_err(|source| Error::SerdeJson { source })
    }

    /// Writes every entry under `key_prefix` whose value changed and, with
    /// `prune`, deletes the keys under it that no entry names.
    async fn handle_put_entries(
        &self,
        config: &Config,
        event_data: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let PreparedEntries { prefix, entries } = prepare_entries(config, event_data)?;
        let _prune_guard = match config.prune {
            true => Some(self.prune_lock.lock().await),
            false => None,
        };

        let existing: HashSet<String> = self.keys_under(prefix).await?.into_iter().collect();
        let mut result = PutEntriesResult::default();
        let mut wanted = HashSet::with_capacity(entries.len());
        for (key, value) in entries {
            let current = match existing.contains(&key) {
                true => self
                    .store
                    .get(&key)
                    .await
                    .map_err(|source| Error::KvEntry { source })?,
                false => None,
            };
            match current {
                Some(current) if current.as_ref() == value.as_bytes() => result.unchanged += 1,
                _ => {
                    self.store
                        .put(key.as_str(), bytes::Bytes::from(value))
                        .await
                        .map_err(|source| Error::KvPut { source })?;
                    result.put.push(key.clone());
                }
            }
            wanted.insert(key);
        }

        if config.prune {
            let mut stale: Vec<String> = existing
                .into_iter()
                .filter(|key| !wanted.contains(key))
                .collect();
            stale.sort();
            for key in stale {
                self.store
                    .delete(&key)
                    .await
                    .map_err(|source| Error::KvDelete { source })?;
                result.deleted.push(key);
            }
        }
        serde_json::to_value(&result).map_err(|source| Error::SerdeJson { source })
    }

    async fn keys_under(&self, prefix: &str) -> Result<Vec<String>, Error> {
        let key_stream = self
            .store
            .keys()
            .await
            .map_err(|source| Error::KvKeys { source })?;
        collect_keys(key_stream, prefix).await
    }

    async fn handle_get(&self, config: &Config) -> Result<serde_json::Value, Error> {
        let key = config.key.as_deref().ok_or_else(|| Error::MissingKey {
            operation: "get".to_string(),
        })?;

        let entry = self
            .store
            .get(key)
            .await
            .map_err(|source| Error::KvEntry { source })?;

        let result = match entry {
            Some(value) => GetResult {
                key: key.to_string(),
                content: Some(String::from_utf8_lossy(&value).to_string()),
                found: true,
            },
            None => GetResult {
                key: key.to_string(),
                content: None,
                found: false,
            },
        };
        serde_json::to_value(&result).map_err(|source| Error::SerdeJson { source })
    }

    async fn handle_list(&self, config: &Config) -> Result<serde_json::Value, Error> {
        let prefix = config.key_prefix.as_deref().unwrap_or("");
        let keys = self.keys_under(prefix).await?;

        let values = if config.include_values {
            let mut map = std::collections::HashMap::with_capacity(keys.len());
            for key in &keys {
                if let Some(bytes) = self
                    .store
                    .get(key)
                    .await
                    .map_err(|source| Error::KvEntry { source })?
                {
                    if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                        map.insert(key.clone(), text);
                    }
                }
            }
            Some(map)
        } else {
            None
        };

        let result = ListResult {
            count: keys.len(),
            keys,
            prefix: prefix.to_string(),
            values,
        };
        serde_json::to_value(&result).map_err(|source| Error::SerdeJson { source })
    }

    async fn handle_delete(&self, config: &Config) -> Result<serde_json::Value, Error> {
        let key = config.key.as_deref().ok_or_else(|| Error::MissingKey {
            operation: "delete".to_string(),
        })?;

        self.store
            .delete(key)
            .await
            .map_err(|source| Error::KvDelete { source })?;

        let result = DeleteResult {
            key: key.to_string(),
        };
        serde_json::to_value(&result).map_err(|source| Error::SerdeJson { source })
    }
}

// --- Processor / Runner ---

/// NATS KV store processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Config>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Opens the bucket when it exists and creates it only when missing, so a
    /// bucket created elsewhere keeps its settings.
    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self
            .config
            .render(&serde_json::Value::Object(serde_json::Map::new()))
            .map_err(|source| Error::ConfigRender { source })?;

        let nats_key = flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
            .field("credentials_path", &config.credentials_path)
            .field("url", &config.url)
            .build();
        let jetstream_ctx = self
            .task_context
            .client_registry
            .get_or_init(nats_key, || {
                let credentials_path = config.credentials_path.clone();
                let url = config.url.clone();
                async move {
                    let mut builder = crate::client::ClientBuilder::new();
                    builder.url(url);
                    if let Some(path) = credentials_path {
                        builder.credentials_path(path);
                    }
                    let client = builder
                        .build()
                        .map_err(|source| Error::Client { source })?
                        .connect()
                        .await
                        .map_err(|source| Error::Client { source })?;
                    client.jetstream.ok_or(Error::MissingJetStream)
                }
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        let jetstream = (*jetstream_ctx).clone();

        let store = match jetstream.get_key_value(&config.bucket).await {
            Ok(store) => store,
            Err(_) => jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: config.bucket.clone(),
                    ..Default::default()
                })
                .await
                .map_err(|source| Error::KvBucketCreate { source })?,
        };

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            store,
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
            prune_lock: tokio::sync::Mutex::new(()),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize NATS KV store processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => return Err(e),
        };

        let mut handlers = Vec::new();
        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    let handle = tokio::spawn(async move {
                        if let Some(error) = event.error.clone() {
                            event.forward_failure(handler.tx.as_ref(), error).await;
                            return;
                        }
                        let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                            handler.handle(event.clone()).await.map_err(|e| {
                                error!(error = %e, "KV store operation failed");
                                match e.is_permanent() {
                                    true => tokio_retry::RetryError::permanent(e),
                                    false => tokio_retry::RetryError::transient(e),
                                }
                            })
                        })
                        .await;

                        if let Err(e) = result {
                            event
                                .forward_failure(handler.tx.as_ref(), e.to_string())
                                .await;
                        }
                    });
                    handlers.push(handle);
                    handlers.retain(|h| !h.is_finished());
                }
                None => {
                    futures_util::future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

/// Builder for NATS KV store processor.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<Config>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<Config>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
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

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
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

    // --- Config deserialization ---

    #[test]
    fn config_deser_minimal() {
        let json = r#"{
            "name": "kv_task",
            "credentials_path": "/etc/nats/creds.json",
            "bucket": "my_bucket",
            "operation": "get",
            "key": "some.key"
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.name, "kv_task");
        assert_eq!(
            cfg.credentials_path,
            Some(std::path::PathBuf::from("/etc/nats/creds.json"))
        );
        assert_eq!(cfg.bucket, "my_bucket");
        assert_eq!(cfg.operation, Operation::Get);
        assert_eq!(cfg.key, Some("some.key".to_string()));
        assert_eq!(cfg.key_prefix, None);
        assert_eq!(cfg.depends_on, None);
        assert_eq!(cfg.retry, None);
        // default url
        assert_eq!(cfg.url, crate::client::DEFAULT_NATS_URL);
    }

    #[test]
    fn config_deser_with_url_override() {
        let json = r#"{
            "name": "kv",
            "credentials_path": "/creds.json",
            "bucket": "b",
            "operation": "put",
            "key": "k",
            "url": "nats://remote:4222"
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.url, "nats://remote:4222");
    }

    #[test]
    fn config_deser_list_operation_with_prefix() {
        let json = r#"{
            "name": "lister",
            "credentials_path": "/creds.json",
            "bucket": "b",
            "operation": "list",
            "key_prefix": "flows."
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.operation, Operation::List);
        assert_eq!(cfg.key_prefix, Some("flows.".to_string()));
        assert_eq!(cfg.key, None);
    }

    #[test]
    fn config_deser_delete_operation() {
        let json = r#"{
            "name": "deleter",
            "credentials_path": "/creds.json",
            "bucket": "b",
            "operation": "delete",
            "key": "old.key"
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.operation, Operation::Delete);
    }

    #[test]
    fn config_deser_with_depends_on() {
        let json = r#"{
            "name": "kv",
            "credentials_path": "/creds.json",
            "bucket": "b",
            "operation": "put",
            "key": "k",
            "depends_on": ["fetch", "transform"]
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(
            cfg.depends_on,
            Some(vec!["fetch".to_string(), "transform".to_string()])
        );
    }

    #[test]
    fn config_deser_unknown_operation_rejected() {
        let json = r#"{
            "name": "kv",
            "credentials_path": "/creds.json",
            "bucket": "b",
            "operation": "upsert"
        }"#;
        let result = serde_json::from_str::<Config>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_roundtrip() {
        let cfg = Config {
            name: "rt".to_string(),
            credentials_path: Some(std::path::PathBuf::from("/c.json")),
            url: "nats://host:4222".to_string(),
            bucket: "bkt".to_string(),
            operation: Operation::Put,
            key: Some("k".to_string()),
            key_prefix: None,
            prune: false,
            allow_empty: false,
            include_values: false,
            depends_on: Some(vec!["a".to_string()]),
            retry: None,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }

    // --- Operation enum ---

    #[test]
    fn operation_default_is_put() {
        assert_eq!(Operation::default(), Operation::Put);
    }

    #[test]
    fn operation_all_variants_roundtrip() {
        for (variant, label) in [
            (Operation::Put, "put"),
            (Operation::Get, "get"),
            (Operation::List, "list"),
            (Operation::Delete, "delete"),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{label}\""));
            let back: Operation = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    // --- Result struct serialization ---

    #[test]
    fn get_result_found_serializes() {
        let r = GetResult {
            key: "flows.main".to_string(),
            content: Some("hello world".to_string()),
            found: true,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["key"], "flows.main");
        assert_eq!(v["content"], "hello world");
        assert_eq!(v["found"], true);
    }

    #[test]
    fn get_result_not_found_serializes() {
        let r = GetResult {
            key: "missing".to_string(),
            content: None,
            found: false,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["key"], "missing");
        assert!(v["content"].is_null());
        assert_eq!(v["found"], false);
    }

    #[test]
    fn put_result_serializes() {
        let r = PutResult {
            key: "k".to_string(),
            revision: 42,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["key"], "k");
        assert_eq!(v["revision"], 42);
    }

    #[test]
    fn list_result_serializes() {
        let r = ListResult {
            keys: vec!["a".to_string(), "b".to_string()],
            count: 2,
            prefix: "".to_string(),
            values: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["count"], 2);
        let keys = v["keys"].as_array().unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], "a");
        assert_eq!(keys[1], "b");
        // values omitted when not opted in.
        assert!(v.get("values").is_none());
    }

    #[test]
    fn list_result_empty_serializes() {
        let r = ListResult {
            keys: vec![],
            count: 0,
            prefix: "p.".to_string(),
            values: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["count"], 0);
        assert_eq!(v["keys"].as_array().unwrap().len(), 0);
        assert_eq!(v["prefix"], "p.");
    }

    #[test]
    fn list_result_with_values_serializes_map() {
        let mut map = std::collections::HashMap::new();
        map.insert("k1".to_string(), "v1".to_string());
        let r = ListResult {
            keys: vec!["k1".to_string()],
            count: 1,
            prefix: "".to_string(),
            values: Some(map),
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["values"]["k1"], "v1");
    }

    #[test]
    fn delete_result_serializes() {
        let r = DeleteResult {
            key: "old.key".to_string(),
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["key"], "old.key");
    }

    fn entries_config(key_prefix: Option<&str>, prune: bool) -> Config {
        Config {
            name: "kv".to_string(),
            bucket: "b".to_string(),
            operation: Operation::Put,
            key_prefix: key_prefix.map(str::to_string),
            prune,
            ..Default::default()
        }
    }

    fn entries(pairs: &[(&str, serde_json::Value)]) -> serde_json::Value {
        let entries = pairs
            .iter()
            .map(|(key, value)| serde_json::json!({"key": key, "value": value}))
            .collect::<Vec<_>>();
        serde_json::json!({ "entries": entries })
    }

    #[test]
    fn prune_rejects_an_empty_key_prefix() {
        let config = entries_config(Some(""), true);
        let result = prepare_entries(&config, entries(&[("a", "1".into())]));
        assert!(matches!(
            result,
            Err(Error::UnsafePrunePrefix { ref prefix }) if prefix.is_empty()
        ));
    }

    #[test]
    fn prune_rejects_a_key_prefix_without_a_trailing_separator() {
        let config = entries_config(Some("flows"), true);
        let result = prepare_entries(&config, entries(&[("a", "1".into())]));
        assert!(matches!(
            result,
            Err(Error::UnsafePrunePrefix { ref prefix }) if prefix == "flows"
        ));
    }

    #[test]
    fn prune_rejects_a_key_prefix_template_that_renders_empty() {
        let config = entries_config(Some("{{event.data.missing}}"), true);
        let rendered = config
            .render(&serde_json::json!({"event": {"data": {}}}))
            .unwrap();
        let result = prepare_entries(&rendered, entries(&[("a", "1".into())]));
        assert!(matches!(result, Err(Error::UnsafePrunePrefix { .. })));
    }

    #[test]
    fn prune_rejects_an_unsafe_key_prefix_before_reading_the_entries() {
        let config = entries_config(Some("flows"), true);
        let result = prepare_entries(&config, serde_json::json!({"unrelated": true}));
        assert!(matches!(result, Err(Error::UnsafePrunePrefix { .. })));
    }

    #[test]
    fn prune_accepts_key_prefixes_ending_in_a_dot_or_a_slash() {
        for prefix in ["flows.", "flows/"] {
            let config = entries_config(Some(prefix), true);
            let prepared = prepare_entries(&config, entries(&[("a", "1".into())])).unwrap();
            assert_eq!(
                prepared.entries,
                vec![(format!("{prefix}a"), "1".to_string())]
            );
        }
    }

    #[test]
    fn put_entries_without_prune_accepts_a_prefix_without_a_separator() {
        let config = entries_config(Some("flows"), false);
        let prepared = prepare_entries(&config, entries(&[("a", "1".into())])).unwrap();
        assert_eq!(prepared.prefix, "flows");
        assert_eq!(
            prepared.entries,
            vec![("flowsa".to_string(), "1".to_string())]
        );
    }

    #[test]
    fn put_entries_stores_non_string_values_as_json() {
        let config = entries_config(Some("p."), false);
        let prepared = prepare_entries(
            &config,
            entries(&[
                ("n", serde_json::json!(1)),
                ("o", serde_json::json!({"x": true})),
            ]),
        )
        .unwrap();
        assert_eq!(
            prepared.entries,
            vec![
                ("p.n".to_string(), "1".to_string()),
                ("p.o".to_string(), r#"{"x":true}"#.to_string()),
            ]
        );
    }

    #[test]
    fn put_entries_rejects_a_missing_key_prefix() {
        let config = entries_config(None, false);
        let result = prepare_entries(&config, entries(&[("a", "1".into())]));
        assert!(matches!(result, Err(Error::MissingKeyPrefix)));
    }

    #[test]
    fn put_entries_rejects_an_empty_list_with_prune_unless_allow_empty() {
        let mut config = entries_config(Some("p."), true);
        let result = prepare_entries(&config, entries(&[]));
        assert!(matches!(result, Err(Error::EmptyPrune { .. })));

        config.allow_empty = true;
        let prepared = prepare_entries(&config, entries(&[])).unwrap();
        assert!(prepared.entries.is_empty());
    }

    #[test]
    fn put_entries_rejects_the_whole_list_when_one_composed_key_is_invalid() {
        let config = entries_config(Some("p."), false);
        let result = prepare_entries(
            &config,
            entries(&[("a", "1".into()), ("bad key", "2".into())]),
        );
        assert!(matches!(
            result,
            Err(Error::InvalidEntryKey { ref key }) if key == "p.bad key"
        ));
    }

    #[test]
    fn put_entries_rejects_an_empty_entry_key_that_leaves_a_trailing_dot() {
        let config = entries_config(Some("p."), false);
        let result = prepare_entries(&config, entries(&[("", "1".into())]));
        assert!(matches!(
            result,
            Err(Error::InvalidEntryKey { ref key }) if key == "p."
        ));
    }

    #[test]
    fn is_valid_key_accepts_kv_key_characters_and_rejects_the_rest() {
        for key in ["a", "a.b", "a/b", "A-b_c=d.0"] {
            assert!(is_valid_key(key), "{key} should be valid");
        }
        for key in ["", ".a", "a.", "a b", "a*", "a.>", "a:b", "ä"] {
            assert!(!is_valid_key(key), "{key} should be invalid");
        }
    }

    #[tokio::test]
    async fn collect_keys_returns_only_keys_under_the_prefix() {
        let stream = futures_util::stream::iter(vec![
            Ok("p.a".to_string()),
            Ok("q.b".to_string()),
            Ok("p.c".to_string()),
        ]);
        let keys = collect_keys(stream, "p.").await.unwrap();
        assert_eq!(keys, vec!["p.a".to_string(), "p.c".to_string()]);
    }

    #[tokio::test]
    async fn collect_keys_fails_on_a_stream_error_instead_of_returning_a_partial_listing() {
        let stream = futures_util::stream::iter(vec![
            Ok("p.a".to_string()),
            Err(async_nats::jetstream::kv::WatcherError::new(
                async_nats::jetstream::kv::WatcherErrorKind::Other,
            )),
            Ok("p.b".to_string()),
        ]);
        let result = collect_keys(stream, "p.").await;
        assert!(matches!(result, Err(Error::KvKeyStream { .. })));
    }

    #[test]
    fn errors_that_cannot_succeed_on_retry_are_permanent() {
        let permanent = [
            Error::MissingKey {
                operation: "get".to_string(),
            },
            Error::MissingContent,
            Error::MissingKeyPrefix,
            Error::EmptyPrune {
                prefix: "p.".to_string(),
            },
            Error::UnsafePrunePrefix {
                prefix: String::new(),
            },
            Error::InvalidEntryKey {
                key: "p.".to_string(),
            },
            Error::InvalidEntries {
                source: serde_json::from_str::<Entries>("{}").unwrap_err(),
            },
        ];
        for error in permanent {
            assert!(error.is_permanent(), "{error} should be permanent");
        }
    }

    #[test]
    fn transport_errors_are_retried() {
        let transient = [
            Error::KvPut {
                source: async_nats::jetstream::kv::PutError::new(
                    async_nats::jetstream::kv::PutErrorKind::Publish,
                ),
            },
            Error::KvKeyStream {
                source: async_nats::jetstream::kv::WatcherError::new(
                    async_nats::jetstream::kv::WatcherErrorKind::Consumer,
                ),
            },
        ];
        for error in transient {
            assert!(!error.is_permanent(), "{error} should be retried");
        }
    }
}
