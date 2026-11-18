//! The `kv_store` task: gets, puts, lists and deletes keys in a key-value
//! store chosen by its URL, a redb file on this machine or a NATS server.

use flowgen_core::cache::{Cache, CacheError, RedbCache};
use flowgen_core::client::Client as _;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::task::TaskConfig;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

/// The URL scheme of a redb file, followed by its path.
pub const REDB_SCHEME: &str = "redb://";

/// The URL schemes of a NATS server; a URL without a scheme is one too.
const NATS_SCHEMES: [&str; 4] = ["nats://", "tls://", "ws://", "wss://"];

/// The most values a NATS bucket keeps per key.
const MAX_HISTORY: i64 = 64;

pub(crate) fn default_history() -> i64 {
    1
}

/// What starts a template in a config value, which is checked once rendered.
const TEMPLATE: &str = "{{";

/// Whether `key` is a key every store accepts: letters, digits and `-/_=.`,
/// not starting or ending with `.`.
pub(crate) fn is_valid_key(key: &str) -> bool {
    !key.is_empty()
        && !key.starts_with('.')
        && !key.ends_with('.')
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '/' | '_' | '=' | '.'))
}

/// `value` unless it holds a template, which can only be checked rendered.
pub(crate) fn literal(value: &str) -> Option<&str> {
    match value.contains(TEMPLATE) {
        true => None,
        false => Some(value),
    }
}

/// Key-value store processor configuration.
///
/// # Example
///
/// ```yaml
/// - kv_store:
///     name: mirror_flows
///     url: "redb://.flowgen/state.redb"
///     bucket: flowgen_system
///     operation: put
///     key_prefix: "flows."
///     prune: true
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct KvStore {
    /// Task name.
    ///
    /// Example: `name: mirror_flows`
    pub name: String,
    /// The store: `redb://<path>` for a redb file, relative to the working
    /// directory unless absolute, or a NATS server: `nats://`, `tls://`,
    /// `ws://`, `wss://` or `<host>:<port>`.
    ///
    /// Example: `url: "nats://{{env.NATS_HOST}}:4222"`
    pub url: String,
    /// NATS credentials file; a redb file takes none. Empty means none.
    ///
    /// Example: `credentials_path: /etc/nats/credentials.json`
    #[serde(default, deserialize_with = "flowgen_core::config::optional_path")]
    pub credentials_path: Option<PathBuf>,
    /// Bucket, created when missing.
    ///
    /// Example: `bucket: flowgen_system`
    pub bucket: String,
    /// Operation to perform.
    ///
    /// Example: `operation: put`
    pub operation: Operation,
    /// Key for get, put and delete (supports templating). A `put` without
    /// `key` writes every entry in `event.data.entries`.
    ///
    /// Example: `key: "flows.{{event.data.path}}"`
    #[serde(default)]
    pub key: Option<String>,
    /// Prefix of the keys a `list` returns, and of the keys of `put` entries.
    /// With `prune` it must end in `.` or `/`, so `flows` cannot also prune
    /// `flows_archive.*`.
    ///
    /// Example: `key_prefix: "flows."`
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
    /// For `list`: also return each key's value under `values`. Values that
    /// are not UTF-8 are left out of `values`.
    #[serde(default)]
    pub include_values: bool,
    /// For `put`: how long the value lives, e.g. `"30d"`.
    ///
    /// Example: `ttl: "1h"`
    #[serde(default, with = "humantime_serde")]
    pub ttl: Option<std::time::Duration>,
    /// Options of a NATS server `url`.
    ///
    /// Example: `nats: { history: 10 }`
    #[serde(default)]
    pub nats: Option<NatsOptions>,
    /// Optional list of upstream task names this task depends on.
    ///
    /// Example: `depends_on: [to_entries]`
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    ///
    /// Example: `retry: {max_attempts: 3, initial_backoff: "2s"}`
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to the number of CPUs.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for KvStore {}

/// Options of a NATS server `url`.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NatsOptions {
    /// Values the bucket keeps per key, 1 to 64, set when this task creates it.
    #[serde(default = "default_history")]
    pub history: i64,
}

/// Key-value store operations.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    /// Write `event.data.content` to `key`, or, without `key`, every entry of
    /// `event.data.entries` under `key_prefix`.
    #[default]
    Put,
    /// Read a value by key.
    Get,
    /// List the keys under `key_prefix`.
    List,
    /// Delete a key.
    Delete,
}

/// The store a URL names.
pub enum Backend<'a> {
    /// A redb file, by path.
    Redb(&'a Path),
    /// A NATS server, by URL.
    Nats(&'a str),
}

/// The store `url` names; only NATS takes credentials.
pub fn backend<'a>(
    url: &'a str,
    credentials_path: Option<&Path>,
) -> Result<Backend<'a>, ConfigError> {
    match url.strip_prefix(REDB_SCHEME) {
        Some(_) if credentials_path.is_some() => Err(ConfigError::RedbCredentials),
        Some("") => Err(ConfigError::MissingPath),
        Some(path) => Ok(Backend::Redb(Path::new(path))),
        None => match literal(url) {
            Some(url)
                if url.contains("://")
                    && !NATS_SCHEMES.iter().any(|scheme| url.starts_with(scheme)) =>
            {
                Err(ConfigError::UnsupportedScheme {
                    url: url.to_string(),
                })
            }
            _ => Ok(Backend::Nats(url)),
        },
    }
}

/// A bucket a task opens.
pub(crate) struct Bucket<'a> {
    pub url: &'a str,
    pub credentials_path: &'a Option<PathBuf>,
    pub name: &'a str,
    /// Values a NATS bucket keeps per key, set when the task creates it.
    pub history: i64,
    /// Whether the task writes values with a TTL, which the NATS bucket must allow.
    pub ttl: bool,
}

impl KvStore {
    /// The store `url` names; only NATS takes credentials.
    pub fn backend(&self) -> Result<Backend<'_>, ConfigError> {
        backend(&self.url, self.credentials_path.as_deref())
    }

    fn bucket(&self) -> Bucket<'_> {
        Bucket {
            url: &self.url,
            credentials_path: &self.credentials_path,
            name: &self.bucket,
            history: match &self.nats {
                Some(nats) => nats.history,
                None => default_history(),
            },
            ttl: self.ttl.is_some(),
        }
    }

    /// The store, the keys the operation needs and a prune prefix that can
    /// only delete its own keys; templated values are checked once rendered.
    pub fn check_config(&self) -> Result<(), ConfigError> {
        match (self.backend()?, &self.nats) {
            (Backend::Redb(_), Some(_)) => {
                return Err(flowgen_core::config::ProviderOptionsMismatch {
                    options: "nats",
                    used: self.url.clone(),
                }
                .into())
            }
            (Backend::Nats(_), Some(nats)) if !(1..=MAX_HISTORY).contains(&nats.history) => {
                return Err(ConfigError::InvalidHistory {
                    history: nats.history,
                })
            }
            _ => {}
        }
        let key = self.key.as_deref();
        let prefix = self.key_prefix.as_deref();
        match (&self.operation, key, prefix) {
            (Operation::Get, None, _) => return Err(ConfigError::MissingKey { operation: "get" }),
            (Operation::Delete, None, _) => {
                return Err(ConfigError::MissingKey {
                    operation: "delete",
                })
            }
            (Operation::Put, None, None) => return Err(ConfigError::MissingKeyOrPrefix),
            _ => {}
        }
        if let Some(key) = key.and_then(literal) {
            if !is_valid_key(key) {
                return Err(ConfigError::InvalidKey {
                    key: key.to_string(),
                });
            }
        }
        match (self.prune, prefix.and_then(literal)) {
            (true, Some(prefix)) if !prefix.ends_with(['.', '/']) => {
                Err(ConfigError::UnsafePrunePrefix {
                    prefix: prefix.to_string(),
                })
            }
            _ => Ok(()),
        }
    }
}

/// An invalid key-value store config.
#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("A redb:// URL needs the path of the file")]
    MissingPath,
    #[error("A redb file takes no credentials_path")]
    RedbCredentials,
    #[error("'{url}' is neither a redb:// file nor a NATS server (nats://, tls://, ws://, wss:// or host:port)")]
    UnsupportedScheme { url: String },
    #[error("The '{operation}' operation needs key")]
    MissingKey { operation: &'static str },
    #[error("A put needs key, or key_prefix for the entries in the event")]
    MissingKeyOrPrefix,
    #[error("Invalid key '{key}': use letters, digits and -/_=., not starting or ending with '.'")]
    InvalidKey { key: String },
    #[error("Prune needs a key_prefix ending in '.' or '/', got '{prefix}'")]
    UnsafePrunePrefix { prefix: String },
    #[error("nats.history must be 1 to 64, got {history}")]
    InvalidHistory { history: i64 },
    #[error(transparent)]
    ProviderOptions(#[from] flowgen_core::config::ProviderOptionsMismatch),
    #[error("A watch needs either key or key_prefix")]
    KeyOrPrefix,
    #[error("'{key}' is not a key or a key_prefix ending in '.' or '/' under a folder, such as 'releases.latest' or 'releases.'")]
    NotInFolder { key: String },
}

/// Result of a get.
#[derive(Debug, Serialize)]
pub struct GetResult {
    /// Key that was looked up.
    pub key: String,
    /// Value as text; null when the key is missing.
    pub content: Option<String>,
    /// Whether the key was found.
    pub found: bool,
}

/// Result of a put of one key, or a delete.
#[derive(Debug, Serialize)]
pub struct KeyResult {
    /// Key that was written or deleted.
    pub key: String,
}

/// One key to write in a `put` of entries.
#[derive(Debug, Deserialize)]
pub struct Entry {
    /// Key relative to `key_prefix`.
    pub key: String,
    /// Value to store; non-string values are stored as JSON.
    pub value: serde_json::Value,
}

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

/// Result of a list.
#[derive(Debug, Serialize)]
pub struct ListResult {
    /// Keys under the prefix.
    pub keys: Vec<String>,
    /// Number of keys found.
    pub count: usize,
    /// Prefix that was listed.
    pub prefix: String,
    /// Each key's value, with `include_values`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<HashMap<String, String>>,
}

/// Errors from the key-value store processor.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Store(#[from] CacheError),
    #[error("Failed to connect to the NATS store: {source}")]
    Nats {
        #[source]
        source: flowgen_nats::cache::Error,
    },
    #[error("Another task registered a different client for this NATS server")]
    ClientRegistryMismatch,
    #[error("Missing key in config for '{operation}' operation")]
    MissingKey { operation: &'static str },
    #[error("Missing content in event data for put operation")]
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
    #[error("Invalid key '{key}': use letters, digits and -/_=., not starting or ending with '.'")]
    InvalidKey { key: String },
    #[error("The NATS bucket '{bucket}' has no per-key TTL, which ttl needs; enable per-message TTL on the bucket (NATS 2.11+) or remove ttl")]
    TtlNotAllowed { bucket: String },
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
    #[error("Failed to render config: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
}

impl Error {
    /// Whether retrying the same event cannot succeed.
    fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::Config(_)
                | Error::MissingKey { .. }
                | Error::MissingContent
                | Error::NotJson { .. }
                | Error::InvalidEntries { .. }
                | Error::MissingKeyPrefix
                | Error::EmptyPrune { .. }
                | Error::UnsafePrunePrefix { .. }
                | Error::InvalidKey { .. }
                | Error::TtlNotAllowed { .. }
                | Error::ConfigRender { .. }
                | Error::EventBuilder { .. }
                | Error::SerdeJson { .. }
        )
    }
}

/// A validated put of entries: the key prefix and the full key and value of each entry.
struct PreparedEntries<'a> {
    prefix: &'a str,
    entries: Vec<(String, String)>,
}

/// Validates a put of entries before it touches the store.
fn prepare_entries(
    config: &KvStore,
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
        valid_key(&key)?;
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

/// `key` when every store accepts it.
fn valid_key(key: &str) -> Result<&str, Error> {
    match is_valid_key(key) {
        true => Ok(key),
        false => Err(Error::InvalidKey {
            key: key.to_string(),
        }),
    }
}

fn to_json(result: &impl Serialize) -> Result<serde_json::Value, Error> {
    serde_json::to_value(result).map_err(|source| Error::SerdeJson { source })
}

/// Event handler for key-value store operations.
pub struct EventHandler {
    config: Arc<KvStore>,
    store: Arc<dyn Cache>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Serializes pruning puts so two events cannot interleave their listing,
    /// writes and deletes.
    prune_lock: tokio::sync::Mutex<()>,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    fn is_retryable(&self, error: &Error) -> bool {
        !error.is_permanent()
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if let Some(error) = event.error.clone() {
            event.forward_failure(self.tx.as_ref(), error).await;
            return Ok(());
        }
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }
        let event = Arc::new(event);
        let completion_tx = event.completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            let config = self
                .config
                .render_for_event(event.as_ref())
                .map_err(|source| Error::ConfigRender { source })?;
            let result = match config.operation {
                Operation::Put => {
                    let data = event
                        .data_as_json()
                        .map_err(|source| Error::NotJson { source })?;
                    match config.key.as_deref() {
                        Some(key) => self.put(key, &data).await?,
                        None => self.put_entries(&config, data).await?,
                    }
                }
                Operation::Get => self.get(&config).await?,
                Operation::List => self.list(&config).await?,
                Operation::Delete => self.delete(&config).await?,
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
                    if let Some(arc) = completion_tx.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => e.completion_tx = completion_tx.clone(),
            }
            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })
        })
        .await
    }
}

impl EventHandler {
    fn ttl_secs(&self) -> Option<u64> {
        self.config.ttl.map(|ttl| ttl.as_secs().max(1))
    }

    async fn put(&self, key: &str, data: &serde_json::Value) -> Result<serde_json::Value, Error> {
        let key = valid_key(key)?;
        let content = match data.get("content") {
            Some(serde_json::Value::String(content)) => content.clone(),
            _ => return Err(Error::MissingContent),
        };
        self.store
            .put(key, bytes::Bytes::from(content), self.ttl_secs())
            .await?;
        to_json(&KeyResult {
            key: key.to_string(),
        })
    }

    /// Writes every entry under `key_prefix` whose value changed and, with
    /// `prune`, deletes the keys under it that no entry names.
    async fn put_entries(
        &self,
        config: &KvStore,
        data: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let PreparedEntries { prefix, entries } = prepare_entries(config, data)?;
        let _prune_guard = match config.prune {
            true => Some(self.prune_lock.lock().await),
            false => None,
        };

        let existing: HashSet<String> = self.store.list_keys(prefix).await?.into_iter().collect();
        let mut result = PutEntriesResult::default();
        let mut wanted = HashSet::with_capacity(entries.len());
        for (key, value) in entries {
            let current = match existing.contains(&key) {
                true => self.store.get(&key).await?,
                false => None,
            };
            match current {
                Some(current) if current.as_ref() == value.as_bytes() => result.unchanged += 1,
                _ => {
                    self.store
                        .put(&key, bytes::Bytes::from(value), self.ttl_secs())
                        .await?;
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
                self.store.delete(&key).await?;
                result.deleted.push(key);
            }
        }
        to_json(&result)
    }

    async fn get(&self, config: &KvStore) -> Result<serde_json::Value, Error> {
        let key = valid_key(
            config
                .key
                .as_deref()
                .ok_or(Error::MissingKey { operation: "get" })?,
        )?;
        let content = self
            .store
            .get(key)
            .await?
            .map(|value| String::from_utf8_lossy(&value).into_owned());
        to_json(&GetResult {
            key: key.to_string(),
            found: content.is_some(),
            content,
        })
    }

    async fn list(&self, config: &KvStore) -> Result<serde_json::Value, Error> {
        let (prefix, keys) = match config.key_prefix.as_deref() {
            Some(prefix) => (prefix, self.store.list_keys(prefix).await?),
            None => ("", self.store.list_keys("").await?),
        };
        let values = match config.include_values {
            true => {
                let mut values = HashMap::with_capacity(keys.len());
                for key in &keys {
                    if let Some(value) = self.store.get(key).await? {
                        if let Ok(text) = String::from_utf8(value.to_vec()) {
                            values.insert(key.clone(), text);
                        }
                    }
                }
                Some(values)
            }
            false => None,
        };
        to_json(&ListResult {
            count: keys.len(),
            keys,
            prefix: prefix.to_string(),
            values,
        })
    }

    async fn delete(&self, config: &KvStore) -> Result<serde_json::Value, Error> {
        let key = valid_key(config.key.as_deref().ok_or(Error::MissingKey {
            operation: "delete",
        })?)?;
        self.store.delete(key).await?;
        to_json(&KeyResult {
            key: key.to_string(),
        })
    }
}

/// Opens `bucket`; tasks on the same NATS server and credentials share one
/// connection through `clients`.
pub(crate) async fn open(
    config: Bucket<'_>,
    clients: &flowgen_core::client_registry::ClientRegistry,
    task_type: &'static str,
) -> Result<Arc<dyn Cache>, Error> {
    match backend(config.url, config.credentials_path.as_deref())? {
        Backend::Redb(path) => {
            let (path, bucket) = (path.to_path_buf(), config.name.to_string());
            let opened = tokio::task::spawn_blocking(move || RedbCache::open(&path, &bucket))
                .await
                .map_err(|source| CacheError::OpenFailed(Box::new(source)))??;
            Ok(Arc::new(opened))
        }
        Backend::Nats(url) => {
            let mut builder = flowgen_nats::cache::CacheBuilder::new()
                .url(url.to_string())
                .history(config.history);
            if let Some(path) = &config.credentials_path {
                builder = builder.credentials_path(path.clone());
            }
            let key = flowgen_core::client_registry::ClientKeyBuilder::new(task_type)
                .field("credentials_path", &config.credentials_path)
                .field("url", &config.url)
                .build();
            let jetstream = clients
                .get_or_init(key, || {
                    let credentials_path = config.credentials_path.clone();
                    let url = url.to_string();
                    async move {
                        let mut client = flowgen_nats::client::ClientBuilder::new();
                        client.url(url);
                        if let Some(path) = credentials_path {
                            client.credentials_path(path);
                        }
                        let connected = client
                            .build()
                            .map_err(|source| flowgen_nats::cache::Error::ClientAuth { source })?
                            .connect()
                            .await
                            .map_err(|source| flowgen_nats::cache::Error::ClientAuth { source })?;
                        connected
                            .jetstream
                            .ok_or(flowgen_nats::cache::Error::MissingJetStreamContext)
                    }
                })
                .await
                .map_err(|error| match error {
                    flowgen_core::client_registry::Error::Init { source } => Error::Nats { source },
                    flowgen_core::client_registry::Error::TypeMismatch => {
                        Error::ClientRegistryMismatch
                    }
                })?;
            let cache = builder
                .build()
                .map_err(|source| Error::Nats { source })?
                .init_on((*jetstream).clone(), config.name)
                .await
                .map_err(|source| Error::Nats { source })?;
            if config.ttl && !cache.allows_ttl() {
                return Err(Error::TtlNotAllowed {
                    bucket: config.name.to_string(),
                });
            }
            Ok(Arc::new(cache))
        }
    }
}

/// Key-value store processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<KvStore>,
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

    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self
            .config
            .render(&serde_json::Value::Object(serde_json::Map::new()))
            .map_err(|source| Error::ConfigRender { source })?;
        Ok(EventHandler {
            config: Arc::clone(&self.config),
            store: open(
                config.bucket(),
                &self.task_context.client_registry,
                self.task_type,
            )
            .await?,
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
            prune_lock: tokio::sync::Mutex::new(()),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let handler =
            match flowgen_core::task::runner::init(&self.task_context, &retry_config, || async {
                self.init().await.map_err(|e| {
                    error!(error = %e, "Failed to initialize key-value store processor");
                    match e.is_permanent() {
                        true => tokio_retry::RetryError::permanent(e),
                        false => tokio_retry::RetryError::transient(e),
                    }
                })
            })
            .await?
            {
                Some(handler) => Arc::new(handler),
                None => return Ok(()),
            };

        flowgen_core::task::runner::process_events(
            self.rx,
            handler,
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
    }
}

/// Builder for the key-value store processor.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<KvStore>>,
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

    pub fn config(mut self, config: Arc<KvStore>) -> Self {
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
    use flowgen_core::cache::WatchEvent;
    use flowgen_core::task::runner::Handler;
    use futures_util::StreamExt;
    use serde_json::json;

    const BUCKET: &str = "a";

    fn config(url: &str, fields: serde_json::Value) -> KvStore {
        let mut config = json!({"name": "kv", "url": url, "bucket": BUCKET, "operation": "put"});
        if let (Some(config), serde_json::Value::Object(fields)) = (config.as_object_mut(), fields)
        {
            config.extend(fields);
        }
        serde_json::from_value(config).unwrap()
    }

    fn context() -> Arc<flowgen_core::task::context::TaskContext> {
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::MemoryCache::new()) as Arc<dyn Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("f".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    /// Runs `config` on `data` and returns the event it emits.
    async fn run(config: KvStore, data: serde_json::Value) -> Result<serde_json::Value, Error> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let task_context = context();
        let handler = EventHandler {
            store: open(config.bucket(), &task_context.client_registry, "kv_store").await?,
            config: Arc::new(config),
            tx: Some(tx),
            task_id: 0,
            task_type: "kv_store",
            task_context,
            prune_lock: tokio::sync::Mutex::new(()),
        };
        let event = EventBuilder::new()
            .data(EventData::Json(data))
            .subject("s".to_string())
            .task_id(0)
            .task_type("generate")
            .build()
            .unwrap();
        handler.handle(event).await?;
        Ok(rx.recv().await.unwrap().data_as_json().unwrap())
    }

    #[test]
    fn the_url_names_the_store_and_only_nats_takes_credentials() {
        let backend = |url: &str, fields| match config(url, fields).backend() {
            Ok(Backend::Redb(path)) => format!("redb {}", path.display()),
            Ok(Backend::Nats(url)) => format!("nats {url}"),
            Err(error) => error.to_string(),
        };
        assert_eq!(
            backend("redb://.flowgen/state.redb", json!({})),
            "redb .flowgen/state.redb"
        );
        assert_eq!(
            backend(
                "nats://localhost:4222",
                json!({"credentials_path": "n.json"})
            ),
            "nats nats://localhost:4222"
        );
        assert_eq!(
            backend("nats.example.com:4222", json!({})),
            "nats nats.example.com:4222"
        );
        assert_eq!(
            backend("{{env.NATS_URL}}", json!({})),
            "nats {{env.NATS_URL}}"
        );
        assert_eq!(
            backend("redb://a.redb", json!({"credentials_path": "n.json"})),
            ConfigError::RedbCredentials.to_string()
        );
        assert_eq!(
            backend("redb://", json!({})),
            ConfigError::MissingPath.to_string()
        );
        assert_eq!(
            backend("rebd://a.redb", json!({})),
            ConfigError::UnsupportedScheme {
                url: "rebd://a.redb".to_string()
            }
            .to_string()
        );
        assert_eq!(backend("tls://n:4222", json!({})), "nats tls://n:4222");
    }

    #[test]
    fn a_config_is_checked_before_it_runs_and_templates_once_rendered() {
        let check = |fields| match config("redb://a.redb", fields).check_config() {
            Ok(()) => "ok".to_string(),
            Err(error) => error.to_string(),
        };
        assert_eq!(
            check(json!({"operation": "get"})),
            ConfigError::MissingKey { operation: "get" }.to_string()
        );
        assert_eq!(
            check(json!({"operation": "delete"})),
            ConfigError::MissingKey {
                operation: "delete"
            }
            .to_string()
        );
        assert_eq!(
            check(json!({})),
            ConfigError::MissingKeyOrPrefix.to_string()
        );
        assert_eq!(
            check(json!({"key": "flows."})),
            ConfigError::InvalidKey {
                key: "flows.".to_string()
            }
            .to_string()
        );
        assert_eq!(
            check(json!({"key_prefix": "flows", "prune": true})),
            ConfigError::UnsafePrunePrefix {
                prefix: "flows".to_string()
            }
            .to_string()
        );
        assert_eq!(
            check(json!({"key": "a", "nats": {"history": 10}})),
            "`nats` options don't apply to redb://a.redb"
        );
        let nats = |fields| match config("nats://a:4222", fields).check_config() {
            Ok(()) => "ok".to_string(),
            Err(error) => error.to_string(),
        };
        for history in [0, 65] {
            assert_eq!(
                nats(json!({"key": "a", "nats": {"history": history}})),
                ConfigError::InvalidHistory { history }.to_string()
            );
        }
        assert_eq!(nats(json!({"key": "a", "nats": {"history": 64}})), "ok");
        assert_eq!(check(json!({"key": "flows.{{event.data.path}}"})), "ok");
        assert_eq!(check(json!({"operation": "list"})), "ok");
        assert_eq!(check(json!({"key_prefix": "flows.", "prune": true})), "ok");
    }

    #[tokio::test]
    async fn a_key_no_store_accepts_fails_the_event_for_good() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("redb://{}", dir.path().join("a.redb").display());
        let entries = json!({"entries": [{"key": "a", "value": "1"}, {"key": "", "value": "2"}]});

        let empty = run(config(&url, json!({"key_prefix": "flows."})), entries).await;
        let templated = run(
            config(&url, json!({"key": "{{event.data.path}}"})),
            json!({"path": "a b", "content": "x"}),
        )
        .await;

        assert!(matches!(&empty, Err(Error::InvalidKey { key }) if key == "flows."));
        assert!(matches!(&templated, Err(Error::InvalidKey { key }) if key == "a b"));
        assert!(empty.as_ref().is_err_and(|error| error.is_permanent()));
    }

    #[test]
    fn the_old_nats_kv_store_shape_loads() {
        let config: KvStore = serde_json::from_value(json!({
            "name": "kv",
            "credentials_path": "/etc/nats/credentials.json",
            "url": "{{env.NATS_URL}}",
            "bucket": "flowgen_system",
            "operation": "list",
            "key_prefix": "flows.",
            "include_values": true
        }))
        .unwrap();
        assert!(matches!(config.backend(), Ok(Backend::Nats(_))));
        assert_eq!(config.operation, Operation::List);
        assert!(serde_json::from_value::<KvStore>(json!({
            "name": "kv", "url": "u", "bucket": "b", "operation": "watch"
        }))
        .is_err());
    }

    #[test]
    fn prune_needs_a_prefix_ending_in_a_separator_and_entries_unless_allowed() {
        let entries = |config: &KvStore, entries: serde_json::Value| {
            prepare_entries(config, json!({ "entries": entries })).map(|prepared| prepared.entries)
        };
        let prune = |prefix: &str, allow_empty: bool| {
            config(
                "redb://a",
                json!({"key_prefix": prefix, "prune": true, "allow_empty": allow_empty}),
            )
        };
        let one = json!([{"key": "a", "value": "1"}]);

        assert!(matches!(
            entries(&prune("flows", false), one.clone()),
            Err(Error::UnsafePrunePrefix { .. })
        ));
        assert!(matches!(
            entries(&prune("", false), one.clone()),
            Err(Error::UnsafePrunePrefix { .. })
        ));
        assert!(entries(&prune("flows.", false), one.clone()).is_ok());
        assert!(entries(&prune("flows/", false), one.clone()).is_ok());
        assert!(matches!(
            entries(&prune("flows.", false), json!([])),
            Err(Error::EmptyPrune { .. })
        ));
        assert!(entries(&prune("flows.", true), json!([])).is_ok());
        let without_prune = config("redb://a", json!({"key_prefix": "flows"}));
        assert!(entries(&without_prune, one.clone()).is_ok());
        let without_prefix = config("redb://a", json!({}));
        assert!(matches!(
            entries(&without_prefix, one),
            Err(Error::MissingKeyPrefix)
        ));
    }

    #[test]
    fn entry_values_that_are_not_strings_are_stored_as_json() {
        let config = config("redb://a", json!({"key_prefix": "p."}));
        let prepared = prepare_entries(
            &config,
            json!({"entries": [
                {"key": "s", "value": "text"},
                {"key": "o", "value": {"a": 1}}
            ]}),
        )
        .unwrap();
        assert_eq!(
            prepared.entries,
            vec![
                ("p.s".to_string(), "text".to_string()),
                ("p.o".to_string(), r#"{"a":1}"#.to_string())
            ]
        );
    }

    #[test]
    fn only_config_and_input_errors_are_permanent() {
        let permanent = [
            Error::Config(ConfigError::MissingPath),
            Error::MissingKey { operation: "get" },
            Error::MissingContent,
            Error::MissingKeyPrefix,
            Error::EmptyPrune {
                prefix: "p.".to_string(),
            },
        ];
        for error in permanent {
            assert!(error.is_permanent(), "{error}");
        }
        let transient = Error::Store(CacheError::PutFailed("down".into()));
        assert!(!transient.is_permanent());
    }

    #[tokio::test]
    async fn entries_are_written_once_and_prune_deletes_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("{REDB_SCHEME}{}", dir.path().join("s.redb").display());
        let mirror = config(&url, json!({"key_prefix": "flows.", "prune": true}));
        let entries = |keys: &[&str]| {
            let entries: Vec<_> = keys
                .iter()
                .map(|key| json!({"key": key, "value": format!("v{key}")}))
                .collect();
            json!({ "entries": entries })
        };

        let first = run(mirror.clone(), entries(&["a", "b"])).await.unwrap();
        assert_eq!(first["put"], json!(["flows.a", "flows.b"]));
        let second = run(mirror.clone(), entries(&["a"])).await.unwrap();
        assert_eq!(second["unchanged"], 1);
        assert_eq!(second["deleted"], json!(["flows.b"]));

        let listed = run(
            config(
                &url,
                json!({"operation": "list", "key_prefix": "flows.", "include_values": true}),
            ),
            json!({}),
        )
        .await
        .unwrap();
        assert_eq!(listed["keys"], json!(["flows.a"]));
        assert_eq!(listed["values"]["flows.a"], "va");

        let empty = run(mirror, entries(&[])).await;
        assert!(matches!(empty, Err(Error::EmptyPrune { .. })));
    }

    #[tokio::test]
    async fn one_key_is_put_read_and_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("{REDB_SCHEME}{}", dir.path().join("s.redb").display());
        let key = |operation: &str| config(&url, json!({"operation": operation, "key": "k"}));

        run(key("put"), json!({"content": "v"})).await.unwrap();
        let found = run(key("get"), json!({})).await.unwrap();
        assert_eq!(found, json!({"key": "k", "content": "v", "found": true}));
        run(key("delete"), json!({})).await.unwrap();
        let missing = run(key("get"), json!({})).await.unwrap();
        assert_eq!(missing["found"], false);

        let no_content = run(key("put"), json!({})).await;
        assert!(matches!(no_content, Err(Error::MissingContent)));
    }

    #[tokio::test]
    async fn a_write_reaches_a_watcher_of_the_same_file_and_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        let watcher = RedbCache::open(&path, BUCKET).unwrap();
        let mut events = watcher.watch("flows.", false).await.unwrap();

        let url = format!("{REDB_SCHEME}{}", path.display());
        run(
            config(&url, json!({"key": "flows.a"})),
            json!({"content": "v"}),
        )
        .await
        .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(event, WatchEvent::Put { key, .. } if key == "flows.a"));
    }
}
