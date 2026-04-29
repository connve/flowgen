//! NATS JetStream Key-Value store processor.
//!
//! Provides get, put, list, and delete operations on a NATS JetStream
//! Key-Value bucket. Each operation receives an event, performs the KV
//! operation, and emits the result as a downstream event.

use flowgen_core::client::Client as FlowgenClientTrait;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use serde::{Deserialize, Serialize};
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
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    /// Task name.
    pub name: String,
    /// Path to credentials file containing NATS authentication details.
    pub credentials_path: PathBuf,
    /// NATS server URL. Defaults to "localhost:4222".
    #[serde(default = "default_nats_url")]
    pub url: String,
    /// KV bucket name.
    pub bucket: String,
    /// KV operation to perform.
    pub operation: Operation,
    /// Key for get, put, and delete operations (supports templating).
    pub key: Option<String>,
    /// Key prefix for list operations (supports templating).
    pub key_prefix: Option<String>,
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
    /// Write a value. Uses `event.data.content` if present, otherwise full event data.
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

/// Result of a KV list operation.
#[derive(Debug, Serialize)]
pub struct ListResult {
    /// Keys matching the prefix.
    pub keys: Vec<String>,
    /// Number of keys found.
    pub count: usize,
    /// Prefix that was used for filtering.
    pub prefix: String,
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
}

impl EventHandler {
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            let event_data = event.data_as_json().unwrap_or_default();
            let rendered = self
                .config
                .render(&event_data)
                .map_err(|source| Error::ConfigRender { source })?;

            let result = match rendered.operation {
                Operation::Put => self.handle_put(&rendered, &event_data).await?,
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
                    // Leaf task: signal completion.
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
        config: &Config,
        event_data: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let key = config.key.as_deref().ok_or_else(|| Error::MissingKey {
            operation: "put".to_string(),
        })?;

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

        use futures_util::StreamExt;
        let mut keys = Vec::new();
        let mut key_stream = self
            .store
            .keys()
            .await
            .map_err(|source| Error::KvKeys { source })?;

        while let Some(Ok(key)) = key_stream.next().await {
            if key.starts_with(prefix) {
                keys.push(key);
            }
        }

        let result = ListResult {
            count: keys.len(),
            keys,
            prefix: prefix.to_string(),
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

    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let client = crate::client::ClientBuilder::new()
            .credentials_path(config.credentials_path.clone())
            .url(config.url.clone())
            .build()
            .map_err(|source| Error::Client { source })?
            .connect()
            .await
            .map_err(|source| Error::Client { source })?;

        let jetstream = client.jetstream.ok_or(Error::MissingJetStream)?;

        let store = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: config.bucket.clone(),
                ..Default::default()
            })
            .await
            .map_err(|source| Error::KvBucketCreate { source })?;

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            store,
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize NATS KV store processor.");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => return Err(e),
        };

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    tokio::spawn(async move {
                        let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                            match handler.handle(event.clone()).await {
                                Ok(()) => Ok(()),
                                Err(e) => {
                                    error!(error = %e, "KV store operation failed.");
                                    Err(tokio_retry::RetryError::transient(e))
                                }
                            }
                        })
                        .await;

                        if let Err(e) = result {
                            error!(error = %e, "KV store operation exhausted all retry attempts.");
                        }
                    });
                }
                None => return Ok(()),
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
