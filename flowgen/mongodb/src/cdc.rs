//! The `mongodb_cdc` task: change data capture from a database, read through
//! a MongoDB change stream.

use super::message::MongoEventsExt;
use crate::client::MongoClientBuilder;
use flowgen_core::config::ConfigExt;
use flowgen_core::delivery::{Delivered, Delivery};
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::task::TaskConfig;
use futures_util::{StreamExt, TryStreamExt};
use mongodb::options::FullDocumentType;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{error, info, warn};

/// MongoDB Change Data Capture reader configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Cdc {
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Path to credentials file containing MongoDB authentication details.
    /// Omit to connect to `localhost:27017` without authentication.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// The unique name / identifier of the task.
    pub name: String,
    /// The Database Name from MongoDB.
    pub db_name: String,
    /// How long to wait for the flow to complete a change. Waits
    /// indefinitely when omitted.
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<std::time::Duration>,
    /// How many times a change is sent through the flow before it is skipped.
    /// Delivered until the flow completes it when omitted.
    #[serde(default)]
    pub max_deliver: Option<std::num::NonZeroU32>,
    /// Emits every document of the database's collections before the
    /// changes. Changes made during the copy are emitted after it as well.
    #[serde(default)]
    pub initial_copy: bool,
    /// Documents per event of the initial copy; defaults to 1000.
    #[serde(default)]
    pub copy_batch_size: Option<std::num::NonZeroUsize>,
    /// Optional retry configuration for connecting and redelivery (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Cdc {}

impl Cdc {
    /// Validates the config after any templating has been applied.
    pub fn validate(&self) -> Result<(), Error> {
        match self.ack_timeout {
            Some(ack_timeout) if ack_timeout.is_zero() => Err(Error::ZeroAckTimeout),
            _ => Ok(()),
        }
    }
}

fn default_copy_batch_size() -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::MIN.saturating_add(999)
}

/// How long the stream waits without a change before storing its position,
/// so the stored resume token stays within the oplog on a quiet database.
const IDLE_POSITION_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Errors that can occur during MongoDB change stream operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: crate::client::Error,
    },
    #[error("Send event message error: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Event error: {source}")]
    Event {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("MongoDB error: {source}")]
    MongoDB {
        #[source]
        source: mongodb::error::Error,
    },
    #[error("Message conversion failed with error: {source}")]
    MessageConversion {
        #[source]
        source: crate::message::Error,
    },
    #[error("Stream ended unexpectedly")]
    StreamEnded,
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Cache error for resume token key '{key}': {source}")]
    Cache {
        key: String,
        #[source]
        source: flowgen_core::cache::Error,
    },
    #[error("Stored resume token under '{key}' is invalid: {source}")]
    InvalidResumeToken {
        key: String,
        #[source]
        source: mongodb::bson::de::Error,
    },
    #[error("Resume token serialization error: {source}")]
    ResumeTokenEncode {
        #[source]
        source: mongodb::bson::ser::Error,
    },
    #[error("The server returned no position to start the change stream from")]
    MissingStartPosition,
    #[error("The initial copy stopped before it finished")]
    CopyInterrupted,
    #[error("The change stream was invalidated")]
    Invalidated,
    #[error("Ack timeout must be greater than zero")]
    ZeroAckTimeout,
}

/// The resume token as stored in the cache.
#[derive(Serialize, Deserialize)]
struct StoredResumeToken {
    token: mongodb::change_stream::event::ResumeToken,
}

/// Cache key holding the resume token after the last completed change.
fn resume_token_key(flow_key: &str, db_name: &str) -> String {
    format!("flow.{flow_key}.mongodb_resume_token.{db_name}")
}

/// Whether the server no longer holds the position of a resume token
/// (`ChangeStreamHistoryLost`).
fn history_lost(error: &mongodb::error::Error) -> bool {
    matches!(
        error.kind.as_ref(),
        mongodb::error::ErrorKind::Command(command) if command.code == 286
    )
}

type ChangeEvent = mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>;

/// Change metadata the event has no field for, merged into `event.meta`.
#[derive(Debug, serde::Serialize)]
struct ChangeMeta<'a, O> {
    database: Option<&'a str>,
    operation_type: O,
    document_key: Option<&'a mongodb::bson::Document>,
}

impl<O: Serialize> ChangeMeta<'_, O> {
    fn into_map(self) -> Result<serde_json::Map<String, serde_json::Value>, Error> {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::Object(meta)) => Ok(meta),
            Ok(_) => Ok(serde_json::Map::new()),
            Err(source) => Err(Error::SerdeJson { source }),
        }
    }
}

/// Where a change lands on the event: its subject, id, and meta.
#[derive(Debug)]
struct ChangeDetails {
    collection: Option<String>,
    id: String,
    meta: serde_json::Map<String, serde_json::Value>,
}

/// Key under a resume token holding its value.
const RESUME_TOKEN_DATA_KEY: &str = "_data";

fn change_details(change_event: &ChangeEvent) -> Result<ChangeDetails, Error> {
    let (database, collection) = match &change_event.ns {
        Some(ns) => (Some(ns.db.as_str()), ns.coll.clone()),
        None => (None, None),
    };
    let meta = ChangeMeta {
        database,
        operation_type: &change_event.operation_type,
        document_key: change_event.document_key.as_ref(),
    }
    .into_map()?;
    Ok(ChangeDetails {
        collection,
        id: token_id(&change_event.id)?,
        meta,
    })
}

/// The event id for a resume token: its `_data`, or the whole token as JSON.
fn token_id(token: &mongodb::change_stream::event::ResumeToken) -> Result<String, Error> {
    let token = serde_json::to_value(token).map_err(|source| Error::SerdeJson { source })?;
    match token.get(RESUME_TOKEN_DATA_KEY) {
        Some(serde_json::Value::String(data)) => Ok(data.clone()),
        _ => Ok(token.to_string()),
    }
}

/// The change's payload: the document when the change carries one, the
/// document key for a delete, and nothing otherwise.
fn change_document(change_event: &ChangeEvent) -> Option<&mongodb::bson::Document> {
    match (
        &change_event.full_document,
        &change_event.operation_type,
        &change_event.document_key,
    ) {
        (Some(document), _, _) => Some(document),
        (None, mongodb::change_stream::event::OperationType::Delete, Some(key)) => Some(key),
        _ => None,
    }
}

/// Event handler that watches a MongoDB change stream and forwards events downstream.
#[derive(Debug)]
pub struct EventHandler {
    /// Change stream configuration.
    config: Arc<Cdc>,
    client: Arc<mongodb::Client>,
    /// Same key `init()` used to cache the MongoDB client, so `run()` can
    /// evict it if the cached client turns out to be permanently broken.
    client_key: flowgen_core::client_registry::ClientKey,
    /// Sets the delay before a failed change is delivered again.
    retry_config: flowgen_core::retry::RetryConfig,
    /// Current task identifier.
    task_id: usize,
    /// Optional channel sender for downstream events.
    tx: Option<Sender<Event>>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    async fn stored_resume_token(
        &self,
        key: &str,
    ) -> Result<Option<mongodb::change_stream::event::ResumeToken>, Error> {
        let value = self
            .task_context
            .cache
            .get(key)
            .await
            .map_err(|source| Error::Cache {
                key: key.to_string(),
                source,
            })?;
        match value {
            None => Ok(None),
            Some(bytes) => mongodb::bson::from_slice::<StoredResumeToken>(&bytes)
                .map(|stored| Some(stored.token))
                .map_err(|source| Error::InvalidResumeToken {
                    key: key.to_string(),
                    source,
                }),
        }
    }

    async fn store_resume_token(
        &self,
        key: &str,
        token: &mongodb::change_stream::event::ResumeToken,
    ) -> Result<(), Error> {
        let bytes = mongodb::bson::to_vec(&StoredResumeToken {
            token: token.clone(),
        })
        .map_err(|source| Error::ResumeTokenEncode { source })?;
        self.task_context
            .cache
            .put(key, bytes.into(), None)
            .await
            .map_err(|source| Error::Cache {
                key: key.to_string(),
                source,
            })
    }

    async fn delete_resume_token(&self, key: &str) -> Result<(), Error> {
        self.task_context
            .cache
            .delete(key)
            .await
            .map_err(|source| Error::Cache {
                key: key.to_string(),
                source,
            })
    }

    /// Opens the change stream after the last completed change. Without one,
    /// or when the server no longer holds it, the stream starts now, after
    /// the initial copy when it is enabled.
    async fn open(
        &self,
        key: &str,
    ) -> Result<mongodb::change_stream::ChangeStream<ChangeEvent>, Error> {
        let db = self.client.database(&self.config.db_name);
        let token = match self.stored_resume_token(key).await {
            Err(Error::InvalidResumeToken { source, .. }) => {
                warn!(error = %source, "The stored resume token is unreadable");
                self.delete_resume_token(key).await?;
                None
            }
            token => token?,
        };
        if let Some(token) = token {
            match db
                .watch()
                .full_document(FullDocumentType::UpdateLookup)
                .start_after(token)
                .await
            {
                Err(source) if history_lost(&source) => {
                    error!(error = %source, "Changes after the stored resume token are no longer in the oplog and are lost, restarting the change stream");
                    self.delete_resume_token(key).await?;
                }
                resumed => return resumed.map_err(|source| Error::MongoDB { source }),
            }
        }
        let watch = db.watch().full_document(FullDocumentType::UpdateLookup);
        match self.config.initial_copy {
            false => watch.await,
            true => {
                let start = self.copy(&db).await?;
                self.store_resume_token(key, &start).await?;
                watch.start_after(start).await
            }
        }
        .map_err(|source| Error::MongoDB { source })
    }

    /// Emits every document of the database's collections, `copy_batch_size`
    /// documents per event and up to `max_in_flight` events in the flow at
    /// once, and returns the position from before the copy, where the change
    /// stream continues.
    async fn copy(
        &self,
        db: &mongodb::Database,
    ) -> Result<mongodb::change_stream::event::ResumeToken, Error> {
        let mongo = |source| Error::MongoDB { source };
        let start = db
            .watch()
            .await
            .map_err(mongo)?
            .resume_token()
            .ok_or(Error::MissingStartPosition)?;
        let position = token_id(&start)?;
        let delivery = Delivery {
            downstream: self.tx.is_some(),
            leaf_count: self.task_context.leaf_count,
            ack_timeout: self.config.ack_timeout,
            max_deliver: self.config.max_deliver,
            retry: &self.retry_config,
            cancellation: &self.task_context.cancellation_token,
        };
        let collections = db
            .list_collection_names()
            .filter(mongodb::bson::doc! { "type": "collection" })
            .await
            .map_err(mongo)?;
        for collection in collections
            .iter()
            .filter(|name| !name.starts_with("system."))
        {
            info!(collection, "Copying collection");
            let documents = db
                .collection::<mongodb::bson::Document>(collection)
                .find(mongodb::bson::Document::new())
                .no_cursor_timeout(true)
                .await
                .map_err(mongo)?;
            let delivered = delivery.deliver_each(
                self.copied_batches(documents, collection, &position),
                self.task_context.max_in_flight,
                self.tx.as_ref(),
                |source| Error::SendMessage { source },
            );
            let mut delivered = std::pin::pin!(delivered);
            while let Some(delivered) = delivered.try_next().await? {
                if delivered == Delivered::Stopped {
                    return Err(Error::CopyInterrupted);
                }
            }
        }
        info!("Initial copy finished");
        Ok(start)
    }

    /// The events of the initial copy of `collection`: one per
    /// `copy_batch_size` documents of the cursor, with the documents as an
    /// array.
    fn copied_batches<'a>(
        &'a self,
        documents: mongodb::Cursor<mongodb::bson::Document>,
        collection: &'a str,
        position: &'a str,
    ) -> impl futures_util::Stream<Item = Result<Event, Error>> + Send + 'a {
        let batch_size = match self.config.copy_batch_size {
            Some(batch_size) => batch_size,
            None => default_copy_batch_size(),
        };
        documents
            .chunks(batch_size.get())
            .enumerate()
            .map(move |(index, documents)| {
                self.copied_event(
                    collection,
                    documents,
                    format!("{position}-copy-{collection}-{}", index + 1),
                )
            })
    }

    /// The event for a batch of documents of the initial copy.
    fn copied_event(
        &self,
        collection: &str,
        documents: Vec<mongodb::error::Result<mongodb::bson::Document>>,
        id: String,
    ) -> Result<Event, Error> {
        let data = documents
            .into_iter()
            .map(|document| {
                let document = document.map_err(|source| Error::MongoDB { source })?;
                serde_json::to_value(document).map_err(|source| Error::SerdeJson { source })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let meta = ChangeMeta {
            database: Some(&self.config.db_name),
            operation_type: "copy",
            document_key: None,
        }
        .into_map()?;
        let mut event = EventBuilder::new()
            .data(EventData::Json(serde_json::Value::Array(data)))
            .subject(collection.to_string())
            .id(id)
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::Event { source })?;
        event.meta = Some(meta);
        Ok(event)
    }

    /// Watches the MongoDB change stream and sends each change through the
    /// flow, storing its resume token once the flow completes it. Returns
    /// after an `invalidate` change, since the server closes its stream.
    #[tracing::instrument(skip(self), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self) -> Result<(), Error> {
        let key = resume_token_key(&self.task_context.flow.id(), &self.config.db_name);
        let cancellation = &self.task_context.cancellation_token;
        let mut change_stream = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            opened = self.open(&key) => opened?,
        };
        let mut stored = None;
        let mut stored_at = tokio::time::Instant::now();

        loop {
            let next = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                next = change_stream.next_if_any() => next.map_err(|source| Error::MongoDB { source })?,
            };

            let change_event = match next {
                Some(change_event) => change_event,
                None if !change_stream.is_alive() => return Err(Error::StreamEnded),
                None if stored_at.elapsed() < IDLE_POSITION_INTERVAL => continue,
                None => {
                    stored_at = tokio::time::Instant::now();
                    match change_stream.resume_token() {
                        Some(position) if stored.as_ref() != Some(&position) => {
                            self.store_resume_token(&key, &position).await?;
                            stored = Some(position);
                        }
                        _ => {}
                    }
                    continue;
                }
            };

            match self.event(&change_event) {
                Ok(event) => {
                    let delivery = Delivery {
                        downstream: self.tx.is_some(),
                        leaf_count: self.task_context.leaf_count,
                        ack_timeout: self.config.ack_timeout,
                        max_deliver: self.config.max_deliver,
                        retry: &self.retry_config,
                        cancellation,
                    };
                    let delivered = delivery
                        .deliver(&event, |attempt| {
                            attempt.send_with_logging(self.tx.as_ref())
                        })
                        .await
                        .map_err(|source| Error::SendMessage { source })?;
                    if delivered == Delivered::Stopped {
                        return Ok(());
                    }
                }
                Err(e) => error!(error = %e, "Failed to read a change, skipping it"),
            }
            self.store_resume_token(&key, &change_event.id).await?;
            if change_event.operation_type
                == mongodb::change_stream::event::OperationType::Invalidate
            {
                return Err(Error::Invalidated);
            }
            stored = Some(change_event.id);
            stored_at = tokio::time::Instant::now();
        }
    }

    /// The event for a change: its document, collection, resume token and metadata.
    fn event(&self, change_event: &ChangeEvent) -> Result<Event, Error> {
        let mut event = match change_document(change_event) {
            Some(document) => document
                .to_event(self.task_type, self.task_id)
                .map_err(|source| Error::MessageConversion { source })?,
            None => EventBuilder::new()
                .subject(self.config.db_name.clone())
                .data(EventData::Json(serde_json::Value::Null))
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::Event { source })?,
        };
        let details = change_details(change_event)?;
        if let Some(collection) = details.collection {
            event.subject = collection;
        }
        event.id = Some(details.id);
        event
            .meta
            .get_or_insert_with(serde_json::Map::new)
            .extend(details.meta);
        Ok(event)
    }
}

/// MongoDB change stream reader that watches for real-time changes.
#[derive(Debug)]
pub struct Processor {
    /// Reader configuration settings.
    config: Arc<Cdc>,
    /// Optional channel sender for downstream events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event tracking.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the reader by establishing a MongoDB client connection.
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::Map::new())
            .map_err(|source| Error::ConfigRender { source })?;
        init_config.validate()?;

        let credentials_path = init_config.credentials_path.clone();
        let client_key =
            flowgen_core::client_registry::ClientKey::new(self.task_type, &credentials_path);
        let client = self
            .task_context
            .client_registry
            .get_or_init(client_key.clone(), || async {
                let mut builder = MongoClientBuilder::new();
                if let Some(path) = credentials_path {
                    builder = builder.credentials_path(path);
                }
                builder
                    .build()
                    .map_err(|e| Error::Auth { source: e })?
                    .connect()
                    .await
                    .map_err(|e| Error::Auth { source: e })
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        Ok(EventHandler {
            client,
            client_key,
            retry_config: flowgen_core::retry::RetryConfig::merge(
                &self.task_context.retry,
                &self.config.retry,
            ),
            config: Arc::new(init_config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let mut reconnect_backoff = retry_config.reconnect_strategy();

        // No inner spawn: run() must stay pending for the task's real
        // lifetime so the caller's JoinHandle/abort actually reaches this loop.
        loop {
            let init = tokio_retry::Retry::spawn(
                retry_config.init_strategy(self.task_context.startup_delay),
                || async {
                    match self.init().await {
                        Ok(handler) => Ok(handler),
                        Err(e) => {
                            error!(error = %e, "Failed to initialize change stream reader");
                            Err(tokio_retry::RetryError::transient(e))
                        }
                    }
                },
            );
            let initialized = tokio::select! {
                _ = self.task_context.cancellation_token.cancelled() => return Ok(()),
                result = init => result,
            };
            let event_handler = match initialized {
                Ok(handler) => {
                    reconnect_backoff = retry_config.reconnect_strategy();
                    handler
                }
                Err(e) => {
                    if self.task_context.cancellation_token.is_cancelled() {
                        return Ok(());
                    }
                    let delay = match reconnect_backoff.next() {
                        Some(d) => d,
                        None => retry_config.initial_backoff,
                    };
                    error!(
                        error = %e,
                        delay_ms = %delay.as_millis(),
                        "Change stream reader initialization exhausted retry attempts, will retry after backoff"
                    );
                    tokio::select! {
                        _ = self.task_context.cancellation_token.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(delay) => {}
                    }
                    continue;
                }
            };

            match event_handler.handle().await {
                _ if self.task_context.cancellation_token.is_cancelled() => return Ok(()),
                Ok(()) => warn!("Change stream ended unexpectedly, reconnecting"),
                Err(Error::Invalidated) => warn!("Change stream was invalidated, reopening it"),
                Err(e) => error!(error = %e, "Change stream error, reconnecting"),
            }

            self.task_context
                .client_registry
                .remove(&event_handler.client_key)
                .await;

            let delay = match reconnect_backoff.next() {
                Some(d) => d,
                None => retry_config.initial_backoff,
            };
            tokio::select! {
                _ = self.task_context.cancellation_token.cancelled() => return Ok(()),
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }
}

/// Builder for constructing Processor instances.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<Cdc>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<Cdc>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
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
    use mongodb::bson::doc;
    use std::path::PathBuf;
    use tokio::sync::mpsc;

    #[test]
    fn test_change_details() {
        let change_event: ChangeEvent = mongodb::bson::from_document(doc! {
            "_id": { "_data": "token" },
            "operationType": "insert",
            "ns": { "db": "d", "coll": "c" },
            "documentKey": { "_id": 1 },
            "fullDocument": { "_id": 1 },
        })
        .unwrap();

        let details = change_details(&change_event).unwrap();

        assert_eq!(details.collection.as_deref(), Some("c"));
        assert_eq!(details.id, "token");
        assert_eq!(details.meta["database"], "d");
        assert_eq!(details.meta["operation_type"], "insert");
        assert_eq!(details.meta["document_key"], serde_json::json!({"_id": 1}));
    }

    #[test]
    fn test_change_document() {
        let change = |fields: mongodb::bson::Document| -> ChangeEvent {
            let mut event = doc! { "_id": { "_data": "token" }, "documentKey": { "_id": 1 } };
            event.extend(fields);
            mongodb::bson::from_document(event).unwrap()
        };
        let delete = change(doc! { "operationType": "delete" });
        let update_without_document = change(doc! { "operationType": "update" });
        let update =
            change(doc! { "operationType": "update", "fullDocument": { "_id": 1, "n": 2 } });

        assert_eq!(change_document(&delete), Some(&doc! { "_id": 1 }));
        assert_eq!(change_document(&update_without_document), None);
        assert_eq!(change_document(&update), Some(&doc! { "_id": 1, "n": 2 }));
    }

    fn create_mock_config() -> Cdc {
        Cdc {
            name: "test_cdc".to_string(),
            db_name: "test_database".to_string(),
            credentials_path: Some(PathBuf::from("/mock/path/credentials.json")),
            depends_on: Some(Vec::new()),
            retry: Default::default(),
            ..Default::default()
        }
    }

    fn mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_error_display_auth() {
        let err = Error::Auth {
            source: crate::client::Error::CredentialsFileRead {
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
            },
        };
        assert!(matches!(err, Error::Auth { .. }));
    }

    #[test]
    fn test_error_display_send_message() {
        let err = Error::SendMessage {
            source: flowgen_core::event::Error::SendMessage,
        };
        assert_eq!(
            err.to_string(),
            "Send event message error: Error sending event to channel (receiver dropped)"
        );
    }

    #[test]
    fn test_error_display_event() {
        let err = Error::Event {
            source: flowgen_core::event::Error::SendMessage,
        };
        assert_eq!(
            err.to_string(),
            "Event error: Error sending event to channel (receiver dropped)"
        );
    }

    #[test]
    fn test_error_display_missing_builder_attribute() {
        let err = Error::MissingBuilderAttribute("test_field".to_string());
        assert_eq!(err.to_string(), "Missing required attribute: test_field");
    }

    #[test]
    fn test_error_display_retry_exhausted() {
        let inner = Box::new(Error::MissingBuilderAttribute("inner".to_string()));
        let err = Error::RetryExhausted { source: inner };
        assert_eq!(
            err.to_string(),
            "Task failed after all retry attempts: Missing required attribute: inner"
        );
    }

    #[test]
    fn test_error_display_mongodb() {
        let mongo_err = mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        let err = Error::MongoDB { source: mongo_err };
        assert!(err.to_string().contains("MongoDB error:"));
    }

    #[test]
    fn test_error_display_message_conversion() {
        let err = Error::MessageConversion {
            source: crate::message::Error::NoRecordBatch(),
        };
        assert_eq!(
            err.to_string(),
            "Message conversion failed with error: Error getting record batch"
        );
    }

    #[test]
    fn test_validate_rejects_a_zero_ack_timeout() {
        let config = |ack_timeout| Cdc {
            ack_timeout,
            ..create_mock_config()
        };

        assert!(matches!(
            config(Some(std::time::Duration::ZERO)).validate(),
            Err(Error::ZeroAckTimeout)
        ));
        assert!(config(Some(std::time::Duration::from_secs(1)))
            .validate()
            .is_ok());
        assert!(config(None).validate().is_ok());
    }

    #[tokio::test]
    async fn test_init_rejects_a_zero_ack_timeout() {
        use flowgen_core::task::runner::Runner;

        let reader = ProcessorBuilder::new()
            .config(Arc::new(Cdc {
                ack_timeout: Some(std::time::Duration::ZERO),
                ..create_mock_config()
            }))
            .task_context(mock_task_context())
            .task_type("zero_ack_timeout")
            .build()
            .await
            .unwrap();

        assert!(matches!(
            reader.init().await.unwrap_err(),
            Error::ZeroAckTimeout
        ));
    }

    #[test]
    fn test_error_display_stream_ended() {
        let err = Error::StreamEnded;
        assert_eq!(err.to_string(), "Stream ended unexpectedly");
    }

    #[test]
    fn test_error_from_mongodb() {
        let source = mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        let err = Error::MongoDB { source };
        assert!(err.to_string().contains("MongoDB error:"));
    }

    #[tokio::test]
    async fn test_builder_missing_config() {
        let result = ProcessorBuilder::new()
            .task_context(mock_task_context())
            .task_type("test")
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(ref attr) if attr == "config"
        ));
    }

    #[tokio::test]
    async fn test_builder_missing_task_context() {
        let config = Arc::new(create_mock_config());
        let result = ProcessorBuilder::new()
            .config(config)
            .task_type("test")
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(ref attr) if attr == "task_context"
        ));
    }

    #[tokio::test]
    async fn test_builder_missing_task_type() {
        let config = Arc::new(create_mock_config());
        let result = ProcessorBuilder::new()
            .config(config)
            .task_context(mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(ref attr) if attr == "task_type"
        ));
    }

    #[tokio::test]
    async fn test_builder_success() {
        let (tx, _rx) = mpsc::channel(10);
        let config = Arc::new(create_mock_config());
        let reader = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .task_id(42)
            .task_context(mock_task_context())
            .task_type("cdc_test")
            .build()
            .await
            .expect("Builder should create Processor successfully");
        assert_eq!(reader.task_id, 42);
        assert_eq!(reader.task_type, "cdc_test");
        assert!(reader.tx.is_some());
    }

    #[tokio::test]
    async fn test_builder_success_without_sender() {
        let config = Arc::new(create_mock_config());
        let reader = ProcessorBuilder::new()
            .config(config)
            .task_id(7)
            .task_context(mock_task_context())
            .task_type("no_sender")
            .build()
            .await
            .expect("Builder should create Processor without sender");
        assert_eq!(reader.task_id, 7);
        assert_eq!(reader.task_type, "no_sender");
        assert!(reader.tx.is_none());
    }

    #[tokio::test]
    async fn test_event_handler_struct_can_be_constructed() {
        let config = Arc::new(create_mock_config());
        let client = Arc::new(
            mongodb::Client::with_uri_str("mongodb://localhost:27017")
                .await
                .expect("Should create client with URI"),
        );
        let (_tx, _rx) = mpsc::channel(10);
        let handler = EventHandler {
            config,
            client,
            client_key: flowgen_core::client_registry::ClientKey::new("test", &"test"),
            retry_config: Default::default(),
            task_id: 1,
            tx: Some(_tx),
            task_type: "test_handler",
            task_context: mock_task_context(),
        };
        assert_eq!(handler.task_id, 1);
        assert_eq!(handler.task_type, "test_handler");
    }

    #[test]
    fn test_processor_debug() {
        let config = Arc::new(create_mock_config());
        let reader = Processor {
            config,
            tx: None,
            task_id: 1,
            task_context: mock_task_context(),
            task_type: "test",
        };
        let debug_str = format!("{:?}", reader);
        assert!(debug_str.contains("Processor"));
        assert!(debug_str.contains("task_id: 1"));
    }

    #[tokio::test]
    async fn test_init_auth_failure() {
        use flowgen_core::task::runner::Runner;

        let config = Arc::new(Cdc {
            name: "auth_fail_test".to_string(),
            db_name: "test_db".to_string(),
            credentials_path: Some(PathBuf::from("/invalid/credentials/path.json")),
            depends_on: Some(Vec::new()),
            retry: Default::default(),
            ..Default::default()
        });
        let reader = ProcessorBuilder::new()
            .config(config)
            .task_context(mock_task_context())
            .task_type("auth_test")
            .build()
            .await
            .unwrap();
        let result = reader.init().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Auth { .. }));
    }

    #[tokio::test]
    async fn test_handle_fails_without_connection() {
        let config = Arc::new(create_mock_config());
        let client = Arc::new(
            mongodb::Client::with_uri_str("mongodb://localhost:27017")
                .await
                .expect("Should create client with URI"),
        );
        let handler = EventHandler {
            config,
            client,
            client_key: flowgen_core::client_registry::ClientKey::new("test", &"test"),
            retry_config: Default::default(),
            task_id: 1,
            tx: None,
            task_type: "handle_test",
            task_context: mock_task_context(),
        };
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), handler.handle()).await;
        match result {
            Ok(Err(_)) => {}
            Err(_elapsed) => {}
            Ok(Ok(())) => panic!("Expected handle to fail without a MongoDB connection"),
        }
    }

    #[tokio::test]
    async fn test_run_stays_alive_until_cancelled() {
        use flowgen_core::task::runner::Runner;

        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        let task_context = Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .cancellation_token(cancellation_token.clone())
                .build()
                .unwrap(),
        );

        let config = Arc::new(Cdc {
            name: "run_test".to_string(),
            db_name: "test_db".to_string(),
            credentials_path: Some(PathBuf::from("/mock/path/creds.json")),
            depends_on: Some(Vec::new()),
            retry: Some(flowgen_core::retry::RetryConfig {
                max_attempts: Some(1),
                initial_backoff: std::time::Duration::from_millis(20),
            }),
            ..Default::default()
        });
        let reader = ProcessorBuilder::new()
            .config(config)
            .task_context(task_context)
            .task_type("run_test")
            .build()
            .await
            .unwrap();

        let handle = tokio::spawn(reader.run());

        // run() must still be alive after the init retries would have
        // exhausted — it reconnects forever until cancelled, unlike the
        // old detached-inner-spawn version which returned instantly.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!handle.is_finished());

        cancellation_token.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("run() must return promptly after cancellation")
            .unwrap();
        assert!(result.is_ok());
    }
}
