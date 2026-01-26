use super::message::MongoEventsExt;
use crate::client::MongoClientBuilder;
use flowgen_core::event::{Event, SenderExt};
use futures::TryStreamExt;
use mongodb::bson::Document;
use mongodb::{bson::doc, Collection};
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{error, Instrument};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: crate::client::Error,
    },
    #[error("Sending event to channel failed with error: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error("Reader event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Configuration template rendering failed with error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Mongo Reader failed after all retry attempts: {source}")]
    MongoReader {
        #[source]
        source: mongodb::error::Error,
    },
    #[error("Message conversion failed with error: {source}")]
    MessageConversion {
        #[source]
        source: crate::message::Error,
    },
    #[error("Other subscriber error")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("MongoDB error: {0}")]
    MongoDB(#[from] mongodb::error::Error),
}

/// Handles processing of individual events by writing them to object storage.
pub struct EventHandler {
    client: mongodb::Client,
    config: Arc<super::config::Reader>,
    task_id: usize,
    tx: Sender<Event>,
    task_type: &'static str,
}

impl EventHandler {
    /// Processes a single message result.
    async fn process_message(
        &self,
        message_result: Result<Document, Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), Error> {
        match message_result {
            Ok(message) => {
                let e = message
                    .to_event(self.task_type, self.task_id)
                    .map_err(|source| Error::MessageConversion { source })?;

                self.tx
                    .send_with_logging(e)
                    .map_err(|source| Error::SendMessage { source })?;
                Ok(())
            }
            Err(err) => Err(Error::Other(err)),
        }
    }
    /// Processes an event and writes it to the configured object store.
    async fn handle(&self, _event: Event) -> Result<(), Error> {
        let document_collection: Collection<Document> = self
            .client
            .database(&self.config.db_name)
            .collection(&self.config.collection_name);

        let mut doc = Document::new();
        for (key, value) in &self.config.filter {
            doc.insert(key, value);
        }

        let mut cursor = document_collection.find(doc).await?;
        while let Some(doc) = cursor.try_next().await? {
            self.process_message(Ok(doc)).await?;
        }

        Ok(())
    }
}

/// Object store reader that processes events from a broadcast receiver.
#[derive(Debug)]
pub struct Reader {
    /// Reader configuration settings.
    config: Arc<super::config::Reader>,
    /// Broadcast receiver for incoming events.
    rx: Receiver<Event>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Reader {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the reader by establishing mongo client connection.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Building and connecting the mongo client with credentials
    async fn init(&self) -> Result<EventHandler, Error> {
        let client = MongoClientBuilder::new()
            .credentials_path(self.config.credentials_path.clone())
            .map_err(|e| Error::Auth { source: e })?
            .default_host("cluster0.mongodb.net".to_string()) // Optional: set your MongoDB host
            .build()
            .map_err(|e| Error::Auth { source: e })?
            .connect()
            .await
            .map_err(|e| Error::Auth { source: e })?;

        let event_handler = EventHandler {
            client,
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(
                    "{}",
                    Error::RetryExhausted {
                        source: Box::new(e)
                    }
                );
                return Ok(());
            }
        };

        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    if Some(event.task_id) == event_handler.task_id.checked_sub(1) {
                        let event_handler = Arc::clone(&event_handler);
                        let retry_strategy = retry_config.strategy();
                        tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!("{}", e);
                                            Err(e)
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(
                                        "{}",
                                        Error::RetryExhausted {
                                            source: Box::new(err)
                                        }
                                    );
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );
                    }
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

/// Builder pattern for constructing Writer instances.
#[derive(Default)]
pub struct ReaderBuilder {
    /// Writer configuration settings.
    config: Option<Arc<super::config::Reader>>,
    /// Broadcast receiver for incoming events.
    rx: Option<Receiver<Event>>,
    /// Event channel sender
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl ReaderBuilder {
    pub fn new() -> ReaderBuilder {
        ReaderBuilder {
            ..Default::default()
        }
    }

    /// Sets the writer configuration.
    pub fn config(mut self, config: Arc<super::config::Reader>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the current task identifier.
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

    /// Builds the Writer instance, validating required fields.
    pub async fn build(self) -> Result<Reader, Error> {
        Ok(Reader {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            task_id: self.task_id,
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}
