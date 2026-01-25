use super::message::MongoEventsExt;
use crate::client::MongoClientBuilder;
use flowgen_core::event::{Event, SenderExt};
use futures::StreamExt;
use mongodb::bson::doc;
use mongodb::bson::Document;
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
    /// Missing full document in change stream event
    #[error("Full document is not available for this operation")]
    NoFullDocument(),
}

/// Handles processing of individual events by writing them to object storage.
pub struct EventHandler {
    tx: Sender<Event>,
    config: Arc<super::config::ChangeStream>,
    client: mongodb::Client,
    task_id: usize,
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
    async fn handle(&self) -> Result<(), Error> {
        let db = self.client.database(&self.config.db_name);

        let mut change_stream = db
            .watch()
            .await
            .map_err(|source| Error::MongoReader { source })?;

        while let Some(event) = change_stream.next().await {
            let result = event.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>);

            match result {
                Ok(event) => {
                    // Get the full document or return error if it doesn't exist.
                    let doc = event
                        .full_document
                        .as_ref()
                        .ok_or(Error::NoFullDocument())?;
                    // Now pass to process_message
                    self.process_message(Ok(doc.clone())).await?;
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    self.process_message(Err(e)).await?;
                }
            }
        }
        Ok(())
    }
}

/// Object store reader that processes events from a broadcast receiver.
#[derive(Debug)]
pub struct Reader {
    /// Reader configuration settings.
    config: Arc<super::config::ChangeStream>,
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
    async fn init(&self) -> Result<EventHandler, Error> {
        let client = MongoClientBuilder::new()
            .credentials_path(self.config.credentials_path.clone())
            .map_err(|e| Error::Auth { source: e })?
            .default_host("cluster0.mongodb.net".to_string())
            .build()
            .map_err(|e| Error::Auth { source: e })?
            .connect()
            .await
            .map_err(|e| Error::Auth { source: e })?;

        let event_handler = EventHandler {
            client,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        // Spawn event handler task.
        tokio::spawn(
            async move {
                // Retry loop with exponential backoff.
                let result = tokio_retry::Retry::spawn(retry_config.strategy(), || async {
                    // Initialize task.
                    let event_handler = match self.init().await {
                        Ok(handler) => handler,
                        Err(e) => {
                            error!("{}", e);
                            return Err(e);
                        }
                    };

                    // Run event handler.
                    match event_handler.handle().await {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            error!("{}", e);
                            Err(e)
                        }
                    }
                })
                .await;

                if let Err(e) = result {
                    error!(
                        "{}",
                        Error::RetryExhausted {
                            source: Box::new(e)
                        }
                    );
                }
            }
            .instrument(tracing::Span::current()),
        );

        Ok(())
    }
}

/// Builder pattern for constructing Writer instances.
#[derive(Default)]
pub struct ReaderBuilder {
    /// Writer configuration settings.
    config: Option<Arc<super::config::ChangeStream>>,
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
    pub fn config(mut self, config: Arc<super::config::ChangeStream>) -> Self {
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
