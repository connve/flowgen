


use flowgen_core::event::{Event};
use std::sync::Arc;
use tokio::sync::{
    broadcast::{Receiver, Sender},
};
use tracing::{error, Instrument};
use mongodb::options::ResolverConfig;
use mongodb::options::ClientOptions;
use std::env;
use serde::Deserialize;
use serde::Serialize;
use mongodb::{ 
    Client,
    Collection,
    bson::{doc, oid::ObjectId}
};
use futures::TryStreamExt;
use futures::StreamExt;



#[derive(Deserialize,Serialize,Debug)]
struct User {
    #[serde(rename = "_id")]
    pub object_id: ObjectId,
        pub id: String, 
    
    pub name: String,
    pub email: String,
    
    #[serde(rename = "createdTS")]
    pub created_ts: String,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
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
    #[error("IO operation failed with error: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    #[error("Arrow operation failed with error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Avro operation failed with error: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    #[error("JSON serialization/deserialization failed with error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Object store operation failed with error: {source}")]
    ObjectStore {
        #[source]
        source: object_store::Error,
    },
    #[error("Configuration template rendering failed with error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Could not initialize object store context")]
    NoObjectStoreContext,
    #[error("Could not retrieve file extension")]
    NoFileExtension,
    #[error("Cache error")]
    Cache,
    #[error("Host coordination failed with error: {source}")]
    Host {
        #[source]
        source: flowgen_core::host::Error,
    },
    #[error("Invalid URL format with error: {source}")]
    ParseUrl {
        #[source]
        source: url::ParseError,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
     #[error("Mongo Reader failed after all retry attempts: {source}")]
     MongoReader{
         #[source]
         source: mongodb::error::Error,
     },
}

/// Handles processing of individual events by writing them to object storage.
pub struct EventHandler {
    /// Writer configuration settings.
    config: Arc<super::config::Reader>,
    client: mongodb::Client,
}

impl EventHandler {
    /// Processes an event and writes it to the configured object store.
    async fn handle(&self) -> Result<(), Error> {

        let users: Collection<User> = self.client.database("rust-example").collection("users");
      
        // let mut cursor = users.find(
        //     doc! { "name": "Sayan 2" },
        //     None,

        // ).await.unwrap();
        
        // while let Some(doc) = cursor.try_next().await.unwrap() {
        //     println!("{:?}", doc);
        // }

        let pipeline = vec![];
        let mut change_stream = users.watch(pipeline, None).await.map_err(|source| Error::MongoReader { source })?;

        while let Some(event) = change_stream.next().await.transpose().map_err(|source| Error::MongoReader { source })? {
            println!("operation performed: {:?}, document: {:?}", event.operation_type, event.full_document);
            // operation performed: Insert, document: Some(Document({"x": Int32(1)}))
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

    /// Initializes the reader by establishing object store client connection.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Building and connecting the object store client with credentials
    async fn init(&self) -> Result<EventHandler, Error> {

         // Load the MongoDB connection string from an environment variable:
        let client_uri = env::var("MONGODB_URI").expect("You must set the MONGODB_URI environment variable!");
        // A Client is needed to connect to MongoDB:
        // An extra line of code to work around a DNS issue on Windows:
        let options =
        ClientOptions::parse_with_resolver_config(&client_uri, ResolverConfig::cloudflare())
            .await.unwrap();
        let client = Client::with_options(options).map_err(|source| Error::MongoReader { source })?;
        
        let event_handler = EventHandler {
            client,
            config: Arc::clone(&self.config),
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
