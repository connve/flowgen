use crate::client::MongoClientBuilder;
use flowgen_core::event::{Event, EventData, SenderExt};
use mongodb::bson::{oid::ObjectId, Bson, Document};
use mongodb::Collection;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::{error, Instrument};

/// Errors that can occur during Salesforce Pub/Sub publishing operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: crate::client::Error,
    },
    #[error("Send event message error: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error("Service error: {source}")]
    Service {
        #[source]
        source: flowgen_core::service::Error,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Unsupported event data")]
    UnsupportedEventData,
    #[error("Invalid Mongo Document")]
    InvalidDocument,
    #[error("Error parsing Schema JSON string to Schema type")]
    SchemaParse(),
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Mongo Writer failed after all retry attempts: {source}")]
    MongoWriter {
        #[from]
        source: mongodb::error::Error,
    },
    #[error("Writer event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
}

/// Event handler for processing and publishing events to Salesforce Pub/Sub.
pub struct EventHandler {
    /// Writer configuration.
    config: Arc<super::config::Writer>,
    client: mongodb::Client,
    /// The Database Name from Mongo.
    pub db_name: String,
    /// The Collection Name from Mongo.
    pub collection_name: String,
    /// Current task identifier.
    task_id: usize,
    /// Channel sender for response events.
    tx: tokio::sync::broadcast::Sender<Event>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event by writing to a database in mongo collection.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let json = match &event.data {
            EventData::Json(value) => value,
            _ => return Err(Error::UnsupportedEventData),
        };

        // Now use json to build your bson Document
        let oid = json["_id"]
            .as_object()
            .and_then(|obj| obj.get("$oid"))
            .and_then(|v| v.as_str())
            .and_then(|s| ObjectId::parse_str(s).ok())
            .unwrap_or_else(ObjectId::new);

        // Convert entire JSON to BSON Document
        let mut doc = match json_to_bson(json) {
            Bson::Document(d) => d,
            _ => return Err(Error::InvalidDocument),
        };

        // Override _id with the native ObjectId
        doc.insert("_id", Bson::ObjectId(oid));     

        // Generate subject prefix from topic name.
        let subject = format!(
            "{}{}{}",
            self.config.db_name, ".", self.config.collection_name
        );

        let db_collection: Collection<Document> = self
            .client
            .database(&self.config.db_name)
            .collection(&self.config.collection_name);

        let resp = db_collection.insert_one(&doc).await?;

        let resp_json = serde_json::to_value(&resp).map_err(|e| Error::SerdeJson { source: e })?;

        let e = flowgen_core::event::EventBuilder::new()
            .data(EventData::Json(resp_json))
            .subject(subject)
            .id(resp.inserted_id.to_string())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()?;

        self.tx
            .send_with_logging(e)
            .map_err(|source| Error::SendMessage { source })?;

        Ok(())
    }
}

/// Salesforce Pub/Sub publisher that receives events and publishes them to configured topics.
#[derive(Debug)]
pub struct Writer {
    /// Writer configuration including topic settings and credentials.
    config: Arc<super::config::Writer>,
    /// Receiver for incoming events to publish.
    rx: Receiver<Event>,
    /// Channel sender for response events.
    tx: tokio::sync::broadcast::Sender<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self.config.as_ref();

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
            config: Arc::clone(&self.config),
            client,
            db_name: config.db_name.clone(),
            collection_name: config.collection_name.clone(),
            task_id: self.task_id,
            tx: self.tx.clone(),
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

#[derive(Default)]
pub struct WriterBuilder {
    config: Option<Arc<super::config::Writer>>,
    rx: Option<Receiver<Event>>,
    tx: Option<tokio::sync::broadcast::Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl WriterBuilder {
    pub fn new() -> WriterBuilder {
        WriterBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Writer>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: tokio::sync::broadcast::Sender<Event>) -> Self {
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

    pub async fn build(self) -> Result<Writer, Error> {
        Ok(Writer {
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

fn json_to_bson(value: &Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Bson::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                Bson::Null
            }
        }
        Value::String(s) => Bson::String(s.clone()),
        Value::Array(arr) => Bson::Array(arr.iter().map(json_to_bson).collect()),
        Value::Object(map) => {
            let doc: Document = map
                .iter()
                .map(|(k, v)| (k.clone(), json_to_bson(v)))
                .collect();
            Bson::Document(doc)
        }
    }
}
