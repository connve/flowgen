use chrono::Utc;
use flowgen_core::{
    connect::client::Client,
    event::{Event, EventBuilder, EventData},
};
use oauth2::TokenResponse;
use serde::{Deserialize, Serialize};
use serde_json::{json,Value};
use std::{path::Path, sync::Arc};
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "bulkapi";
const DEFAULT_URI_PATH: &str = "/services/data/v61.0/jobs/query";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error("missing required attribute: {}", 1)]
    MissingRequiredAttribute(String),
    #[error("no array data available")]
    EmptyArray(),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    ReqwestInvalidHeaderName(#[from] reqwest::header::InvalidHeaderName),
    #[error(transparent)]
    ReqwestInvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
    #[error(transparent)]
    SalesforceAuth(#[from] crate::client::Error),
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error("could not connect to Salesforce: {}", _0)]
    NoSalesforceAuthToken(String),
}
#[derive(Deserialize, Serialize, PartialEq, Clone, Debug, Default)]
struct Credentials {
    bearer_auth: Option<String>,
}

pub struct JobCreator {
    config: Arc<super::config::JobCreator>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

struct EventHandler {
    /// HTTP client.
    client: Arc<reqwest::Client>,
    /// Processor configuration settings.
    config: Arc<super::config::JobCreator>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Task identifier for event tracking
    current_task_id: usize,
}

impl EventHandler {
    /// Processes an event and writes it to the configured object store.
    async fn handle(self, event: Event) -> Result<(), Error> {
        let config = self.config.as_ref();
        let creds_path = Path::new(&config.credentials);

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(creds_path.to_path_buf())
            .build()
            .map_err(Error::SalesforceAuth)?
            .connect()
            .await?;

        let token_result = sfdc_client.token_result.ok_or_else(|| {
            Error::NoSalesforceAuthToken("Error getting access token".to_string())
        })?;

        let payload: Value;

        match self.config.operation {
            super::config::Operation::Query | super::config::Operation::QueryAll => {
                // Create payload for query or queryall job.
                payload = json!({
                    "operation": self.config.operation,
                    "query": Some(self.config.query.clone()),
                    "contentType": Some(self.config.content_type.clone()),
                    "columnDelimiter": Some(self.config.column_delimiter.clone()),
                    "lineEnding": Some(self.config.line_ending.clone()),
                });
            }
            super::config::Operation::Insert => {
                todo!()
            }
            super::config::Operation::Update => {
                todo!()
            }
            super::config::Operation::Upsert => {
                todo!()
            }
            super::config::Operation::Delete => {
                todo!()
            }
            super::config::Operation::HardDelete => {
                todo!()
            }
            _ => {
                payload = json!({});
            }
        }

        // Create http client with sf endpoint.
        let mut client = self
            .client
            .post(sfdc_client.instance_url + DEFAULT_URI_PATH);

        client = client.bearer_auth(token_result.access_token().secret());
        client = client.json(&payload);

        // Do API Call.
        let resp = client.send().await?.text().await?;

        // Prepare processor output.
        let data = json!(resp);

        let timestamp = Utc::now().timestamp_micros();
        let subject = match &self.config.label {
            Some(label) => format!(
                "{}.{}.{}",
                DEFAULT_MESSAGE_SUBJECT,
                label.to_lowercase(),
                timestamp
            ),
            None => format!("{DEFAULT_MESSAGE_SUBJECT}.{timestamp}"),
        };

        // Send processor output as event.
        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(subject.clone())
            .current_task_id(self.current_task_id)
            .build()?;
        self.tx.send(e)?;
        event!(Level::INFO, "event processed: {}", subject);

        Ok(())
    }
}

impl flowgen_core::task::runner::Runner for JobCreator {
    type Error = Error;
    async fn run(mut self) -> Result<(), Error> {
        let client = reqwest::ClientBuilder::new().https_only(true).build()?;
        let client = Arc::new(client);

        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let config = Arc::clone(&self.config);
                let client = Arc::clone(&client);
                let tx = self.tx.clone();
                let current_task_id = self.current_task_id;

                let event_handler = EventHandler {
                    config,
                    current_task_id,
                    tx,
                    client,
                };
                tokio::spawn(async move {
                    if let Err(err) = event_handler.handle(event).await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::JobCreator>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::JobCreator>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<JobCreator, Error> {
        Ok(JobCreator {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
