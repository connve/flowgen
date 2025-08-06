use chrono::Utc;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData},
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::{
    fs,
    sync::broadcast::{Receiver, Sender},
};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &'static str = "http.response";

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug, Default)]
struct Credentials {
    bearer_auth: Option<String>,
    basic_auth: Option<BasicAuth>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug, Default)]
struct BasicAuth {
    username: String,
    password: String,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    ReqwestInvalidHeaderName(#[from] reqwest::header::InvalidHeaderName),
    #[error(transparent)]
    ReqwestInvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("provided attribute not found")]
    NotFound(),
    #[error("either payload json or payload input is required")]
    PayloadConfig(),
}

/// Handles processing of https call outs..
struct EventHandler {
    /// HTTP client.
    client: Arc<reqwest::Client>,
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Task identifier for event tracking
    current_task_id: usize,
}

impl EventHandler {
    /// Processes an event and writes it to the configured object store.
    async fn handle(self, event: Event) -> Result<(), Error> {
        let config = self.config.render(&event.data)?;

        // Setup http client with endpoint according to chosen method.
        let mut client = match config.method {
            crate::config::HttpMethod::GET => self.client.get(config.endpoint),
            crate::config::HttpMethod::POST => self.client.post(config.endpoint),
            crate::config::HttpMethod::PUT => self.client.put(config.endpoint),
            crate::config::HttpMethod::DELETE => self.client.delete(config.endpoint),
            crate::config::HttpMethod::PATCH => self.client.patch(config.endpoint),
            crate::config::HttpMethod::HEAD => self.client.head(config.endpoint),
        };

        // Add headers if present in the config.
        if let Some(headers) = self.config.headers.to_owned() {
            let mut header_map = HeaderMap::new();
            for (key, value) in headers {
                let header_name = HeaderName::try_from(key)?;
                let header_value = HeaderValue::try_from(value)?;
                header_map.insert(header_name, header_value);
            }
            client = client.headers(header_map);
        }

        // Set client body to json from the provided json string.
        if let Some(payload) = &self.config.payload {
            let json = match &payload.object {
                Some(obj) => Value::Object(obj.to_owned()),
                None => match &payload.input {
                    Some(input) => serde_json::from_str::<serde_json::Value>(input.as_str())?,
                    None => return Err(Error::PayloadConfig()),
                },
            };

            client = match payload.send_as {
                crate::config::PayloadSendAs::Json => client.json(&json),
                crate::config::PayloadSendAs::UrlEncoded => client.form(&json),
                crate::config::PayloadSendAs::QueryParams => client.query(&json),
            }
        }

        // Set client auth method & credentials.
        if let Some(credentials) = &self.config.credentials {
            let credentials_string = fs::read_to_string(credentials).await?;
            let credentials: Credentials = serde_json::from_str(&credentials_string)?;

            if let Some(bearer_token) = credentials.bearer_auth {
                client = client.bearer_auth(bearer_token);
            }

            if let Some(basic_auth) = credentials.basic_auth {
                client = client.basic_auth(basic_auth.username, Some(basic_auth.password));
            }
        };

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
        event!(Level::INFO, "event processeds: {}", subject);
        Ok(())
    }
}

pub struct Processor {
    config: Arc<super::config::Processor>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Processor {
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
    config: Option<Arc<super::config::Processor>>,
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

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
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

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
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
