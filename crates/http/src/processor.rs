use chrono::Utc;
use flowgen_core::{
    convert::{recordbatch::RecordBatchExt, render::Render},
    stream::event::{Event, EventBuilder},
};
use futures_util::future::try_join_all;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::{
    fs,
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "http.response";

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
    #[error("error with reading file")]
    IO(#[source] std::io::Error),
    #[error("error with executing async task")]
    TaskJoin(#[source] tokio::task::JoinError),
    #[error("error with sending event over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("error with creating event")]
    Event(#[source] flowgen_core::stream::event::Error),
    #[error("error with parsing credentials file")]
    ParseCredentials(#[source] serde_json::Error),
    #[error("error with processing recordbatch")]
    RecordBatch(#[source] flowgen_core::convert::recordbatch::Error),
    #[error("error with rendering content")]
    Render(#[source] flowgen_core::convert::render::Error),
    #[error("error with processing http request")]
    Reqwest(#[source] reqwest::Error),
    #[error("error with converting a key to header name")]
    ReqwestInvalidHeaderName(#[source] reqwest::header::InvalidHeaderName),
    #[error("error with converting a value to a header value")]
    ReqwestInvalidHeaderValue(#[source] reqwest::header::InvalidHeaderValue),
    #[error("error with parsing a given value")]
    SerdeJson(#[source] serde_json::Error),
    #[error("missing required attrubute")]
    MissingRequiredAttribute(String),
    #[error("provided attribute not found")]
    NotFound(),
    #[error("error parsing string to json")]
    ParseJson(),
    #[error("either payload json or payload input is required")]
    PayloadConfig(),
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
        let mut handle_list = Vec::new();

        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(Error::Reqwest)?;

        let client = Arc::new(client);

        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let config = Arc::clone(&self.config);
                let client = Arc::clone(&client);
                let tx = self.tx.clone();

                let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    // Get input dynamic values.
                    let mut data = Map::new();
                    if let Some(inputs) = &config.inputs {
                        for (key, input) in inputs {
                            let value = input.extract(&event.data, &event.extensions);
                            if let Ok(value) = value {
                                data.insert(key.to_owned(), value);
                            }
                        }
                    }

                    // Setup http client with endpoint according to chosen method.
                    let endpoint = config.endpoint.render(&data).map_err(Error::Render)?;
                    let mut client = match config.method {
                        crate::config::HttpMethod::GET => client.get(endpoint),
                        crate::config::HttpMethod::POST => client.post(endpoint),
                        crate::config::HttpMethod::PUT => client.put(endpoint),
                        crate::config::HttpMethod::DELETE => client.delete(endpoint),
                        crate::config::HttpMethod::PATCH => client.patch(endpoint),
                        crate::config::HttpMethod::HEAD => client.head(endpoint),
                    };

                    // Add headers if present in the config.
                    if let Some(headers) = config.headers.to_owned() {
                        let mut header_map = HeaderMap::new();
                        for (key, value) in headers {
                            let header_name = HeaderName::try_from(key)
                                .map_err(Error::ReqwestInvalidHeaderName)?;
                            let header_value = HeaderValue::try_from(value)
                                .map_err(Error::ReqwestInvalidHeaderValue)?;
                            header_map.insert(header_name, header_value);
                        }
                        client = client.headers(header_map);
                    }

                    // Set client body to json from the provided json string.
                    if let Some(payload) = &config.payload {
                        let json = match &payload.object {
                            Some(obj) => Value::Object(obj.to_owned()),
                            None => match &payload.input {
                                Some(input) => {
                                    let key = input.replace("{{", "").replace("}}", "");

                                    let json_string = data.get(&key).ok_or_else(Error::NotFound)?;
                                    serde_json::from_str::<serde_json::Value>(
                                        json_string.as_str().ok_or_else(Error::ParseJson)?,
                                    )
                                    .map_err(Error::SerdeJson)?
                                }
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
                    if let Some(credentials) = &config.credentials {
                        let credentials_string =
                            fs::read_to_string(credentials).await.map_err(Error::IO)?;

                        let credentials: Credentials = serde_json::from_str(&credentials_string)
                            .map_err(Error::ParseCredentials)?;

                        if let Some(bearer_token) = credentials.bearer_auth {
                            client = client.bearer_auth(bearer_token);
                        }

                        if let Some(basic_auth) = credentials.basic_auth {
                            client =
                                client.basic_auth(basic_auth.username, Some(basic_auth.password));
                        }
                    };

                    // Do API Call.
                    let resp = client
                        .send()
                        .await
                        .map_err(Error::Reqwest)?
                        .text()
                        .await
                        .map_err(Error::Reqwest)?;

                    // Prepare processor output.
                    let recordbatch = resp.to_recordbatch().map_err(Error::RecordBatch)?;
                    let extensions = Value::Object(data)
                        .to_string()
                        .to_recordbatch()
                        .map_err(Error::RecordBatch)?;

                    let timestamp = Utc::now().timestamp_micros();
                    let subject = match &config.label {
                        Some(label) => format!(
                            "{}.{}.{}",
                            DEFAULT_MESSAGE_SUBJECT,
                            label.to_lowercase(),
                            timestamp
                        ),
                        None => format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, timestamp),
                    };
                    let event_message = format!("event processed: {}", subject);

                    // Send processor output as event.
                    let e = EventBuilder::new()
                        .data(recordbatch)
                        .extensions(extensions)
                        .subject(subject)
                        .current_task_id(self.current_task_id)
                        .build()
                        .map_err(Error::Event)?;

                    tx.send(e).map_err(Error::SendMessage)?;
                    event!(Level::INFO, "{}", event_message);
                    Ok(())
                });
                handle_list.push(handle);
            }
        }
        let _ = try_join_all(handle_list.iter_mut()).await;
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
