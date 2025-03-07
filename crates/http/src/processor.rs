use chrono::Utc;
use flowgen_core::{
    event::{Event, EventBuilder},
    recordbatch::RecordBatchExt,
    render::Render,
};
use futures_util::future::try_join_all;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::{
    fs,
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "http.response.out";

#[derive(Deserialize, Serialize)]
struct Credentials {
    bearer_auth: Option<String>,
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
    Event(#[source] flowgen_core::event::Error),
    #[error("error with parsing credentials file")]
    ParseCredentials(#[source] serde_json::Error),
    #[error("error with processing recordbatch")]
    RecordBatch(#[source] flowgen_core::recordbatch::Error),
    #[error("error with rendering content")]
    Render(#[source] flowgen_core::render::Error),
    #[error("error with processing http request")]
    Request(#[source] reqwest::Error),
    #[error("missing required attrubute")]
    MissingRequiredAttribute(String),
}
pub struct Processor {
    config: Arc<super::config::Processor>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl Processor {
    pub async fn process(mut self) -> Result<(), Error> {
        let mut handle_list = Vec::new();

        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(Error::Request)?;

        let client = Arc::new(client);

        while let Ok(event) = self.rx.recv().await {
            let config = Arc::clone(&self.config);
            let client = Arc::clone(&client);
            let tx = self.tx.clone();

            let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                if event.current_task_id == Some(self.current_task_id - 1) {
                    let mut data = Map::new();
                    if let Some(inputs) = &config.inputs {
                        for (key, input) in inputs {
                            let value = input.extract(&event.data, &event.extensions);
                            if let Ok(value) = value {
                                data.insert(key.to_owned(), value);
                            }
                        }
                    }

                    let endpoint = config.endpoint.render(&data).map_err(Error::Render)?;

                    let client = client.get(endpoint);
                    let mut resp = String::new();

                    if let Some(ref credentials) = config.credentials {
                        let credentials_string =
                            fs::read_to_string(credentials).await.map_err(Error::IO)?;

                        let credentials: Credentials = serde_json::from_str(&credentials_string)
                            .map_err(Error::ParseCredentials)?;

                        if let Some(bearer_token) = credentials.bearer_auth {
                            resp = client
                                .bearer_auth(bearer_token)
                                .send()
                                .await
                                .map_err(Error::Request)?
                                .text()
                                .await
                                .map_err(Error::Request)?;
                        }
                    };

                    let recordbatch = resp.to_recordbatch().map_err(Error::RecordBatch)?;

                    let extensions = Value::Object(data)
                        .to_string()
                        .to_recordbatch()
                        .map_err(Error::RecordBatch)?;

                    let timestamp = Utc::now().timestamp_micros();
                    let subject = format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, timestamp);

                    let e = EventBuilder::new()
                        .data(recordbatch)
                        .extensions(extensions)
                        .subject(subject)
                        .current_task_id(self.current_task_id)
                        .build()
                        .map_err(Error::Event)?;

                    event!(Level::INFO, "event processed: {}", e.subject);
                    tx.send(e).map_err(Error::SendMessage)?;
                }
                Ok(())
            });
            handle_list.push(handle);
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
