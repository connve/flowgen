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

#[derive(Deserialize, Serialize)]
struct Credentials {
    bearer_auth: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessorError {
    #[error("There was an error reading/writing/seeking file.")]
    IOError(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    JoinError(#[source] tokio::task::JoinError),
    #[error("There was an error with sending event over channel.")]
    SendMessageError(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("There was an error constructing Flowgen Event.")]
    EventError(#[source] flowgen_core::event::EventError),
    #[error("There was an error with parsing credentials file.")]
    ParseCredentialsError(#[source] serde_json::Error),
    #[error("There was an error with processing record batch.")]
    RecordBatchError(#[source] flowgen_core::recordbatch::RecordBatchError),
    #[error("There was an error with rendering a given value.")]
    RenderError(#[source] flowgen_core::render::RenderError),
    #[error("There was an error with processing a request.")]
    RequestError(#[source] reqwest::Error),
    #[error("Missing required event attrubute.")]
    MissingRequiredAttributeError(String),
}
pub struct Processor {
    config: Arc<super::config::Processor>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl Processor {
    pub async fn process(mut self) -> Result<(), ProcessorError> {
        let mut handle_list = Vec::new();

        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(ProcessorError::RequestError)?;

        let client = Arc::new(client);

        while let Ok(event) = self.rx.recv().await {
            let config = Arc::clone(&self.config);
            let client = Arc::clone(&client);
            let tx = self.tx.clone();

            let handle: JoinHandle<Result<(), ProcessorError>> = tokio::spawn(async move {
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

                    let endpoint = config
                        .endpoint
                        .render(&data)
                        .map_err(ProcessorError::RenderError)?;

                    let client = client.get(endpoint);
                    let mut resp = String::new();

                    if let Some(ref credentials) = config.credentials {
                        let credentials_string = fs::read_to_string(credentials)
                            .await
                            .map_err(ProcessorError::IOError)?;

                        let credentials: Credentials = serde_json::from_str(&credentials_string)
                            .map_err(ProcessorError::ParseCredentialsError)?;

                        if let Some(bearer_token) = credentials.bearer_auth {
                            resp = client
                                .bearer_auth(bearer_token)
                                .send()
                                .await
                                .map_err(ProcessorError::RequestError)?
                                .text()
                                .await
                                .map_err(ProcessorError::RequestError)?;
                        }
                    };

                    let recordbatch = resp
                        .to_recordbatch()
                        .map_err(ProcessorError::RecordBatchError)?;

                    let extensions = Value::Object(data)
                        .to_string()
                        .to_recordbatch()
                        .map_err(ProcessorError::RecordBatchError)?;

                    let subject = "http.response.out".to_string();

                    let e = EventBuilder::new()
                        .data(recordbatch)
                        .extensions(extensions)
                        .subject(subject)
                        .current_task_id(self.current_task_id)
                        .build()
                        .map_err(ProcessorError::EventError)?;

                    tx.send(e).map_err(ProcessorError::SendMessageError)?;
                }
                Ok(())
            });
            handle_list.push(handle);
        }

        let _ = try_join_all(handle_list.iter_mut()).await;

        Ok(())
    }
}

/// A builder of the http processor.
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

    pub async fn build(self) -> Result<Processor, ProcessorError> {
        Ok(Processor {
            config: self.config.ok_or_else(|| {
                ProcessorError::MissingRequiredAttributeError("config".to_string())
            })?,
            rx: self.rx.ok_or_else(|| {
                ProcessorError::MissingRequiredAttributeError("receiver".to_string())
            })?,
            tx: self.tx.ok_or_else(|| {
                ProcessorError::MissingRequiredAttributeError("sender".to_string())
            })?,
            current_task_id: self.current_task_id,
        })
    }
}
