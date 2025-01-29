use flowgen_core::{
    event::{Event, EventBuilder},
    recordbatch::RecordBatchExt,
    render::Render,
};
use futures_util::future::TryJoinAll;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
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
    TokioJoinError(#[source] tokio::task::JoinError),
    #[error("There was an error with sending event over channel.")]
    TokioSendMessageError(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("There was an error constructing Flowgen Event.")]
    EventError(#[source] flowgen_core::event::Error),
    #[error("There was an error with parsing credentials file.")]
    ParseCredentialsError(#[source] serde_json::Error),
    #[error("There was an error with processing record batch.")]
    RecordBatchError(#[source] flowgen_core::recordbatch::RecordBatchError),
    #[error("There was an error with rendering a given value.")]
    RenderError(#[source] flowgen_core::render::Error),
    #[error("There was an error with processing request.")]
    ReqwestError(#[source] reqwest::Error),
}
pub struct Processor {
    handle_list: Vec<JoinHandle<Result<(), ProcessorError>>>,
}

impl Processor {
    pub async fn process(self) -> Result<(), ProcessorError> {
        tokio::spawn(async move {
            let _ = self
                .handle_list
                .into_iter()
                .collect::<TryJoinAll<_>>()
                .await
                .map_err(ProcessorError::TokioJoinError);
        });
        Ok(())
    }
}

/// A builder of the http processor.
pub struct Builder {
    config: super::config::Processor,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl Builder {
    /// Creates a new instance of a Builder.
    pub fn new(
        config: super::config::Processor,
        tx: &Sender<Event>,
        current_task_id: usize,
    ) -> Builder {
        Builder {
            config,
            tx: tx.clone(),
            rx: tx.subscribe(),
            current_task_id,
        }
    }

    pub async fn build(mut self) -> Result<Processor, ProcessorError> {
        let mut handle_list: Vec<JoinHandle<Result<(), ProcessorError>>> = Vec::new();

        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(ProcessorError::ReqwestError)?;

        let handle: JoinHandle<Result<(), ProcessorError>> = tokio::spawn(async move {
            while let Ok(event) = self.rx.recv().await {
                if event.current_task_id == Some(self.current_task_id - 1) {
                    let mut data = Map::new();
                    if let Some(inputs) = &self.config.inputs {
                        for (key, input) in inputs {
                            let value = input.extract(&event.data, &event.extensions);
                            if let Ok(value) = value {
                                data.insert(key.to_owned(), value);
                            }
                        }
                    }

                    let endpoint = self
                        .config
                        .endpoint
                        .render(&data)
                        .map_err(ProcessorError::RenderError)?;

                    let client = client.get(endpoint);
                    let mut resp = String::new();

                    if let Some(ref credentials) = self.config.credentials {
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
                                .map_err(ProcessorError::ReqwestError)?
                                .text()
                                .await
                                .map_err(ProcessorError::ReqwestError)?;
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

                    self.tx
                        .send(e)
                        .map_err(ProcessorError::TokioSendMessageError)?;
                }
            }
            Ok(())
        });

        handle_list.push(handle);

        Ok(Processor { handle_list })
    }
}
