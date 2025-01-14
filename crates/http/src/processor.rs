use std::collections::HashMap;

use arrow::array::StringArray;
use flowgen_core::event::{Event, EventBuilder, RecordBatchExt};
use futures_util::future::TryJoinAll;
use handlebars::Handlebars;
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};

#[derive(Serialize, Deserialize)]
struct Credentials {
    bearer_auth: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error reading/writing/seeking file.")]
    InputOutput(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error with sending event over channel.")]
    TokioSendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("There was an error constructing Flowgen Event.")]
    FlowgenEvent(#[source] flowgen_core::event::Error),
    #[error("Cannot parse the credentials file")]
    ParseCredentials(#[source] serde_json::Error),
}
pub struct Processor {
    handle_list: Vec<JoinHandle<Result<(), Error>>>,
}

impl Processor {
    pub async fn process(self) -> Result<(), Error> {
        tokio::spawn(async move {
            let _ = self
                .handle_list
                .into_iter()
                .collect::<TryJoinAll<_>>()
                .await
                .map_err(Error::TokioJoin);
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

    pub async fn build(mut self) -> Result<Processor, Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let handlebars = Handlebars::new();

        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .unwrap();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            while let Ok(e) = self.rx.recv().await {
                if e.current_task_id == Some(self.current_task_id - 1) {
                    let mut data = HashMap::new();
                    if let Some(inputs) = &self.config.inputs {
                        for (key, input) in inputs {
                            if !input.is_static && !input.is_extension {
                                let array: StringArray = e
                                    .data
                                    .column_by_name(&input.value)
                                    .unwrap()
                                    .to_data()
                                    .into();

                                for item in (&array).into_iter().flatten() {
                                    data.insert(key.to_string(), item.to_string());
                                }
                            }
                        }
                    }

                    let endpoint = handlebars
                        .render_template(&self.config.endpoint, &data)
                        .unwrap();

                    let client = client.get(endpoint);
                    let mut text_resp = String::new();

                    if let Some(ref credentials) = self.config.credentials {
                        let credentials_string = fs::read_to_string(credentials).await.unwrap();
                        let credentials: Credentials = serde_json::from_str(&credentials_string)
                            .map_err(Error::ParseCredentials)?;

                        if let Some(bearer_token) = credentials.bearer_auth {
                            text_resp = client
                                .bearer_auth(bearer_token)
                                .send()
                                .await
                                .unwrap()
                                .text()
                                .await
                                .unwrap();
                        }
                    };

                    let resp: serde_json::Value = serde_json::from_str(&text_resp).unwrap();

                    let record_batch: arrow::array::RecordBatch = resp.to_recordbatch().unwrap();
                    let subject = "http.respone.out".to_string();

                    let e = EventBuilder::new()
                        .data(record_batch)
                        .subject(subject)
                        .current_task_id(self.current_task_id)
                        .extensions(data)
                        .build()
                        .map_err(Error::FlowgenEvent)?;

                    self.tx.send(e).map_err(Error::TokioSendMessage)?;
                }
            }
            Ok(())
        });

        handle_list.push(handle);

        Ok(Processor { handle_list })
    }
}
