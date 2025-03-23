use crate::{
    conversion::recordbatch::RecordBatchExt,
    event::{Event, EventBuilder},
};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "enumerate";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error with reading file")]
    IO(#[source] std::io::Error),
    #[error("error with executing async task")]
    TaskJoin(#[source] tokio::task::JoinError),
    #[error("error with sending event over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("error with creating event")]
    Event(#[source] crate::event::Error),
    #[error("error with parsing credentials file")]
    ParseCredentials(#[source] serde_json::Error),
    #[error("error with processing recordbatch")]
    RecordBatch(#[source] crate::conversion::recordbatch::Error),
    #[error("error with rendering content")]
    Render(#[source] crate::conversion::render::Error),
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
        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let data = self.config.array.extract(&event.data, &event.extensions);
                if let Ok(data) = data {
                    // let obj = data.get(0).unwrap().as_object().unwrap();
                    // for (_, v) in obj {
                    let arr = data.as_array().unwrap().iter().enumerate();
                    for (i, el) in arr {
                        let recordbatch = el.to_string().to_recordbatch().unwrap();

                        let timestamp = Utc::now().timestamp_micros();
                        let subject = match &self.config.label {
                            Some(label) => format!(
                                "{}.{}.{}.{}",
                                DEFAULT_MESSAGE_SUBJECT,
                                label.to_lowercase(),
                                i,
                                timestamp
                            ),
                            None => format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, i, timestamp),
                        };

                        let e = EventBuilder::new()
                            .data(recordbatch)
                            .subject(subject)
                            .current_task_id(self.current_task_id)
                            .build()
                            .map_err(Error::Event)?;

                        event!(Level::INFO, "event processed: {}", e.subject);
                        self.tx.send(e).map_err(Error::SendMessage)?;
                    }
                }
                // }
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
