use crate::event::{Event, EventBuilder, EventData};
use chrono::Utc;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast::Sender, time};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &'static str = "generate";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error(transparent)]
    Event(#[from] crate::event::Error),
    #[error(transparent)]
    RecordBatch(#[from] crate::convert::recordbatch::Error),
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}
pub struct Subscriber {
    config: Arc<super::config::Subscriber>,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl crate::task::runner::Runner for Subscriber {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let mut counter = 0;
        loop {
            time::sleep(Duration::from_secs(self.config.interval)).await;
            counter += 1;

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

            let data = match &self.config.message {
                Some(message) => json!(message),
                None => json!(null),
            };

            let e = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(subject.clone())
                .current_task_id(self.current_task_id)
                .build()
                .map_err(Error::Event)?;

            self.tx.send(e).map_err(Error::SendMessage)?;
            event!(Level::INFO, "Event processed: {}", subject);

            match self.config.count {
                Some(count) if count == counter => break,
                Some(_) | None => continue,
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct SubscriberBuilder {
    config: Option<Arc<super::config::Subscriber>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Subscriber>) -> Self {
        self.config = Some(config);
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

    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
