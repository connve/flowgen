use crate::{
    conversion::recordbatch::RecordBatchExt,
    event::{Event, EventBuilder},
};
use chrono::Utc;
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast::Sender, time};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "generate.out";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error with sending event over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("error with creating event")]
    Event(#[source] crate::event::Error),
    #[error("error with processing recordbatch")]
    RecordBatch(#[source] crate::conversion::recordbatch::Error),
    #[error("missing required attrubute")]
    MissingRequiredAttribute(String),
}
pub struct Subscriber {
    config: Arc<super::config::Source>,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl Subscriber {
    pub async fn subscribe(self) -> Result<(), Error> {
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
                None => format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, timestamp),
            };

            let recordbatch = self.config.message.to_recordbatch().unwrap();

            let e = EventBuilder::new()
                .data(recordbatch)
                .subject(subject)
                .current_task_id(self.current_task_id)
                .build()
                .map_err(Error::Event)?;

            event!(Level::INFO, "event received: {}", e.subject);
            self.tx.send(e).map_err(Error::SendMessage)?;

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
    config: Option<Arc<super::config::Source>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Source>) -> Self {
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
