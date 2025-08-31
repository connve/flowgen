//! Event generation subscriber for producing scheduled synthetic events.
//!
//! Implements a timer-based event generator that creates events at regular intervals
//! with optional message content and count limits for testing and simulation workflows.

use crate::event::{generate_subject, Event, EventBuilder, EventData, SubjectSuffix};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast::Sender, time};

/// Default subject prefix for generated events.
const DEFAULT_MESSAGE_SUBJECT: &str = "generate";

/// Errors that can occur during generate task execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to send event through broadcast channel.
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    /// Event construction failed.
    #[error(transparent)]
    Event(#[from] crate::event::Error),
    /// Required builder attribute was not provided.
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}
/// Event generator that produces events at scheduled intervals.
pub struct Subscriber {
    /// Configuration settings for event generation.
    config: Arc<super::config::Subscriber>,
    /// Channel sender for broadcasting generated events.
    tx: Sender<Event>,
    /// Task identifier for event tracking.
    current_task_id: usize,
}

impl crate::task::runner::Runner for Subscriber {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let mut counter = 0;
        loop {
            time::sleep(Duration::from_secs(self.config.interval)).await;
            counter += 1;

            let data = match &self.config.message {
                Some(message) => json!(message),
                None => json!(null),
            };

            // Generate event subject.
            let subject = generate_subject(
                self.config.label.as_deref(),
                DEFAULT_MESSAGE_SUBJECT,
                SubjectSuffix::Timestamp,
            );
            // Build and send event.
            let e = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(subject)
                .current_task_id(self.current_task_id)
                .build()?;

            e.log();
            self.tx.send(e)?;

            match self.config.count {
                Some(count) if count == counter => break,
                Some(_) | None => continue,
            }
        }
        Ok(())
    }
}

/// Builder for constructing Subscriber instances.
#[derive(Default)]
pub struct SubscriberBuilder {
    /// Generate task configuration (required for build).
    config: Option<Arc<super::config::Subscriber>>,
    /// Event broadcast sender (required for build).
    tx: Option<Sender<Event>>,
    /// Current task identifier for event tracking.
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
