//! Event generation subscriber for producing scheduled synthetic events.
//!
//! Implements a timer-based event generator that creates events at regular intervals
//! with optional message content and count limits for testing and simulation workflows.

use crate::event::{
    generate_subject, Event, EventBuilder, EventData, SubjectSuffix, DEFAULT_LOG_MESSAGE,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast::Sender, time};
use tracing::{event, Level};

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
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}
/// Event generator that produces events at scheduled intervals.
#[derive(Debug)]
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

            event!(Level::INFO, "{}: {}", DEFAULT_LOG_MESSAGE, e.subject);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::runner::Runner;
    use tokio::sync::broadcast;

    #[test]
    fn test_subscriber_builder_new() {
        let builder = SubscriberBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_subscriber_builder_build_success() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            label: Some("test".to_string()),
            message: Some("test message".to_string()),
            interval: 1,
            count: Some(1),
        });

        let (tx, _rx) = broadcast::channel(100);

        let subscriber = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx)
            .current_task_id(1)
            .build()
            .await
            .unwrap();

        assert_eq!(subscriber.current_task_id, 1);
        assert_eq!(subscriber.config.interval, 1);
    }

    #[tokio::test]
    async fn test_subscriber_builder_missing_config() {
        let (tx, _rx) = broadcast::channel(100);

        let result = SubscriberBuilder::new().sender(tx).build().await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: config"));
    }

    #[tokio::test]
    async fn test_subscriber_builder_missing_sender() {
        let config = Arc::new(crate::task::generate::config::Subscriber::default());

        let result = SubscriberBuilder::new().config(config).build().await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: sender"));
    }

    #[tokio::test]
    async fn test_subscriber_run_with_count() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            label: Some("test".to_string()),
            message: Some("test message".to_string()),
            interval: 0,
            count: Some(2),
        });

        let (tx, mut rx) = broadcast::channel(100);

        let subscriber = Subscriber {
            config,
            tx,
            current_task_id: 1,
        };

        let handle = tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        let event1 = rx.recv().await.unwrap();
        let event2 = rx.recv().await.unwrap();

        assert!(event1.subject.starts_with("test."));
        assert!(event2.subject.starts_with("test."));
        assert_eq!(event1.current_task_id, Some(1));
        assert_eq!(event2.current_task_id, Some(1));

        let _ = handle.await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_subscriber_event_content() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            label: None,
            message: Some("custom message".to_string()),
            interval: 0,
            count: Some(1),
        });

        let (tx, mut rx) = broadcast::channel(100);

        let subscriber = Subscriber {
            config,
            tx,
            current_task_id: 0,
        };

        tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        let event = rx.recv().await.unwrap();

        match event.data {
            EventData::Json(value) => {
                assert_eq!(value, "custom message");
            }
            _ => panic!("Expected JSON event data"),
        }
    }
}
