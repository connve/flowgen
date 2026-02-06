//! Event generation subscriber for producing scheduled synthetic events.
//!
//! Implements a timer-based event generator that creates events at regular intervals
//! with optional message content and count limits for testing and simulation workflows.

use crate::event::{Event, EventBuilder, EventData, EventExt};
use chrono::DateTime;
use croner::Cron;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc::Sender, time};
use tracing::{error, warn, Instrument};

/// System information included in generated events for time-based filtering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Last run time in seconds since UNIX epoch (if available from cache).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_time: Option<u64>,
    /// Next scheduled run time in seconds since UNIX epoch (if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_run_time: Option<u64>,
}

/// Errors that can occur during generate task execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Sending event to channel failed: {source}")]
    SendMessage {
        #[source]
        source: crate::event::Error,
    },
    #[error("Subscriber event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("Cache operation failed with error: {_0}")]
    Cache(String),
    #[error("System time error: {source}")]
    SystemTime {
        #[source]
        source: std::time::SystemTimeError,
    },
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64),
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Invalid cron expression '{expression}': {source}")]
    InvalidCron {
        expression: String,
        #[source]
        source: croner::errors::CronError,
    },
    #[error("Cron schedule has no next occurrence")]
    CronNoNextOccurrence,
    #[error("Configuration validation failed: {source}")]
    ConfigValidation {
        #[source]
        source: crate::task::generate::config::ConfigError,
    },
}
/// Event handler for generating scheduled events.
pub struct EventHandler {
    config: Arc<crate::task::generate::config::Subscriber>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<crate::task::context::TaskContext>,
    task_type: &'static str,
}

impl EventHandler {
    /// Calculate next run time based on interval or cron schedule.
    fn calculate_next_run(&self, now: u64, last_run: Option<u64>) -> Result<u64, Error> {
        match (&self.config.interval, &self.config.cron) {
            // Use interval if it's configured.
            (Some(interval), _) => Ok(last_run
                .map(|t| t + interval.as_secs())
                .unwrap_or_else(|| now + interval.as_secs())),
            // Use cron expression if it's configured.
            (None, Some(cron_expr)) => {
                let cron = Cron::from_str(cron_expr).map_err(|e| Error::InvalidCron {
                    expression: cron_expr.clone(),
                    source: e,
                })?;

                let datetime = DateTime::from_timestamp(now as i64, 0)
                    .ok_or_else(|| Error::InvalidTimestamp(now as i64))?;

                let next = cron.find_next_occurrence(&datetime, false).map_err(|e| {
                    Error::InvalidCron {
                        expression: cron_expr.clone(),
                        source: e,
                    }
                })?;

                Ok(next.timestamp() as u64)
            }
            // If none configured, return error.
            (None, None) => Err(Error::ConfigValidation {
                source: crate::task::generate::config::ConfigError::MissingSchedule,
            }),
        }
    }

    /// Generates events at scheduled intervals.
    async fn handle(&self) -> Result<(), Error> {
        // Note: This generator creates events from scratch (no incoming events),
        // so we don't use with_event_context() here. EventBuilder::new() will
        // create events with meta: None, which is correct for a pipeline starter.
        let mut counter = 0;

        // Get cache from task context if available.
        let cache = self.task_context.cache.as_ref();

        // Generate a cache_key based on flow name, task type, and task name.
        let cache_key = format!(
            "{flow_name}.{task_type}.{task_name}.last_run",
            flow_name = self.task_context.flow.name,
            task_type = self.task_type,
            task_name = self.config.name
        );

        loop {
            // Calculate now timestamp.
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::SystemTime { source: e })?
                .as_secs();

            // Get last_run from cache or return none.
            let last_run = match cache {
                Some(c) => c
                    .get(&cache_key)
                    .await
                    .ok()
                    .and_then(|bytes| String::from_utf8_lossy(&bytes).parse::<u64>().ok()),
                None => None,
            };

            // Calculate next run time for interval or cron.
            let next_run_time = self.calculate_next_run(now, last_run)?;

            // Sleep until it's time to generate the next event.
            if next_run_time > now {
                let sleep_duration = next_run_time - now;
                time::sleep(Duration::from_secs(sleep_duration)).await;
            }

            // Get current time after sleeping (actual execution time).
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::SystemTime { source: e })?
                .as_secs();

            // Update cache with current execution time before sending the event to ensure
            // we don't lose track of execution times if the process crashes.
            if let Some(cache) = cache {
                if let Err(cache_err) = cache.put(&cache_key, current_time.to_string().into()).await
                {
                    // Log warn for cache errors.
                    warn!("Failed to update cache: {:?}", cache_err);
                }
            }

            // Determine if there will be a next run
            let next_run_time_val = match self.config.count {
                Some(count) if count == counter + 1 => None,
                _ => Some(self.calculate_next_run(current_time, Some(current_time))?),
            };

            // Create system information.
            let system_info = SystemInfo {
                last_run_time: last_run,
                next_run_time: next_run_time_val,
            };

            // Prepare message data with system information.
            let data = match &self.config.message {
                Some(message) => {
                    json!({
                        "message": message,
                        "system_info": system_info
                    })
                }
                None => {
                    json!({
                        "system_info": system_info
                    })
                }
            };

            // Build and send event.
            let e = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(self.config.name.to_owned())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;
            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            counter += 1;
            match self.config.count {
                Some(count) if count == counter => return Ok(()),
                Some(_) | None => {}
            }
        }
    }
}

/// Event generator that produces events at scheduled intervals.
#[derive(Debug)]
pub struct Subscriber {
    /// Configuration settings for event generation.
    config: Arc<crate::task::generate::config::Subscriber>,
    /// Channel sender for broadcasting generated events.
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Subscriber {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        self.config
            .validate()
            .map_err(|source| Error::ConfigValidation { source })?;

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_context: Arc::clone(&self.task_context),
            task_type: self.task_type,
        })
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        // Spawn task init.
        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(handler) => handler,
            Err(e) => {
                error!(
                    "{}",
                    Error::RetryExhausted {
                        source: Box::new(e)
                    }
                );
                return Ok(());
            }
        };

        // Spawn event handler task.
        let retry_strategy = retry_config.strategy();
        tokio::spawn(
            async move {
                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                    event_handler.handle().await
                })
                .await;

                if let Err(e) = result {
                    error!(
                        "{}",
                        Error::RetryExhausted {
                            source: Box::new(e)
                        }
                    );
                }
            }
            .instrument(tracing::Span::current()),
        );

        Ok(())
    }
}

/// Builder for constructing Subscriber instances.
#[derive(Default)]
pub struct SubscriberBuilder {
    /// Generate task configuration (required for build).
    config: Option<Arc<crate::task::generate::config::Subscriber>>,
    /// Event broadcast sender (required for build).
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<crate::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<crate::task::generate::config::Subscriber>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::runner::Runner;
    use serde_json::{Map, Value};
    use std::collections::HashMap;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;

    /// Mock cache implementation for testing.
    #[derive(Debug)]
    struct MockCache {
        data: Arc<Mutex<HashMap<String, bytes::Bytes>>>,
        should_error: bool,
    }

    impl Default for MockCache {
        fn default() -> Self {
            MockCache {
                data: Arc::new(Mutex::new(HashMap::new())),
                should_error: false,
            }
        }
    }

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(crate::task::manager::TaskManagerBuilder::new().build());
        Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .build()
                .unwrap(),
        )
    }

    #[derive(Debug)]
    struct MockError;

    impl std::fmt::Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Mock cache error")
        }
    }

    impl std::error::Error for MockError {}

    #[async_trait::async_trait]
    impl crate::cache::Cache for MockCache {
        async fn put(&self, key: &str, value: bytes::Bytes) -> Result<(), crate::cache::Error> {
            if self.should_error {
                Err(Box::new(MockError))
            } else {
                self.data.lock().await.insert(key.to_string(), value);
                Ok(())
            }
        }

        async fn get(&self, key: &str) -> Result<bytes::Bytes, crate::cache::Error> {
            if self.should_error {
                Err(Box::new(MockError))
            } else {
                self.data
                    .lock()
                    .await
                    .get(key)
                    .cloned()
                    .ok_or_else(|| Box::new(MockError) as crate::cache::Error)
            }
        }

        async fn delete(&self, key: &str) -> Result<(), crate::cache::Error> {
            if self.should_error {
                Err(Box::new(MockError))
            } else {
                self.data.lock().await.remove(key);
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn test_subscriber_builder() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            message: Some("test message".to_string()),
            interval: Some(Duration::from_secs(1)),
            cron: None,
            count: Some(1),
            retry: None,
        });
        let (tx, _rx) = mpsc::channel(100);

        // Success case.
        let subscriber = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(subscriber.is_ok());

        // Error case - missing config.
        let (tx2, _rx2) = mpsc::channel(100);
        let result = SubscriberBuilder::new()
            .sender(tx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_subscriber_run_with_count() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            message: Some("test message".to_string()),
            interval: Some(Duration::from_secs(0)),
            cron: None,
            count: Some(2),
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let subscriber = Subscriber {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let handle = tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        let event1 = rx.recv().await.unwrap();
        let event2 = rx.recv().await.unwrap();

        assert_eq!(event1.subject, "test");
        assert_eq!(event2.subject, "test");
        assert_eq!(event1.task_id, 1);
        assert_eq!(event2.task_id, 1);

        let _ = handle.await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_subscriber_event_content() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            message: Some("custom message".to_string()),
            interval: Some(Duration::from_secs(0)),
            cron: None,
            count: Some(1),
            retry: None,
        });

        let (tx, mut rx) = mpsc::channel(100);

        let subscriber = Subscriber {
            config,
            tx: Some(tx),
            task_id: 0,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        let event = rx.recv().await.unwrap();

        match event.data {
            EventData::Json(value) => {
                assert_eq!(value["message"], "custom message");
                assert!(value["system_info"].is_object());
                // First run with count=1, so no last_run_time and no next_run_time
                assert!(value["system_info"]["last_run_time"].is_null());
                assert!(value["system_info"]["next_run_time"].is_null());
            }
            _ => panic!("Expected JSON event data"),
        }
    }

    #[tokio::test]
    async fn test_cache_key_generation() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            message: None,
            interval: Some(Duration::from_secs(1)),
            cron: None,
            count: Some(1), // Only run once
            retry: None,
        });

        let (tx, mut _rx) = mpsc::channel(100);
        let mock_cache = Arc::new(MockCache::default());

        // Create task context with cache
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Cache Test".to_string()),
        );
        let task_manager = Arc::new(crate::task::manager::TaskManagerBuilder::new().build());
        let task_context = Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(Some(mock_cache.clone() as Arc<dyn crate::cache::Cache>))
                .build()
                .unwrap(),
        );

        let subscriber = Subscriber {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context,
        };

        // Run subscriber to completion
        let _ = subscriber.run().await;

        // Wait for the spawned task to complete (interval is 1s, so wait 1.5s to be safe).
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // Check that cache key was created with flow.task_type.task format
        let cache_data = mock_cache.data.lock().await;
        assert!(cache_data.contains_key("test-flow.test.test.last_run"));
    }
}
