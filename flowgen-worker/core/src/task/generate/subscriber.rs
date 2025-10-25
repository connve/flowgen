//! Event generation subscriber for producing scheduled synthetic events.
//!
//! Implements a timer-based event generator that creates events at regular intervals
//! with optional message content and count limits for testing and simulation workflows.

use crate::event::{generate_subject, Event, EventBuilder, EventData, SenderExt, SubjectSuffix};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::broadcast::Sender, time};
use tracing::{error, warn, Instrument};

/// Default subject prefix for generated events.
const DEFAULT_MESSAGE_SUBJECT: &str = "generate";

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
    /// Failed to send event through broadcast channel.
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: tokio::sync::broadcast::error::SendError<Event>,
    },
    /// Event construction failed.
    #[error(transparent)]
    Event(#[from] crate::event::Error),
    /// Required builder attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Cache operation error with descriptive message.
    #[error("Cache error: {_0}")]
    Cache(String),
    /// System time error when getting current timestamp.
    #[error("System time error: {source}")]
    SystemTime {
        #[source]
        source: std::time::SystemTimeError,
    },
    /// Host coordination error.
    #[error("Host coordination error")]
    Host(#[source] crate::host::Error),
}
/// Event handler for generating scheduled events.
pub struct EventHandler {
    config: Arc<crate::task::generate::config::Subscriber>,
    tx: Sender<Event>,
    current_task_id: usize,
    task_context: Arc<crate::task::context::TaskContext>,
}

impl EventHandler {
    /// Generates events at scheduled intervals.
    async fn handle(self) -> Result<(), Error> {
        let mut counter = 0;

        // Get cache from task context if available.
        let cache = self.task_context.cache.as_ref();

        // Generate a cache_key based on flow name and task name.
        let cache_key = format!(
            "{flow_name}.{DEFAULT_MESSAGE_SUBJECT}.{task_name}.last_run",
            flow_name = self.task_context.flow.name,
            task_name = self.config.name
        );

        loop {
            // Calculate when the next event should be generated.
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::SystemTime { source: e })?
                .as_secs();

            let next_run_time = if let Some(cache) = cache {
                match cache.get(&cache_key).await {
                    Ok(cached_bytes) => {
                        let cached_str = String::from_utf8_lossy(&cached_bytes);
                        match cached_str.parse::<u64>() {
                            Ok(last_run) => last_run + self.config.interval,
                            Err(_) => now, // Invalid cache, run immediately.
                        }
                    }
                    Err(_) => now, // No cache entry, run immediately.
                }
            } else {
                now // No cache available, run immediately.
            };

            // Sleep until it's time to generate the next event.
            if next_run_time > now {
                let sleep_duration = next_run_time - now;
                time::sleep(Duration::from_secs(sleep_duration)).await;
            }

            // Get last_run_time from cache for system info
            let last_run_time = if let Some(cache) = cache {
                match cache.get(&cache_key).await {
                    Ok(cached_bytes) => {
                        let cached_str = String::from_utf8_lossy(&cached_bytes);
                        cached_str.parse::<u64>().ok()
                    }
                    Err(_) => None,
                }
            } else {
                None
            };

            // Determine if there will be a next run
            let next_run_time_val = match self.config.count {
                Some(count) if count == counter + 1 => None, // This is the last run
                _ => {
                    let current_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|e| Error::SystemTime { source: e })?
                        .as_secs();
                    Some(current_time + self.config.interval)
                }
            };

            // Create system information
            let system_info = SystemInfo {
                last_run_time,
                next_run_time: next_run_time_val,
            };

            // Prepare message data with system information
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

            // Generate event subject.
            let subject = generate_subject(
                Some(&self.config.name),
                DEFAULT_MESSAGE_SUBJECT,
                SubjectSuffix::Timestamp,
            );

            // Build and send event.
            let mut event_builder = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(subject.clone())
                .current_task_id(self.current_task_id);

            if let Some(task_type) = self.task_context.task_type {
                event_builder = event_builder.task_type(task_type);
            }

            let e = event_builder.build()?;
            self.tx
                .send_with_logging(e)
                .map_err(|e| Error::SendMessage { source: e })?;

            // Update cache with current time after sending the event.
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::SystemTime { source: e })?
                .as_secs();
            if let Some(cache) = cache {
                if let Err(cache_err) = cache.put(&cache_key, current_time.to_string().into()).await
                {
                    // Log warn for cache errors.
                    warn!("Failed to update cache: {:?}", cache_err);
                }
            }

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
    tx: Sender<Event>,
    /// Task identifier for event tracking.
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Subscriber {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        Ok(EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            current_task_id: self.current_task_id,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.current_task_id))]
    async fn run(self) -> Result<(), Error> {
        let event_handler = self.init().await?;

        // Spawn event handler task.
        tokio::spawn(
            async move {
                if let Err(e) = event_handler.handle().await {
                    error!("{}", e);
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
    /// Current task identifier for event tracking.
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<crate::task::context::TaskContext>>,
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

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
        self.task_context = Some(task_context);
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
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::runner::Runner;
    use serde_json::{Map, Value};
    use std::collections::HashMap;
    use tokio::sync::{broadcast, Mutex};

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
                .task_type("test")
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
    }

    #[test]
    fn test_subscriber_builder_new() {
        let builder = SubscriberBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.task_context.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_subscriber_builder_build_success() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            message: Some("test message".to_string()),
            interval: 1,
            count: Some(1),
        });

        let (tx, _rx) = broadcast::channel(100);

        let subscriber = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx)
            .current_task_id(1)
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        assert_eq!(subscriber.current_task_id, 1);
        assert_eq!(subscriber.config.interval, 1);
    }

    #[tokio::test]
    async fn test_subscriber_builder_missing_config() {
        let (tx, _rx) = broadcast::channel(100);

        let result = SubscriberBuilder::new()
            .sender(tx)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: config"));
    }

    #[tokio::test]
    async fn test_subscriber_builder_missing_sender() {
        let config = Arc::new(crate::task::generate::config::Subscriber::default());

        let result = SubscriberBuilder::new()
            .config(config)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: sender"));
    }

    #[tokio::test]
    async fn test_subscriber_run_with_count() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            message: Some("test message".to_string()),
            interval: 0,
            count: Some(2),
        });

        let (tx, mut rx) = broadcast::channel(100);

        let subscriber = Subscriber {
            config,
            tx,
            current_task_id: 1,
            task_context: create_mock_task_context(),
        };

        let handle = tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        let event1 = rx.recv().await.unwrap();
        let event2 = rx.recv().await.unwrap();

        assert!(event1.subject.starts_with("generate.test."));
        assert!(event2.subject.starts_with("generate.test."));
        assert_eq!(event1.current_task_id, Some(1));
        assert_eq!(event2.current_task_id, Some(1));

        let _ = handle.await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_subscriber_event_content() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            message: Some("custom message".to_string()),
            interval: 0,
            count: Some(1),
        });

        let (tx, mut rx) = broadcast::channel(100);

        let subscriber = Subscriber {
            config,
            tx,
            current_task_id: 0,
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
            interval: 1,    // Short interval for testing
            count: Some(1), // Only run once
        });

        let (tx, mut _rx) = broadcast::channel(100);
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
            tx,
            current_task_id: 1,
            task_context,
        };

        // Run subscriber to completion
        let _ = subscriber.run().await;

        // Wait a bit for the spawned task to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Check that cache key was created with label format
        let cache_data = mock_cache.data.lock().await;
        assert!(cache_data.contains_key("test-flow.generate.test.last_run"));
    }

    #[tokio::test]
    async fn test_subscriber_builder_build_missing_task_context() {
        let config = Arc::new(crate::task::generate::config::Subscriber::default());
        let (tx, _rx) = broadcast::channel(100);

        let result = SubscriberBuilder::new()
            .config(config)
            .sender(tx)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "task_context")
        );
    }
}
