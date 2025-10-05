//! Event generation subscriber for producing scheduled synthetic events.
//!
//! Implements a timer-based event generator that creates events at regular intervals
//! with optional message content and count limits for testing and simulation workflows.

use crate::event::{
    generate_subject, Event, EventBuilder, EventData, SubjectSuffix, DEFAULT_LOG_MESSAGE,
};
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::broadcast::Sender, time};
use tracing::{debug, error, info, warn};

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
    /// Cache operation error with descriptive message.
    #[error("Cache error: {_0}")]
    Cache(String),
    /// System time error when getting current timestamp.
    #[error(transparent)]
    SystemTime(#[from] std::time::SystemTimeError),
    /// Host coordination error.
    #[error("Host coordination error")]
    Host(#[source] crate::host::Error),
    /// Task manager error.
    #[error(transparent)]
    TaskManager(#[from] crate::task::manager::Error),
}
/// Event generator that produces events at scheduled intervals.
#[derive(Debug)]
pub struct Subscriber<T: crate::cache::Cache> {
    /// Configuration settings for event generation.
    config: Arc<super::config::Subscriber>,
    /// Channel sender for broadcasting generated events.
    tx: Sender<Event>,
    /// Task identifier for event tracking.
    current_task_id: usize,
    /// Cache instance for storing last run time.
    cache: Arc<T>,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
}

impl<T: crate::cache::Cache> crate::task::runner::Runner for Subscriber<T> {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let mut counter = 0;

        // Register task with task manager.
        let task_id = format!(
            "{}.{}.{}",
            self.task_context.flow.name, DEFAULT_MESSAGE_SUBJECT, self.config.name
        );
        let mut task_manager_rx = self
            .task_context
            .task_manager
            .register(
                task_id,
                Some(crate::task::manager::LeaderElectionOptions {}),
            )
            .await?;

        // Generate a cache_key based on flow name and task name.
        let cache_key = format!(
            "{task_context}.{DEFAULT_MESSAGE_SUBJECT}.{task_name}.last_run",
            task_context = self.task_context.flow.name,
            task_name = self.config.name
        );

        loop {
            // Check for leadership changes.
            tokio::select! {
                biased;

                // Check for leadership election results.
                Some(status) = task_manager_rx.recv() => {
                    if status == crate::task::manager::LeaderElectionResult::NotLeader {
                        debug!("Lost leadership for task: {}", self.config.name);
                        return Ok(());
                    }
                }

                // Continue with normal event generation logic.
                _ = async {
                    // Calculate when the next event should be generated.
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                    let next_run_time = match self.cache.get(&cache_key).await {
                        Ok(cached_bytes) => {
                            let cached_str = String::from_utf8_lossy(&cached_bytes);
                            match cached_str.parse::<u64>() {
                                Ok(last_run) => last_run + self.config.interval,
                                Err(_) => now, // Invalid cache, run immediately.
                            }
                        }
                        Err(_) => now, // No cache entry, run immediately.
                    };

                    // Sleep until it's time to generate the next event.
                    if next_run_time > now {
                        let sleep_duration = next_run_time - now;
                        time::sleep(Duration::from_secs(sleep_duration)).await;
                    }
                } =>
            {
            // Prepare message data.
            let data = match &self.config.message {
                Some(message) => json!(message),
                None => json!(null),
            };

            // Generate event subject.
            let subject = generate_subject(
                &self.config.name,
                DEFAULT_MESSAGE_SUBJECT,
                SubjectSuffix::Timestamp,
            );

            // Build and send event.
            let e = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(subject.clone())
                .current_task_id(self.current_task_id)
                .build()?;
            self.tx.send(e)?;
            info!("{}: {}", DEFAULT_LOG_MESSAGE, subject);

            // Update cache with current time after sending the event.
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            if let Err(cache_err) = self
                .cache
                .put(&cache_key, current_time.to_string().into())
                .await
            {
                // Log warn for cache errors.
                warn!("Failed to update cache: {:?}", cache_err);
            }

                    counter += 1;
                    match self.config.count {
                        Some(count) if count == counter => return Ok(()),
                        Some(_) | None => {}
                    }
                }
            }
        }
    }
}

/// Builder for constructing Subscriber instances.
#[derive(Default)]
pub struct SubscriberBuilder<T: crate::cache::Cache> {
    /// Generate task configuration (required for build).
    config: Option<Arc<super::config::Subscriber>>,
    /// Event broadcast sender (required for build).
    tx: Option<Sender<Event>>,
    /// Current task identifier for event tracking.
    current_task_id: usize,
    /// Cache instance for storing last run time.
    cache: Option<Arc<T>>,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<crate::task::context::TaskContext>>,
}

impl<T: crate::cache::Cache> SubscriberBuilder<T>
where
    T: Default,
{
    pub fn new() -> SubscriberBuilder<T> {
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

    pub fn cache(mut self, cache: Arc<T>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub async fn build(self) -> Result<Subscriber<T>, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
            cache: self
                .cache
                .ok_or_else(|| Error::MissingRequiredAttribute("cache".to_string()))?,
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
        Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
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

    impl crate::cache::Cache for MockCache {
        type Error = MockError;

        async fn init(self, _bucket: &str) -> Result<Self, Self::Error> {
            if self.should_error {
                Err(MockError)
            } else {
                Ok(self)
            }
        }

        async fn put(&self, key: &str, value: bytes::Bytes) -> Result<(), Self::Error> {
            if self.should_error {
                Err(MockError)
            } else {
                self.data.lock().await.insert(key.to_string(), value);
                Ok(())
            }
        }

        async fn get(&self, key: &str) -> Result<bytes::Bytes, Self::Error> {
            if self.should_error {
                Err(MockError)
            } else {
                self.data.lock().await.get(key).cloned().ok_or(MockError)
            }
        }
    }

    #[test]
    fn test_subscriber_builder_new() {
        let builder = SubscriberBuilder::<MockCache>::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.cache.is_none());
        assert!(builder.task_context.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_subscriber_builder_build_success() {
        let config = Arc::new(crate::generate::config::Subscriber {
            name: "test".to_string(),
            message: Some("test message".to_string()),
            interval: 1,
            count: Some(1),
        });

        let (tx, _rx) = broadcast::channel(100);
        let cache = Arc::new(MockCache::default());

        let subscriber = SubscriberBuilder::<MockCache>::new()
            .config(config.clone())
            .sender(tx)
            .current_task_id(1)
            .cache(cache)
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
        let cache = Arc::new(MockCache::default());

        let result = SubscriberBuilder::<MockCache>::new()
            .sender(tx)
            .cache(cache)
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
        let config = Arc::new(crate::generate::config::Subscriber::default());
        let cache = Arc::new(MockCache::default());

        let result = SubscriberBuilder::<MockCache>::new()
            .config(config)
            .cache(cache)
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
    async fn test_subscriber_builder_missing_cache() {
        let config = Arc::new(crate::generate::config::Subscriber::default());
        let (tx, _rx) = broadcast::channel(100);

        let result = SubscriberBuilder::<MockCache>::new()
            .config(config)
            .sender(tx)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: cache"));
    }

    #[tokio::test]
    async fn test_subscriber_builder_missing_task_context() {
        let config = Arc::new(crate::generate::config::Subscriber::default());
        let (tx, _rx) = broadcast::channel(100);
        let cache = Arc::new(MockCache::default());

        let result = SubscriberBuilder::<MockCache>::new()
            .config(config)
            .sender(tx)
            .cache(cache)
            .build()
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: task_context"));
    }

    #[tokio::test]
    async fn test_subscriber_run_with_count() {
        let config = Arc::new(crate::generate::config::Subscriber {
            name: "test".to_string(),
            message: Some("test message".to_string()),
            interval: 0,
            count: Some(2),
        });

        let (tx, mut rx) = broadcast::channel(100);
        let cache = Arc::new(MockCache::default());

        let subscriber = Subscriber {
            config,
            tx,
            current_task_id: 1,
            cache,
            task_context: create_mock_task_context(),
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
        let config = Arc::new(crate::generate::config::Subscriber {
            name: "test".to_string(),
            message: Some("custom message".to_string()),
            interval: 0,
            count: Some(1),
        });

        let (tx, mut rx) = broadcast::channel(100);
        let cache = Arc::new(MockCache::default());

        let subscriber = Subscriber {
            config,
            tx,
            current_task_id: 0,
            cache,
            task_context: create_mock_task_context(),
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

    #[tokio::test]
    async fn test_cache_key_generation() {
        let config = Arc::new(crate::generate::config::Subscriber {
            name: "test".to_string(),
            message: None,
            interval: 1,    // Short interval for testing
            count: Some(1), // Only run once
        });

        let (tx, mut _rx) = broadcast::channel(100);
        let cache = Arc::new(MockCache::default());

        let subscriber = Subscriber {
            config,
            tx,
            current_task_id: 1,
            cache: cache.clone(),
            task_context: create_mock_task_context(),
        };

        // Run subscriber to completion
        let _ = subscriber.run().await;

        // Check that cache key was created with label format
        let cache_data = cache.data.lock().await;
        assert!(cache_data.contains_key("generate.my_task.last_run"));
    }
}
