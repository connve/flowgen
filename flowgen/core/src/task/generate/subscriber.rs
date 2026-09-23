//! Event generation subscriber for producing scheduled synthetic events.
//!
//! Implements a timer-based event generator that creates events at regular intervals
//! with optional structured data payloads and count limits for testing and simulation workflows.

use crate::event::{new_completion_channel, Event, EventBuilder, EventData, EventExt};
use crate::task::generate::config::Schedule;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::mpsc::Sender,
    time::{self, Instant},
};
use tracing::{error, warn};

/// System information included in generated events for time-based filtering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Time of the current run in seconds since UNIX epoch.
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
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: crate::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("Cache error for key '{key}': {source}")]
    Cache {
        key: String,
        #[source]
        source: crate::cache::Error,
    },
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
    #[error("Failed to compute the next cron occurrence: {source}")]
    CronNextOccurrence {
        #[source]
        source: croner::errors::CronError,
    },
    #[error("Flow did not complete the run-once event")]
    RunOnceNotCompleted,
    #[error("Configuration validation error: {source}")]
    ConfigValidation {
        #[source]
        source: crate::task::generate::config::ConfigError,
    },
}

/// When the next run is due.
#[derive(Debug, Clone, Copy)]
enum Due {
    /// Monotonic deadline, so wall-clock jumps do not skew intervals.
    At(Instant),
    /// Wall-clock second since the UNIX epoch, used for cron occurrences.
    WallClock(u64),
}

/// Current wall-clock time as a duration since the UNIX epoch.
fn unix_now() -> Result<Duration, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| Error::SystemTime { source })
}

/// Returns the first occurrence of `cron` strictly after `after`, in seconds since the UNIX epoch.
fn next_cron_occurrence(
    cron: &croner::Cron,
    timezone: &chrono_tz::Tz,
    after: Duration,
) -> Result<u64, Error> {
    let after_secs =
        i64::try_from(after.as_secs()).map_err(|_| Error::InvalidTimestamp(i64::MAX))?;
    let utc = DateTime::from_timestamp(after_secs, 0).ok_or(Error::InvalidTimestamp(after_secs))?;
    let next = cron
        .find_next_occurrence(&utc.with_timezone(timezone), false)
        .map_err(|source| Error::CronNextOccurrence { source })?;
    u64::try_from(next.timestamp()).map_err(|_| Error::InvalidTimestamp(next.timestamp()))
}

/// Event handler for generating scheduled events.
pub struct EventHandler {
    config: Arc<crate::task::generate::config::Subscriber>,
    schedule: Schedule,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<crate::task::context::TaskContext>,
    task_type: &'static str,
}

impl EventHandler {
    fn last_run_key(&self) -> String {
        format!(
            "flow.{}.last_run.{}",
            self.task_context.flow.id(),
            self.config.name
        )
    }

    fn counter_key(&self) -> String {
        format!(
            "flow.{}.counter.{}",
            self.task_context.flow.id(),
            self.config.name
        )
    }

    /// Reads a number persisted in the cache. A cache failure is an error so it
    /// is never mistaken for "never ran".
    async fn read_cached_u64(&self, key: &str) -> Result<Option<u64>, Error> {
        match self.task_context.cache.get(key).await {
            Ok(Some(bytes)) => match String::from_utf8_lossy(&bytes).parse::<u64>() {
                Ok(value) => Ok(Some(value)),
                Err(_) => {
                    warn!(key = %key, "Ignoring non-numeric value in generate cache");
                    Ok(None)
                }
            },
            Ok(None) => Ok(None),
            Err(source) => Err(Error::Cache {
                key: key.to_string(),
                source,
            }),
        }
    }

    async fn write_cached_u64(&self, key: &str, value: u64) -> Result<(), Error> {
        self.task_context
            .cache
            .put(key, value.to_string().into(), None)
            .await
            .map_err(|source| Error::Cache {
                key: key.to_string(),
                source,
            })
    }

    /// Resets the persisted counter when `allow_rerun` is set. Runs once per
    /// task start, so retries of `run_loop` resume instead of starting over.
    async fn reset_counter_if_rerun(&self) -> Result<(), Error> {
        match self.config.allow_rerun {
            true => self.write_cached_u64(&self.counter_key(), 0).await,
            false => Ok(()),
        }
    }

    /// When the first run after startup is due.
    async fn first_due(&self) -> Result<Due, Error> {
        match &self.schedule {
            Schedule::Interval(interval) => {
                // Capped at one interval so a last run stamped in the future
                // (clock skew, restored cache) cannot stall the task.
                let wait = match self.read_cached_u64(&self.last_run_key()).await? {
                    Some(last_run) => Duration::from_secs(last_run)
                        .saturating_add(*interval)
                        .saturating_sub(unix_now()?)
                        .min(*interval),
                    None => *interval,
                };
                Ok(Due::At(Instant::now() + wait))
            }
            Schedule::Cron { cron, timezone } => Ok(Due::WallClock(next_cron_occurrence(
                cron,
                timezone,
                unix_now()?,
            )?)),
            Schedule::Once => Ok(Due::At(Instant::now())),
        }
    }

    /// When the run after an attempt that started at `attempt_at` is due.
    /// Measuring from the attempt, not the last success, keeps a failing flow
    /// from being refired in a tight loop.
    fn next_due(&self, attempt_at: Instant) -> Result<Due, Error> {
        match &self.schedule {
            Schedule::Interval(interval) => Ok(Due::At(attempt_at + *interval)),
            Schedule::Cron { cron, timezone } => Ok(Due::WallClock(next_cron_occurrence(
                cron,
                timezone,
                unix_now()?,
            )?)),
            Schedule::Once => Ok(Due::At(Instant::now())),
        }
    }

    /// Wall-clock second of the run after one starting at `now`, for `system_info`.
    fn next_run_time(&self, now: Duration) -> Result<u64, Error> {
        match &self.schedule {
            Schedule::Interval(interval) => Ok((now + *interval).as_secs()),
            Schedule::Cron { cron, timezone } => next_cron_occurrence(cron, timezone, now),
            Schedule::Once => Ok(now.as_secs()),
        }
    }

    /// Sleeps until `deadline`. Returns false if the task was cancelled first.
    async fn sleep_until(&self, deadline: Instant) -> bool {
        tokio::select! {
            _ = time::sleep_until(deadline) => true,
            _ = self.task_context.cancellation_token.cancelled() => false,
        }
    }

    /// Waits until `due`. Returns false if the task was cancelled first.
    async fn wait_until(&self, due: Due) -> Result<bool, Error> {
        match due {
            Due::At(deadline) => Ok(self.sleep_until(deadline).await),
            Due::WallClock(secs) => {
                let target = Duration::from_secs(secs);
                // The monotonic clock can drift from the wall clock over long
                // waits, so re-check to never fire a cron run early.
                loop {
                    let remaining = target.saturating_sub(unix_now()?);
                    if remaining.is_zero() {
                        return Ok(true);
                    }
                    if !self.sleep_until(Instant::now() + remaining).await {
                        return Ok(false);
                    }
                }
            }
        }
    }

    /// Generates events on the configured schedule.
    async fn run_loop(&self) -> Result<(), Error> {
        let last_run_key = self.last_run_key();
        let counter_key = self.counter_key();

        // Resume the counter so a restart does not repeat completed runs.
        let mut counter = self
            .read_cached_u64(&counter_key)
            .await?
            .unwrap_or_default();
        match self.config.count {
            Some(count) if counter >= count => return Ok(()),
            Some(_) | None => {}
        }

        let mut due = self.first_due().await?;

        loop {
            if !self.wait_until(due).await? {
                return Ok(());
            }

            let attempt_at = Instant::now();
            let current_time = unix_now()?;

            let next_run_time = match self.config.count {
                Some(count) if count == counter + 1 => None,
                _ => Some(self.next_run_time(current_time)?),
            };
            let system_info = SystemInfo {
                last_run_time: Some(current_time.as_secs()),
                next_run_time,
            };

            let mut data = match &self.config.payload {
                Some(user_data) => user_data.clone(),
                None => Value::Object(Map::new()),
            };
            if let Some(obj) = data.as_object_mut() {
                obj.insert(
                    "system_info".to_string(),
                    serde_json::to_value(&system_info).unwrap_or(Value::Null),
                );
            }

            // Create a completion channel sized to the number of leaves in
            // this flow's directed acyclic graph. The source acks the
            // generated event only after every leaf has signalled.
            let (completion_state, completion_rx) =
                new_completion_channel(self.task_context.leaf_count);

            self.handle(data, completion_state).await?;

            let success = match self.config.ack_timeout {
                Some(timeout) => matches!(
                    tokio::time::timeout(timeout, completion_rx).await,
                    Ok(Ok(Ok(_)))
                ),
                None => matches!(completion_rx.await, Ok(Ok(_))),
            };

            // Persist progress only for completed runs, so a restart retries a failed one.
            if success && !self.task_context.cancellation_token.is_cancelled() {
                counter += 1;
                if let Err(e) = self
                    .write_cached_u64(&last_run_key, current_time.as_secs())
                    .await
                {
                    warn!(error = %e, "Failed to persist generate last run time");
                }
                if let Err(e) = self.write_cached_u64(&counter_key, counter).await {
                    warn!(error = %e, "Failed to persist generate counter");
                }
            } else {
                warn!("Flow completion failed or timed out");
                // Run-once has no schedule to wait for, so hand the failure
                // to the retry backoff instead of refiring immediately.
                if let Schedule::Once = self.schedule {
                    return Err(Error::RunOnceNotCompleted);
                }
            }

            match self.config.count {
                Some(count) if count == counter => return Ok(()),
                Some(_) | None => {}
            }

            due = self.next_due(attempt_at)?;
        }
    }

    #[tracing::instrument(skip(self, data, completion_state), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(
        &self,
        data: serde_json::Value,
        completion_state: crate::event::SharedCompletionTx,
    ) -> Result<(), Error> {
        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(self.config.name.to_owned())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .completion_tx(completion_state)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;
        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|source| Error::SendMessage { source })
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
        let schedule = self
            .config
            .schedule()
            .map_err(|source| Error::ConfigValidation { source })?;

        let handler = EventHandler {
            config: Arc::clone(&self.config),
            schedule,
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_context: Arc::clone(&self.task_context),
            task_type: self.task_type,
        };
        handler.reset_counter_if_rerun().await?;
        Ok(handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let cancellation_token = self.task_context.cancellation_token.clone();

        // Initialise with retry. Cancellation during init returns early so the
        // task tears down without waiting for the retry strategy to exhaust.
        let init_future = tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    // A bad config cannot fix itself, so fail fast instead of retrying.
                    Err(e @ Error::ConfigValidation { .. }) => {
                        error!(error = %e, "Invalid generate config");
                        Err(tokio_retry::RetryError::permanent(e))
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to initialize generate subscriber");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        );

        let event_handler = tokio::select! {
            _ = cancellation_token.cancelled() => return Ok(()),
            result = init_future => match result {
                Ok(handler) => handler,
                Err(e) => return Err(e),
            },
        };

        // Scheduled tasks are long-lived and retry forever like other
        // subscribers; run-once gives up after the configured attempts.
        // Cancellation also short-circuits the sleeps inside `run_loop()`.
        let retry_strategy = match event_handler.schedule {
            Schedule::Once => retry_config.strategy(),
            Schedule::Interval(_) | Schedule::Cron { .. } => retry_config.reconnect_strategy(),
        };
        let handle_future = tokio_retry::Retry::spawn(retry_strategy, || async {
            event_handler
                .run_loop()
                .await
                .map_err(tokio_retry::RetryError::transient)
        });

        tokio::select! {
            _ = cancellation_token.cancelled() => Ok(()),
            result = handle_future => {
                match result {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        error!(error = %e, "Generate failed after all retry attempts");
                        Err(Error::RetryExhausted { source: Box::new(e) })
                    }
                }
            }
        }
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
    use crate::cache::Cache;
    use crate::task::generate::config::Subscriber as Config;
    use crate::task::runner::Runner;
    use serde_json::{json, Map, Value};
    use tokio::sync::mpsc;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_subscriber_builder() {
        let config = Arc::new(Config {
            name: "test".to_string(),
            payload: Some(json!({"test": "data"})),
            interval: Some(Duration::from_secs(1)),
            cron: None,
            count: Some(1),
            ack_timeout: None,
            retry: None,
            ..Default::default()
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

    fn memory_cache() -> Arc<dyn Cache> {
        Arc::new(crate::cache::memory::MemoryCache::new())
    }

    async fn read_cached(cache: &Arc<dyn Cache>, kind: &str) -> Option<u64> {
        cache
            .get(&cache_key(kind))
            .await
            .unwrap()
            .map(|bytes| String::from_utf8_lossy(&bytes).parse().unwrap())
    }

    fn complete(event: &Event) {
        if let Some(completion) = &event.completion_tx {
            completion.signal_completion(None);
        }
    }

    #[tokio::test]
    async fn test_subscriber_run_with_count() {
        let (subscriber, mut rx) = subscriber_with(
            Config {
                payload: Some(json!({"test": "data"})),
                interval: Some(Duration::from_secs(0)),
                count: Some(2),
                ..Default::default()
            },
            memory_cache(),
        );
        let handle = tokio::spawn(subscriber.run());

        let event1 = rx.recv().await.unwrap();
        complete(&event1);
        let event2 = rx.recv().await.unwrap();
        complete(&event2);

        assert_eq!(event1.subject, "test");
        assert_eq!(event2.subject, "test");
        assert_eq!(event1.task_id, 1);
        assert_eq!(event2.task_id, 1);

        let _ = handle.await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_subscriber_event_content() {
        let (subscriber, mut rx) = subscriber_with(
            Config {
                payload: Some(json!({"custom_field": "custom_value"})),
                interval: Some(Duration::from_secs(0)),
                count: Some(1),
                ..Default::default()
            },
            memory_cache(),
        );
        tokio::spawn(subscriber.run());

        let event = rx.recv().await.unwrap();

        match event.data {
            EventData::Json(value) => {
                assert_eq!(value["custom_field"], "custom_value");
                assert!(value["system_info"].is_object());
                assert!(value["system_info"]["last_run_time"].is_number());
                assert!(value["system_info"]["next_run_time"].is_null());
            }
            _ => panic!("Expected JSON event data"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_cache_key_generation() {
        let cache = memory_cache();
        let (subscriber, mut rx) = subscriber_with(
            Config {
                interval: Some(Duration::from_secs(1)),
                count: Some(1),
                ..Default::default()
            },
            Arc::clone(&cache),
        );
        let handle = tokio::spawn(subscriber.run());

        complete(&rx.recv().await.unwrap());
        let _ = handle.await;

        assert!(read_cached(&cache, "last_run").await.is_some());
        assert_eq!(read_cached(&cache, "counter").await, Some(1));
    }

    #[tokio::test]
    async fn test_count_resume_after_restart() {
        let cache = memory_cache();
        cache
            .put(&cache_key("counter"), "1".into(), None)
            .await
            .unwrap();
        let (subscriber, mut rx) = subscriber_with(
            Config {
                interval: Some(Duration::from_secs(0)),
                count: Some(2),
                ..Default::default()
            },
            Arc::clone(&cache),
        );
        let handle = tokio::spawn(subscriber.run());

        complete(&rx.recv().await.unwrap());
        let _ = handle.await;

        assert!(rx.try_recv().is_err());
        assert_eq!(read_cached(&cache, "counter").await, Some(2));
    }

    #[tokio::test(start_paused = true)]
    async fn test_count_one_with_long_interval_fires_once() {
        let cache = memory_cache();
        let (subscriber, mut rx) = subscriber_with(
            Config {
                interval: Some(Duration::from_secs(60)),
                count: Some(1),
                ..Default::default()
            },
            Arc::clone(&cache),
        );
        let started_at = Instant::now();
        let handle = tokio::spawn(subscriber.run());

        complete(&rx.recv().await.expect("expected single event"));
        let _ = handle.await;

        assert!(Instant::now() - started_at >= Duration::from_secs(60));
        assert!(
            rx.try_recv().is_err(),
            "generate with count=1 must not emit a second event"
        );
        assert_eq!(read_cached(&cache, "counter").await, Some(1));
    }

    #[tokio::test]
    async fn test_count_skip_when_already_complete() {
        let cache = memory_cache();
        cache
            .put(&cache_key("counter"), "3".into(), None)
            .await
            .unwrap();
        let (subscriber, mut rx) = subscriber_with(
            Config {
                interval: Some(Duration::from_secs(0)),
                count: Some(3),
                ..Default::default()
            },
            cache,
        );

        let _ = subscriber.run().await;

        assert!(rx.try_recv().is_err());
    }

    /// Cache whose first `failures` reads fail, then behaves like `MemoryCache`.
    #[derive(Debug)]
    struct FlakyCache {
        inner: crate::cache::memory::MemoryCache,
        failures: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::cache::Cache for FlakyCache {
        async fn put(
            &self,
            key: &str,
            value: bytes::Bytes,
            ttl_secs: Option<u64>,
        ) -> Result<(), crate::cache::Error> {
            self.inner.put(key, value, ttl_secs).await
        }

        async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>, crate::cache::Error> {
            let remaining = self.failures.fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| n.checked_sub(1),
            );
            match remaining {
                Ok(_) => Err(crate::cache::Error::StoreNotInitialized),
                Err(_) => self.inner.get(key).await,
            }
        }

        async fn delete(&self, key: &str) -> Result<(), crate::cache::Error> {
            self.inner.delete(key).await
        }

        async fn create(
            &self,
            key: &str,
            value: bytes::Bytes,
            ttl_secs: Option<u64>,
        ) -> Result<u64, crate::cache::Error> {
            self.inner.create(key, value, ttl_secs).await
        }

        async fn update(
            &self,
            key: &str,
            value: bytes::Bytes,
            expected_revision: u64,
            ttl_secs: Option<u64>,
        ) -> Result<u64, crate::cache::Error> {
            self.inner
                .update(key, value, expected_revision, ttl_secs)
                .await
        }

        async fn get_with_revision(
            &self,
            key: &str,
        ) -> Result<Option<(bytes::Bytes, u64)>, crate::cache::Error> {
            self.inner.get_with_revision(key).await
        }

        async fn delete_with_revision(
            &self,
            key: &str,
            expected_revision: u64,
        ) -> Result<(), crate::cache::Error> {
            self.inner
                .delete_with_revision(key, expected_revision)
                .await
        }

        async fn get_revision(&self, key: &str) -> Result<Option<u64>, crate::cache::Error> {
            self.inner.get_revision(key).await
        }

        async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, crate::cache::Error> {
            self.inner.list_keys(prefix).await
        }
    }

    fn cache_key(kind: &str) -> String {
        format!(
            "flow.{}.{kind}.test",
            crate::identity::encode_key("test-flow")
        )
    }

    fn subscriber_with(
        config: Config,
        cache: Arc<dyn Cache>,
    ) -> (Subscriber, mpsc::Receiver<Event>) {
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let task_context = Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        );
        let (tx, rx) = mpsc::channel(100);
        let subscriber = Subscriber {
            config: Arc::new(Config {
                name: "test".to_string(),
                ..config
            }),
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context,
        };
        (subscriber, rx)
    }

    #[tokio::test(start_paused = true)]
    async fn test_sub_second_interval_waits_between_events() {
        let (subscriber, mut rx) = subscriber_with(
            Config {
                interval: Some(Duration::from_millis(100)),
                count: Some(3),
                ..Default::default()
            },
            memory_cache(),
        );
        tokio::spawn(subscriber.run());

        let mut received_at = Vec::new();
        for _ in 0..3 {
            let event = rx.recv().await.expect("expected an event");
            received_at.push(Instant::now());
            if let Some(completion) = &event.completion_tx {
                completion.signal_completion(None);
            }
        }

        for pair in received_at.windows(2) {
            assert!(
                pair[1] - pair[0] >= Duration::from_millis(100),
                "events fired {:?} apart",
                pair[1] - pair[0]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_failed_run_waits_one_interval_before_refiring() {
        let cache = memory_cache();
        cache
            .put(&cache_key("last_run"), "0".into(), None)
            .await
            .unwrap();
        let (subscriber, mut rx) = subscriber_with(
            Config {
                interval: Some(Duration::from_secs(10)),
                ..Default::default()
            },
            cache,
        );
        tokio::spawn(subscriber.run());

        let first = rx.recv().await.expect("expected an overdue event");
        let first_at = Instant::now();
        if let Some(completion) = &first.completion_tx {
            completion.signal_completion_with_error("downstream failed".to_string());
        }

        rx.recv().await.expect("expected a refire");
        assert!(Instant::now() - first_at >= Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn test_failed_run_once_backs_off_before_refiring() {
        let (subscriber, mut rx) = subscriber_with(
            Config {
                count: Some(1),
                ..Default::default()
            },
            memory_cache(),
        );
        tokio::spawn(subscriber.run());

        let first = rx.recv().await.expect("expected an event");
        let first_at = Instant::now();
        if let Some(completion) = &first.completion_tx {
            completion.signal_completion_with_error("downstream failed".to_string());
        }

        rx.recv().await.expect("expected a retry");
        assert!(Instant::now() > first_at);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cache_read_error_does_not_rerun_completed_run_once() {
        let inner = crate::cache::memory::MemoryCache::new();
        inner
            .put(&cache_key("counter"), "1".into(), None)
            .await
            .unwrap();
        let cache = Arc::new(FlakyCache {
            inner,
            failures: std::sync::atomic::AtomicUsize::new(1),
        });
        let (subscriber, mut rx) = subscriber_with(
            Config {
                count: Some(1),
                ..Default::default()
            },
            cache,
        );

        let result = tokio::time::timeout(Duration::from_secs(60), subscriber.run()).await;

        assert!(rx.try_recv().is_err(), "completed run-once must not rerun");
        assert!(matches!(result, Ok(Ok(()))), "run should recover");
    }

    #[tokio::test(start_paused = true)]
    async fn test_future_last_run_waits_at_most_one_interval() {
        let cache = memory_cache();
        cache
            .put(&cache_key("last_run"), u64::MAX.to_string().into(), None)
            .await
            .unwrap();
        let (subscriber, mut rx) = subscriber_with(
            Config {
                interval: Some(Duration::from_secs(10)),
                count: Some(1),
                ..Default::default()
            },
            cache,
        );
        let started_at = Instant::now();
        tokio::spawn(subscriber.run());

        rx.recv().await.expect("expected an event");

        assert!(Instant::now() - started_at <= Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn test_scheduled_task_keeps_retrying_past_max_attempts() {
        let cache = Arc::new(FlakyCache {
            inner: crate::cache::memory::MemoryCache::new(),
            failures: std::sync::atomic::AtomicUsize::new(20),
        });
        let (subscriber, mut rx) = subscriber_with(
            Config {
                interval: Some(Duration::from_secs(1)),
                ..Default::default()
            },
            cache,
        );
        tokio::spawn(subscriber.run());

        rx.recv()
            .await
            .expect("scheduled task should outlast transient cache failures");
    }

    #[tokio::test(start_paused = true)]
    async fn test_invalid_cron_fails_without_retrying() {
        let (subscriber, _rx) = subscriber_with(
            Config {
                cron: Some("0 25 * * *".to_string()),
                ..Default::default()
            },
            memory_cache(),
        );
        let started_at = Instant::now();

        let result = subscriber.run().await;

        assert!(matches!(result, Err(Error::ConfigValidation { .. })));
        assert!(Instant::now() - started_at < Duration::from_secs(1));
    }
}
