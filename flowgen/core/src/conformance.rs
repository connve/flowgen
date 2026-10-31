//! Tests every source task passes against its real server.
//!
//! A connector implements [`Source`] for its task and server, and
//! [`source_conformance!`](crate::source_conformance) turns it into one test
//! per case.

use crate::cache::{memory::MemoryCache, Cache};
use crate::event::Event;
use crate::retry::RetryConfig;
use crate::task::context::{TaskContext, TaskContextBuilder};
use crate::task::manager::TaskManagerBuilder;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// How long a stopped source may take to return.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a record may take to be emitted.
const ARRIVAL_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the suite waits for a record that must not be emitted.
const QUIET: Duration = Duration::from_secs(3);

/// How long the suite waits for a probe before it writes the next one.
const PROBE_INTERVAL: Duration = Duration::from_secs(2);

/// Payload prefix of the records written until the source reads, which the
/// cases complete and skip.
const PROBE: &str = "probe-";

/// Held by each case, so the cases of a test binary start one server at a
/// time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Waits until no other case of this test binary runs.
pub async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL.lock().await
}

/// A source task and the server it reads from.
#[async_trait::async_trait]
pub trait Source: Send + Sync {
    /// Writes a record carrying `payload` to the server.
    async fn produce(&self, payload: &str);

    /// Starts the source task with `context`, sending its events to `tx`.
    async fn start(&self, context: Arc<TaskContext>, tx: Sender<Event>) -> JoinHandle<()>;

    /// The payload of an event the source emitted.
    fn payload(&self, event: &Event) -> String;

    /// Longer than the server lets an idle connection or request wait.
    fn idle(&self) -> Duration;

    /// Longer than the server waits for an unfinished record before it sends
    /// it again.
    fn ack_deadline(&self) -> Duration;
}

/// Every record written while the source runs is emitted once.
pub async fn delivers_each_record_once(source: &impl Source) {
    let mut running = Running::start(source, &new_cache(), 16).await;
    running.ready().await;
    let written = ["a", "b", "c", "d", "e"];
    for payload in written {
        source.produce(payload).await;
    }

    let mut emitted = running.receive(written.len()).await;
    running.quiet().await;
    running.stop().await;

    emitted.sort();
    assert_eq!(emitted, written);
}

/// A record written after the source idled past the server's timeouts is
/// emitted, and the idle source stops at once.
pub async fn delivers_after_idling(source: &impl Source) {
    let mut running = Running::start(source, &new_cache(), 16).await;
    running.ready().await;
    tokio::time::sleep(source.idle()).await;
    source.produce("after-idle").await;

    let emitted = running.receive(1).await;
    running.stop().await;

    assert_eq!(emitted, ["after-idle"]);
}

/// A record the flow fails is emitted again, and once completed, not again.
pub async fn delivers_a_failed_record_again(source: &impl Source) {
    let mut running = Running::start(source, &new_cache(), 16).await;
    running.ready().await;
    source.produce("failed").await;

    let failed = running.next_record("the record").await;
    fail(&failed);
    let again = running.next_record("the failed record again").await;
    complete(&again);
    running.quiet().await;
    running.stop().await;

    assert_eq!(source.payload(&failed), "failed");
    assert_eq!(source.payload(&again), "failed");
}

/// A record the flow holds when the source stops is emitted again after a
/// restart, and the stop returns at once.
pub async fn delivers_a_record_held_at_stop_after_a_restart(source: &impl Source) {
    let cache = new_cache();
    let mut running = Running::start(source, &cache, 16).await;
    running.ready().await;
    source.produce("held").await;
    let held = running.next_record("the record").await;
    running.stop().await;
    drop(held);

    let mut restarted = Running::start(source, &cache, 16).await;
    let again = restarted
        .next_record("the record held at stop after the restart")
        .await;
    complete(&again);
    restarted.quiet().await;
    restarted.stop().await;

    assert_eq!(source.payload(&again), "held");
}

/// Records that wait for a busy next task past the server's ack deadline are
/// emitted once each.
pub async fn delivers_once_to_a_busy_next_task(source: &impl Source) {
    let mut running = Running::start(source, &new_cache(), 1).await;
    running.ready().await;
    let written = ["x", "y", "z"];
    for payload in written {
        source.produce(payload).await;
    }
    tokio::time::sleep(source.ack_deadline() * 3).await;

    let mut emitted = running.receive(written.len()).await;
    running.quiet().await;
    running.stop().await;

    emitted.sort();
    assert_eq!(emitted, written);
}

/// Records the flow completed are not emitted again after a restart.
pub async fn completed_records_stay_done_after_a_restart(source: &impl Source) {
    let cache = new_cache();
    let mut running = Running::start(source, &cache, 16).await;
    running.ready().await;
    for payload in ["done-1", "done-2"] {
        source.produce(payload).await;
    }
    running.receive(2).await;
    running.stop().await;

    let mut restarted = Running::start(source, &cache, 16).await;
    source.produce("after-restart").await;
    let emitted = restarted.receive(1).await;
    restarted.quiet().await;
    restarted.stop().await;

    assert_eq!(emitted, ["after-restart"]);
}

/// Defines one test per case for the source `$source` evaluates to; each
/// test evaluates it again, so every case gets its own server.
#[macro_export]
macro_rules! source_conformance {
    ($source:expr) => {
        mod source_conformance {
            use super::*;

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn delivers_each_record_once() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::delivers_each_record_once(&$source).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn delivers_after_idling() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::delivers_after_idling(&$source).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn delivers_a_failed_record_again() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::delivers_a_failed_record_again(&$source).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn delivers_a_record_held_at_stop_after_a_restart() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::delivers_a_record_held_at_stop_after_a_restart(&$source).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn delivers_once_to_a_busy_next_task() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::delivers_once_to_a_busy_next_task(&$source).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn completed_records_stay_done_after_a_restart() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::completed_records_stay_done_after_a_restart(&$source).await;
            }
        }
    };
}

fn new_cache() -> Arc<dyn Cache> {
    Arc::new(MemoryCache::new())
}

/// The context of a one-task flow whose single leaf is the suite.
fn context(cache: &Arc<dyn Cache>, stop: &CancellationToken) -> Arc<TaskContext> {
    let task_manager = TaskManagerBuilder::new()
        .build()
        .expect("build the task manager");
    let context = TaskContextBuilder::new()
        .flow_name("conformance".to_string())
        .task_manager(Arc::new(task_manager))
        .cache(Arc::clone(cache))
        .cancellation_token(stop.clone())
        .retry(RetryConfig {
            max_attempts: Some(3),
            initial_backoff: Duration::from_millis(100),
        })
        .build()
        .expect("build the task context");
    Arc::new(context)
}

fn complete(event: &Event) {
    if let Some(completion) = &event.completion_tx {
        completion.signal_completion(None);
    }
}

fn fail(event: &Event) {
    if let Some(completion) = &event.completion_tx {
        completion.signal_completion_with_error("Failed by the conformance suite".to_string());
    }
}

/// A started source and the events it emits.
struct Running<'a, S: Source> {
    source: &'a S,
    rx: Receiver<Event>,
    stop: CancellationToken,
    handle: JoinHandle<()>,
}

impl<'a, S: Source> Running<'a, S> {
    /// Starts `source` keeping its position in `cache`, emitting into a
    /// channel of `capacity`.
    async fn start(source: &'a S, cache: &Arc<dyn Cache>, capacity: usize) -> Self {
        let stop = CancellationToken::new();
        let (tx, rx) = mpsc::channel(capacity);
        let handle = source.start(context(cache, &stop), tx).await;
        Self {
            source,
            rx,
            stop,
            handle,
        }
    }

    /// Writes probes until the source emits one, so the records written next
    /// are read.
    async fn ready(&mut self) {
        let deadline = Instant::now() + ARRIVAL_TIMEOUT;
        for attempt in 0.. {
            assert!(
                Instant::now() < deadline,
                "the source emitted none of the records written after it started"
            );
            self.source.produce(&format!("{PROBE}{attempt}")).await;
            match tokio::time::timeout(PROBE_INTERVAL, self.rx.recv()).await {
                Ok(Some(event)) => {
                    complete(&event);
                    return;
                }
                Ok(None) => panic!("the source stopped before it emitted a record"),
                Err(_) => {}
            }
        }
    }

    /// The next event that is not a probe, within `within`.
    async fn next(&mut self, within: Duration) -> Option<Event> {
        let deadline = Instant::now() + within;
        loop {
            match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                Ok(Some(event)) if self.source.payload(&event).starts_with(PROBE) => {
                    complete(&event)
                }
                Ok(Some(event)) => return Some(event),
                Ok(None) | Err(_) => return None,
            }
        }
    }

    async fn next_record(&mut self, what: &str) -> Event {
        match self.next(ARRIVAL_TIMEOUT).await {
            Some(event) => event,
            None => panic!("the source did not emit {what}"),
        }
    }

    /// The payloads of the next `count` records, each completed.
    async fn receive(&mut self, count: usize) -> Vec<String> {
        let mut payloads = Vec::with_capacity(count);
        while payloads.len() < count {
            match self.next(ARRIVAL_TIMEOUT).await {
                Some(event) => {
                    complete(&event);
                    payloads.push(self.source.payload(&event));
                }
                None => panic!("the source emitted {payloads:?}, expected {count} records"),
            }
        }
        payloads
    }

    /// Asserts the source emits nothing more for a while.
    async fn quiet(&mut self) {
        if let Some(event) = self.next(QUIET).await {
            panic!("the source emitted '{}' again", self.source.payload(&event));
        }
    }

    /// Stops the source and asserts it returns at once.
    async fn stop(self) {
        self.stop.cancel();
        let stopped = tokio::time::timeout(STOP_TIMEOUT, self.handle).await;
        assert!(
            stopped.is_ok(),
            "the source did not stop within {STOP_TIMEOUT:?}"
        );
    }
}
