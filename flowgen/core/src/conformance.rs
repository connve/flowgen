//! Tests every source and sink task passes against its real server.
//!
//! A connector implements [`Source`] or [`Sink`] for its task and server, and
//! [`source_conformance!`](crate::source_conformance) or
//! [`sink_conformance!`](crate::sink_conformance) turns it into one test per
//! case.

use crate::cache::{memory::MemoryCache, Cache};
use crate::delivery::{wait_for_completion, CompletionFailure};
use crate::event::{new_completion_channel, CompletionRx, Event};
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

/// A record in flight when the flow stops reading is finished and not emitted again.
pub async fn finishes_a_record_in_flight_when_it_stops_reading(source: &impl Source) {
    let cache = new_cache();
    let mut running = Running::start(source, &cache, 16).await;
    running.ready().await;
    source.produce("draining").await;
    let held = running.next_record("the record").await;
    running.drain(&held).await;

    let mut restarted = Running::start(source, &cache, 16).await;
    source.produce("after-drain").await;
    let emitted = restarted.receive(1).await;
    restarted.quiet().await;
    restarted.stop().await;

    assert_eq!(source.payload(&held), "draining");
    assert_eq!(emitted, ["after-drain"]);
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
            async fn finishes_a_record_in_flight_when_it_stops_reading() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::finishes_a_record_in_flight_when_it_stops_reading(&$source)
                    .await;
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
fn context(
    cache: &Arc<dyn Cache>,
    stop: &CancellationToken,
    retry: RetryConfig,
) -> Arc<TaskContext> {
    let task_manager = TaskManagerBuilder::new()
        .build()
        .expect("build the task manager");
    let context = TaskContextBuilder::new()
        .flow_name("conformance".to_string())
        .task_manager(Arc::new(task_manager))
        .cache(Arc::clone(cache))
        .cancellation_token(stop.clone())
        .retry(retry)
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
    stop_reading: CancellationToken,
    handle: JoinHandle<()>,
}

impl<'a, S: Source> Running<'a, S> {
    /// Starts `source` keeping its position in `cache`, emitting into a
    /// channel of `capacity`.
    async fn start(source: &'a S, cache: &Arc<dyn Cache>, capacity: usize) -> Self {
        let stop = CancellationToken::new();
        let (tx, rx) = mpsc::channel(capacity);
        let retry = RetryConfig {
            max_attempts: Some(3),
            initial_backoff: Duration::from_millis(100),
        };
        let context = context(cache, &stop, retry);
        let stop_reading = context.stop_reading.clone();
        let handle = source.start(context, tx).await;
        Self {
            source,
            rx,
            stop,
            stop_reading,
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

    /// Stops the source reading, completes `held`, and asserts the source then returns.
    async fn drain(self, held: &Event) {
        self.stop_reading.cancel();
        tokio::time::sleep(PROBE_INTERVAL).await;
        complete(held);
        let drained = tokio::time::timeout(STOP_TIMEOUT, self.handle).await;
        self.stop.cancel();
        assert!(
            drained.is_ok(),
            "the source did not return within {STOP_TIMEOUT:?} of finishing its record"
        );
    }

    /// Stops the source and asserts it returns at once.
    async fn stop(self) {
        self.stop.cancel();
        assert_returns(self.handle, "source").await;
    }
}

/// How long a sink may take to fail an event it cannot write.
const FAIL_TIMEOUT: Duration = Duration::from_secs(120);

/// How long the server stays down in the case the sink recovers from.
const OUTAGE: Duration = Duration::from_secs(3);

/// A sink task, the last task of its flow, and the server it writes to.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    /// Starts the sink task with `context` on `rx`, with each write bounded to a few seconds.
    async fn start(&self, context: Arc<TaskContext>, rx: Receiver<Event>) -> JoinHandle<()>;

    /// An event the sink writes as a record carrying `payload`.
    fn event(&self, payload: &str) -> Event;

    /// The payloads of the records the server holds.
    async fn written(&self) -> Vec<String>;

    /// Takes the server down, as an outage would.
    async fn take_down(&self);

    /// Brings the server back.
    async fn bring_back(&self);
}

/// Every event is written once, and its completion reaches the source.
pub async fn writes_each_event_once(sink: &impl Sink) {
    let writing = Writing::start(sink).await;
    let written = ["a", "b", "c", "d", "e"];
    let mut completions = Vec::new();
    for payload in written {
        completions.push(writing.send(payload).await);
    }
    for completion in completions {
        assert_completed(completion, "the event").await;
    }
    writing.stop().await;

    assert_eq!(stored(sink).await, written);
}

/// An event sent while the server is down fails within `FAIL_TIMEOUT`; a late write may still land.
pub async fn fails_an_event_while_the_server_is_down(sink: &impl Sink) {
    let writing = Writing::start(sink).await;
    sink.take_down().await;
    let refused = writing.send("refused").await;
    let failed = wait_for_completion(Some(FAIL_TIMEOUT), refused).await;
    sink.bring_back().await;
    let accepted = writing.send("accepted").await;
    assert_completed(accepted, "the event after the server came back").await;
    writing.stop().await;

    match failed {
        Ok(()) => panic!("the sink completed an event its server never took"),
        Err(CompletionFailure::Timeout { .. }) => {
            panic!("the sink neither wrote nor failed the event within {FAIL_TIMEOUT:?}")
        }
        Err(_) => {}
    }
    let stored = stored(sink).await;
    assert!(
        stored
            .iter()
            .all(|payload| payload == "accepted" || payload == "refused")
            && stored.iter().any(|payload| payload == "accepted"),
        "the server holds {stored:?}"
    );
}

/// An event sent during a brief outage is written and completed once the server is back.
pub async fn writes_an_event_once_the_server_is_back(sink: &impl Sink) {
    let writing = Writing::start(sink).await;
    sink.take_down().await;
    let delayed = writing.send("delayed").await;
    tokio::time::sleep(OUTAGE).await;
    sink.bring_back().await;
    assert_completed(delayed, "the event sent during the outage").await;
    writing.stop().await;

    let stored = stored(sink).await;
    assert!(
        !stored.is_empty() && stored.iter().all(|payload| payload == "delayed"),
        "the server holds {stored:?}"
    );
}

/// A sink stopped while it writes returns at once, with an outcome for every event it took.
pub async fn stops_while_writing(sink: &impl Sink) {
    let writing = Writing::start(sink).await;
    let mut completions = Vec::new();
    for n in 0..20 {
        completions.push(writing.send(&format!("stopping-{n}")).await);
    }
    writing.stop().await;

    for completion in completions {
        let outcome = wait_for_completion(Some(STOP_TIMEOUT), completion).await;
        assert!(
            !matches!(outcome, Err(CompletionFailure::Timeout { .. })),
            "an event the stopped sink took got no outcome within {STOP_TIMEOUT:?}"
        );
    }
}

/// Defines one test per sink case, each with its own server from `$sink`.
#[macro_export]
macro_rules! sink_conformance {
    ($sink:expr) => {
        mod sink_conformance {
            use super::*;

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn writes_each_event_once() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::writes_each_event_once(&$sink).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn fails_an_event_while_the_server_is_down() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::fails_an_event_while_the_server_is_down(&$sink).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn writes_an_event_once_the_server_is_back() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::writes_an_event_once_the_server_is_back(&$sink).await;
            }

            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires Docker"]
            async fn stops_while_writing() {
                let _serial = $crate::conformance::serial().await;
                $crate::conformance::stops_while_writing(&$sink).await;
            }
        }
    };
}

/// The payloads `sink`'s server holds, without the probes.
async fn stored(sink: &impl Sink) -> Vec<String> {
    let mut stored: Vec<String> = sink
        .written()
        .await
        .into_iter()
        .filter(|payload| !payload.starts_with(PROBE))
        .collect();
    stored.sort();
    stored
}

/// Waits for `completion` and asserts the flow completed `what`.
async fn assert_completed(completion: CompletionRx, what: &str) {
    if let Err(failure) = wait_for_completion(Some(ARRIVAL_TIMEOUT), completion).await {
        panic!("the sink did not complete {what}: {failure}");
    }
}

/// Asserts the task behind `handle` returns within `STOP_TIMEOUT`.
async fn assert_returns(handle: JoinHandle<()>, what: &str) {
    let returned = tokio::time::timeout(STOP_TIMEOUT, handle).await;
    assert!(
        returned.is_ok(),
        "the {what} did not stop within {STOP_TIMEOUT:?}"
    );
}

/// A started sink and the channel the flow hands it events on.
struct Writing<'a, S: Sink> {
    sink: &'a S,
    tx: Sender<Event>,
    stop: CancellationToken,
    handle: JoinHandle<()>,
}

impl<'a, S: Sink> Writing<'a, S> {
    /// Starts the sink and waits until it has written a probe.
    async fn start(sink: &'a S) -> Self {
        let stop = CancellationToken::new();
        let (tx, rx) = mpsc::channel(32);
        let retry = RetryConfig {
            max_attempts: Some(6),
            initial_backoff: Duration::from_secs(1),
        };
        let handle = sink.start(context(&new_cache(), &stop, retry), rx).await;
        let writing = Self {
            sink,
            tx,
            stop,
            handle,
        };
        let probe = writing.send(&format!("{PROBE}0")).await;
        assert_completed(probe, "the probe").await;
        writing
    }

    /// Hands the sink an event carrying `payload`, returning its completion.
    async fn send(&self, payload: &str) -> CompletionRx {
        let (completion, completed) = new_completion_channel(1);
        let mut event = self.sink.event(payload);
        event.completion_tx = Some(completion);
        self.tx.send(event).await.expect("the sink takes events");
        completed
    }

    /// Stops the sink and asserts it returns at once.
    async fn stop(self) {
        self.stop.cancel();
        assert_returns(self.handle, "sink").await;
    }
}
