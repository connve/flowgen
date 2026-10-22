//! Base trait for all task execution implementations.
//!
//! Defines the common interface that all flowgen task types must implement
//! to participate in the workflow execution pipeline.

use crate::event::Event;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Trait for executing workflow tasks asynchronously.
///
/// All task implementations (subscribers, publishers, processors) must implement
/// this trait to provide a standardized execution interface for the flowgen runtime.
#[async_trait::async_trait]
pub trait Runner {
    /// Error type for task execution failures.
    type Error;

    /// Event handler type for processing events.
    type EventHandler: Send;

    /// Initializes the task by setting up connections, authentication, and other resources.
    ///
    /// This method performs all setup operations that can fail, allowing errors to be
    /// logged with proper span context before the task enters its main event loop.
    ///
    /// # Returns
    /// The initialized event handler or an error if setup fails
    async fn init(&self) -> Result<Self::EventHandler, Self::Error>;

    /// Executes the task until completion or error.
    ///
    /// # Returns
    /// Success or an error if the task execution fails
    async fn run(self) -> Result<(), Self::Error>
    where
        Self: Sized;
}

/// What a processor does with each event.
#[async_trait::async_trait]
pub trait Handler: Send + Sync + 'static {
    type Error: std::fmt::Display + Send;

    async fn handle(&self, event: Event) -> Result<(), Self::Error>;

    /// Where to forward an event that failed all attempts.
    fn sender(&self) -> Option<&Sender<Event>>;

    /// Whether retrying might help.
    fn is_retryable(&self, _error: &Self::Error) -> bool {
        true
    }

    /// The error message sent along with a failed event.
    fn failure_message(&self, _event: &Event, error: &Self::Error) -> String {
        error.to_string()
    }
}

/// Events a task handles at once by default: the number of CPUs available to
/// the process for a CPU-intensive task, 64 for one that mostly waits on an
/// external service.
pub fn max_in_flight(cpu_intensive: bool) -> NonZeroUsize {
    match (cpu_intensive, std::thread::available_parallelism()) {
        (true, Ok(cpus)) => cpus,
        (true, Err(_)) => NonZeroUsize::MIN,
        (false, _) => NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN),
    }
}

/// Runs `handler` on events from `rx`, at most `max_in_flight` at a time
/// (the task's default when unset), until the channel closes or the task is
/// cancelled.
///
/// It takes the next event only when a slot frees up, so with a slow handler
/// events stay in the channel and the task upstream waits. On cancellation it
/// closes the channel, so upstream sends fail instead of hanging, and lets the
/// events it already took finish without retrying them. An event whose
/// upstream task failed is passed on unhandled, unless the task handles failed events.
pub async fn process_events<H: Handler>(
    mut rx: Receiver<Event>,
    handler: Arc<H>,
    retry: &crate::retry::RetryConfig,
    max_in_flight: Option<NonZeroUsize>,
    task_context: &crate::task::context::TaskContext,
) {
    let cancellation = &task_context.cancellation_token;
    let max_in_flight = match max_in_flight {
        Some(max_in_flight) => max_in_flight,
        None => task_context.max_in_flight,
    };
    let slots = Arc::new(tokio::sync::Semaphore::new(max_in_flight.get()));
    let mut running = tokio::task::JoinSet::new();
    loop {
        let slot = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            slot = Arc::clone(&slots).acquire_owned() => match slot {
                Ok(slot) => slot,
                Err(_) => break,
            },
        };
        let event = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            event = rx.recv() => match event {
                Some(event) => event,
                None => break,
            },
        };
        let handler = Arc::clone(&handler);
        let delays = retry.strategy();
        let cancellation = cancellation.clone();
        let handles_failed = task_context.handles_failed;
        running.spawn(
            async move {
                let _slot = slot;
                match (event.error.clone(), handles_failed) {
                    (Some(error), false) => event.forward_failure(handler.sender(), error).await,
                    _ => handle_with_retry(handler.as_ref(), event, delays, &cancellation).await,
                }
            }
            .instrument(tracing::Span::current()),
        );
        while let Some(finished) = running.try_join_next() {
            log_panic(finished);
        }
    }
    rx.close();
    while let Some(finished) = running.join_next().await {
        log_panic(finished);
    }
}

fn log_panic(finished: Result<(), tokio::task::JoinError>) {
    if let Err(error) = finished {
        error!(error = %error, "Event handler panicked");
    }
}

/// Handles `event`, retrying after each delay while the error is retryable,
/// and forwards it as failed once retries run out. Once `cancellation` fires
/// it stops retrying, so a side effect done before a failed send to a stopped
/// task is not repeated.
async fn handle_with_retry<H: Handler>(
    handler: &H,
    event: Event,
    mut delays: Box<dyn Iterator<Item = std::time::Duration> + Send>,
    cancellation: &tokio_util::sync::CancellationToken,
) {
    loop {
        let error = match handler.handle(event.clone()).await {
            Ok(()) => return,
            Err(error) => error,
        };
        error!(error = %error, "Failed to handle event");
        let delay = match (handler.is_retryable(&error), cancellation.is_cancelled()) {
            (true, false) => delays.next(),
            _ => None,
        };
        let retried = match delay {
            Some(delay) => tokio::select! {
                biased;
                _ = cancellation.cancelled() => false,
                _ = tokio::time::sleep(delay) => true,
            },
            None => false,
        };
        if !retried {
            error!(error = %error, "Event failed after all retry attempts");
            let message = handler.failure_message(&event, &error);
            event.forward_failure(handler.sender(), message).await;
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventBuilder, EventData};
    use std::sync::atomic::{AtomicUsize, Ordering};

    const WAIT: std::time::Duration = std::time::Duration::from_secs(5);

    struct Gated {
        started: AtomicUsize,
        open: tokio::sync::Semaphore,
        fails: bool,
        retryable: bool,
        tx: Option<Sender<Event>>,
    }

    #[async_trait::async_trait]
    impl Handler for Gated {
        type Error = String;

        async fn handle(&self, _event: Event) -> Result<(), String> {
            self.started.fetch_add(1, Ordering::SeqCst);
            match self.open.acquire().await {
                Ok(permit) => permit.forget(),
                Err(_) => return Err("closed".to_string()),
            }
            match self.fails {
                true => Err("bad input".to_string()),
                false => Ok(()),
            }
        }

        fn sender(&self) -> Option<&Sender<Event>> {
            self.tx.as_ref()
        }

        fn is_retryable(&self, _error: &String) -> bool {
            self.retryable
        }
    }

    fn gated(open: usize, fails: bool, retryable: bool, tx: Option<Sender<Event>>) -> Arc<Gated> {
        Arc::new(Gated {
            started: AtomicUsize::new(0),
            open: tokio::sync::Semaphore::new(open),
            fails,
            retryable,
            tx,
        })
    }

    fn event() -> Event {
        EventBuilder::new()
            .data(EventData::Json(serde_json::Value::Null))
            .subject("a".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap()
    }

    async fn started(handler: &Gated, count: usize) {
        tokio::time::timeout(WAIT, async {
            while handler.started.load(Ordering::SeqCst) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("handler started");
    }

    fn run(
        rx: Receiver<Event>,
        handler: &Arc<Gated>,
        retry: crate::retry::RetryConfig,
        max_in_flight: usize,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        run_handling_failed(rx, handler, retry, max_in_flight, cancellation, false)
    }

    fn run_handling_failed(
        rx: Receiver<Event>,
        handler: &Arc<Gated>,
        retry: crate::retry::RetryConfig,
        max_in_flight: usize,
        cancellation: &tokio_util::sync::CancellationToken,
        handles_failed: bool,
    ) -> tokio::task::JoinHandle<()> {
        let handler = Arc::clone(handler);
        let mut task_context = crate::task::context::TaskContextBuilder::new()
            .flow_name("a".to_string())
            .task_manager(Arc::new(
                crate::task::manager::TaskManagerBuilder::new()
                    .build()
                    .unwrap(),
            ))
            .cache(Arc::new(crate::cache::memory::MemoryCache::new()))
            .cancellation_token(cancellation.clone())
            .build()
            .unwrap();
        task_context.handles_failed = handles_failed;
        tokio::spawn(async move {
            process_events(
                rx,
                handler,
                &retry,
                NonZeroUsize::new(max_in_flight),
                &task_context,
            )
            .await
        })
    }

    #[test]
    fn a_cpu_intensive_task_handles_as_many_events_as_there_are_cpus() {
        assert_eq!(
            max_in_flight(true),
            std::thread::available_parallelism().unwrap()
        );
        assert_eq!(max_in_flight(false).get(), 64);
    }

    #[tokio::test]
    async fn a_busy_task_takes_no_more_events_and_the_sender_waits() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let handler = gated(0, false, true, None);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let running = run(rx, &handler, Default::default(), 2, &cancellation);

        for _ in 0..3 {
            tx.send(event()).await.unwrap();
        }
        started(&handler, 2).await;
        let full = tx.try_send(event());
        handler.open.add_permits(3);
        started(&handler, 3).await;
        drop(tx);
        running.await.unwrap();

        assert!(matches!(
            full,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        ));
    }

    #[tokio::test]
    async fn cancelling_closes_the_channel_and_lets_running_events_finish() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let handler = gated(0, false, true, None);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let running = run(rx, &handler, Default::default(), 1, &cancellation);
        tx.send(event()).await.unwrap();
        started(&handler, 1).await;

        cancellation.cancel();
        tokio::time::timeout(WAIT, tx.closed())
            .await
            .expect("the channel closes");
        let send = tx.send(event()).await;
        handler.open.add_permits(1);
        tokio::time::timeout(WAIT, running)
            .await
            .expect("finishes once the running event does")
            .unwrap();

        assert!(send.is_err());
        assert_eq!(handler.started.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_error_that_cannot_be_retried_forwards_the_event_after_one_attempt() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (failed_tx, mut failed_rx) = tokio::sync::mpsc::channel(1);
        let handler = gated(1, true, false, Some(failed_tx));
        let cancellation = tokio_util::sync::CancellationToken::new();
        tx.send(event()).await.unwrap();
        drop(tx);

        run(rx, &handler, Default::default(), 1, &cancellation)
            .await
            .unwrap();

        assert_eq!(handler.started.load(Ordering::SeqCst), 1);
        assert_eq!(
            failed_rx.recv().await.unwrap().error.as_deref(),
            Some("bad input")
        );
    }

    fn failed_upstream() -> Event {
        let mut event = event();
        event.error = Some("upstream failed".to_string());
        event
    }

    #[tokio::test]
    async fn an_event_whose_upstream_failed_is_passed_on_unhandled() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(1);
        let handler = gated(1, false, false, Some(out_tx));
        let cancellation = tokio_util::sync::CancellationToken::new();
        tx.send(failed_upstream()).await.unwrap();
        drop(tx);

        run(rx, &handler, Default::default(), 1, &cancellation)
            .await
            .unwrap();

        assert_eq!(handler.started.load(Ordering::SeqCst), 0);
        assert_eq!(
            out_rx.recv().await.unwrap().error.as_deref(),
            Some("upstream failed")
        );
    }

    #[tokio::test]
    async fn a_failed_event_at_the_last_task_reports_the_error_to_the_source() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let handler = gated(1, false, false, None);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let (completion_tx, completion_rx) = crate::event::new_completion_channel(1);
        let mut event = failed_upstream();
        event.completion_tx = Some(completion_tx);
        tx.send(event).await.unwrap();
        drop(tx);

        run(rx, &handler, Default::default(), 1, &cancellation)
            .await
            .unwrap();

        let reported = completion_rx.await.unwrap();
        assert_eq!(
            reported.map_err(|error| error.to_string()),
            Err("upstream failed".to_string())
        );
        assert_eq!(handler.started.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_task_that_handles_failed_events_runs_on_them() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let handler = gated(1, false, false, None);
        let cancellation = tokio_util::sync::CancellationToken::new();
        tx.send(failed_upstream()).await.unwrap();
        drop(tx);

        run_handling_failed(rx, &handler, Default::default(), 1, &cancellation, true)
            .await
            .unwrap();

        assert_eq!(handler.started.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failed_event_is_not_retried_once_the_task_is_cancelled() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (failed_tx, mut failed_rx) = tokio::sync::mpsc::channel(1);
        let handler = gated(1, true, true, Some(failed_tx));
        let cancellation = tokio_util::sync::CancellationToken::new();
        let retry = crate::retry::RetryConfig {
            initial_backoff: std::time::Duration::from_secs(3600),
            ..Default::default()
        };
        let running = run(rx, &handler, retry, 1, &cancellation);
        tx.send(event()).await.unwrap();
        started(&handler, 1).await;

        cancellation.cancel();
        let failed = tokio::time::timeout(WAIT, failed_rx.recv()).await;
        tokio::time::timeout(WAIT, running)
            .await
            .expect("stops without waiting out the backoff")
            .unwrap();

        assert_eq!(
            failed.expect("forwarded").unwrap().error.as_deref(),
            Some("bad input")
        );
        assert_eq!(handler.started.load(Ordering::SeqCst), 1);
    }
}
