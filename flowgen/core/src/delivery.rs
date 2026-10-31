//! Delivery of source events to the flow, again until it completes them.

use crate::event::{new_completion_channel, CompletionRx, Event};
use crate::retry::RetryConfig;
use std::future::IntoFuture;
use std::num::NonZeroU32;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

/// How often an event the flow still holds is reported.
pub const STILL_HELD_REPORT_INTERVAL: Duration = Duration::from_secs(300);

/// Why the flow did not complete a delivered event.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum CompletionFailure {
    /// The flow took longer than `ack_timeout`.
    #[error("Flow did not complete the event within {timeout:?}")]
    Timeout { timeout: Duration },
    /// A leaf of the flow reported an error.
    #[error("Flow failed to complete the event: {source}")]
    Failed {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The flow dropped the event without reporting completion.
    #[error("Flow dropped the event without completing it")]
    Dropped,
}

/// How a delivered event ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivered {
    /// The flow completed the event.
    Completed,
    /// Every delivery failed and `max_deliver` ran out.
    Skipped,
    /// The task stopped first.
    Stopped,
}

/// How a source sends its events through the flow.
#[derive(Debug, Clone, Copy)]
pub struct Delivery<'a> {
    /// Whether the source has tasks after it; without them an event is
    /// complete once sent.
    pub downstream: bool,
    /// Leaves that must complete each event.
    pub leaf_count: usize,
    /// How long to wait for the flow to complete an event.
    pub ack_timeout: Option<Duration>,
    /// How many times an event is sent before it is skipped.
    pub max_deliver: Option<NonZeroU32>,
    /// Sets the delay before a failed event is sent again.
    pub retry: &'a RetryConfig,
    /// Stops the delivery.
    pub cancellation: &'a CancellationToken,
}

impl<'a> Delivery<'a> {
    /// Sends `event` with `send` until the flow completes it, `max_deliver`
    /// deliveries have failed, or the task stops.
    pub async fn deliver<F, S>(
        &self,
        event: &Event,
        mut send: F,
    ) -> Result<Delivered, crate::event::Error>
    where
        F: FnMut(Event) -> S,
        S: IntoFuture<Output = Result<(), crate::event::Error>>,
    {
        let mut deliveries: u32 = 0;
        loop {
            if self.cancellation.is_cancelled() {
                return Ok(Delivered::Stopped);
            }
            deliveries = deliveries.saturating_add(1);
            let (completion_state, completion_rx) = new_completion_channel(self.leaf_count);
            let mut attempt = event.clone();
            attempt.completion_tx = Some(completion_state);
            tokio::select! {
                biased;
                sent = send(attempt).into_future() => sent?,
                _ = self.cancellation.cancelled() => return Ok(Delivered::Stopped),
            }
            if !self.downstream {
                return Ok(Delivered::Completed);
            }

            let sent = tokio::time::Instant::now();
            let waiting = wait_for_completion(self.ack_timeout, completion_rx);
            tokio::pin!(waiting);
            let mut reports = tokio::time::interval_at(
                sent + STILL_HELD_REPORT_INTERVAL,
                STILL_HELD_REPORT_INTERVAL,
            );
            let completion = loop {
                tokio::select! {
                    biased;
                    completion = &mut waiting => break completion,
                    _ = self.cancellation.cancelled() => return Ok(Delivered::Stopped),
                    _ = reports.tick() => warn!(
                        event_id = ?event.id,
                        elapsed_secs = sent.elapsed().as_secs(),
                        "The flow still holds the event"
                    ),
                }
            };
            let reason = match completion {
                Ok(()) => return Ok(Delivered::Completed),
                Err(_) if self.cancellation.is_cancelled() => return Ok(Delivered::Stopped),
                Err(reason) => reason,
            };
            if self.max_deliver.is_some_and(|max| deliveries >= max.get()) {
                error!(event_id = ?event.id, deliveries, error = %reason, "Flow failed for event on every delivery, skipping it");
                return Ok(Delivered::Skipped);
            }
            let delay = self.retry.redelivery_delay(deliveries);
            warn!(event_id = ?event.id, deliveries, error = %reason, delay_ms = %delay.as_millis(), "Flow failed for event, delivering it again");
            tokio::select! {
                _ = self.cancellation.cancelled() => return Ok(Delivered::Stopped),
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    /// Delivers each event of `events` to `tx` as [`deliver`](Self::deliver)
    /// does, up to `window` at a time, and yields how each one ended. A read
    /// error of `events` is yielded as it is and a send error through
    /// `send_error`; either ends the stream without waiting for the events
    /// still in the flow.
    pub fn deliver_each<'b, St, E>(
        self,
        events: St,
        window: std::num::NonZeroUsize,
        tx: Option<&'b tokio::sync::mpsc::Sender<Event>>,
        send_error: fn(crate::event::Error) -> E,
    ) -> impl futures_util::Stream<Item = Result<Delivered, E>> + Send + 'b
    where
        'a: 'b,
        St: futures_util::Stream<Item = Result<Event, E>> + Send + 'b,
        E: Send + 'b,
    {
        use crate::event::EventExt;
        use futures_util::TryStreamExt;
        events
            .map_ok(move |event| async move {
                self.deliver(&event, |attempt| attempt.send_with_logging(tx))
                    .await
                    .map_err(send_error)
            })
            .try_buffer_unordered(window.get())
    }
}

/// Waits for every leaf of the flow to complete the event.
pub async fn wait_for_completion(
    ack_timeout: Option<Duration>,
    completion_rx: CompletionRx,
) -> Result<(), CompletionFailure> {
    let completion = match ack_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, completion_rx).await {
            Ok(completion) => completion,
            Err(_) => return Err(CompletionFailure::Timeout { timeout }),
        },
        None => completion_rx.await,
    };
    match completion {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(source)) => Err(CompletionFailure::Failed { source }),
        Err(_) => Err(CompletionFailure::Dropped),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventBuilder, EventData, EventExt};
    use tokio::sync::mpsc;

    fn event() -> Event {
        EventBuilder::new()
            .subject("s".to_string())
            .data(EventData::Json(serde_json::Value::Null))
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap()
    }

    /// Answers every delivered event with `fail` and counts the deliveries.
    fn flow(fail: bool) -> (mpsc::Sender<Event>, tokio::task::JoinHandle<usize>) {
        let (tx, mut rx) = mpsc::channel::<Event>(4);
        let answers = tokio::spawn(async move {
            let mut deliveries = 0;
            while let Some(event) = rx.recv().await {
                deliveries += 1;
                let completion = event.completion_tx.unwrap();
                match fail {
                    true => completion.signal_completion_with_error("Write failed".to_string()),
                    false => completion.signal_completion(None),
                }
            }
            deliveries
        });
        (tx, answers)
    }

    fn delivery<'a>(
        retry: &'a RetryConfig,
        cancellation: &'a CancellationToken,
        max_deliver: Option<u32>,
    ) -> Delivery<'a> {
        Delivery {
            downstream: true,
            leaf_count: 1,
            ack_timeout: None,
            max_deliver: max_deliver.and_then(NonZeroU32::new),
            retry,
            cancellation,
        }
    }

    #[tokio::test]
    async fn a_completed_event_is_delivered_once() {
        let (retry, cancellation) = (RetryConfig::default(), CancellationToken::new());
        let (tx, answers) = flow(false);

        let delivered = delivery(&retry, &cancellation, None)
            .deliver(&event(), |e| e.send_with_logging(Some(&tx)))
            .await
            .unwrap();
        drop(tx);

        assert_eq!(delivered, Delivered::Completed);
        assert_eq!(answers.await.unwrap(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_event_is_skipped_after_max_deliver() {
        let (retry, cancellation) = (RetryConfig::default(), CancellationToken::new());
        let (tx, answers) = flow(true);

        let delivered = delivery(&retry, &cancellation, Some(3))
            .deliver(&event(), |e| e.send_with_logging(Some(&tx)))
            .await
            .unwrap();
        drop(tx);

        assert_eq!(delivered, Delivered::Skipped);
        assert_eq!(answers.await.unwrap(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stop_ends_redelivery() {
        let (retry, cancellation) = (RetryConfig::default(), CancellationToken::new());
        let (tx, _answers) = flow(true);
        let stop = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            stop.cancel();
        });

        let delivered = delivery(&retry, &cancellation, None)
            .deliver(&event(), |e| e.send_with_logging(Some(&tx)))
            .await
            .unwrap();

        assert_eq!(delivered, Delivered::Stopped);
    }

    #[tokio::test(start_paused = true)]
    async fn an_event_the_flow_holds_for_long_still_completes() {
        let (retry, cancellation) = (RetryConfig::default(), CancellationToken::new());
        let (tx, mut rx) = mpsc::channel::<Event>(1);
        let held_for = STILL_HELD_REPORT_INTERVAL * 2 + Duration::from_secs(1);
        let flow = tokio::spawn(async move {
            let event = rx.recv().await.unwrap();
            tokio::time::sleep(held_for).await;
            event.completion_tx.unwrap().signal_completion(None);
        });
        let started = tokio::time::Instant::now();

        let delivered = delivery(&retry, &cancellation, None)
            .deliver(&event(), |e| e.send_with_logging(Some(&tx)))
            .await
            .unwrap();
        flow.await.unwrap();

        assert_eq!(delivered, Delivered::Completed);
        assert!(started.elapsed() >= held_for);
    }

    /// Delivers `events` two at a time and collects how each ended, up to
    /// the first error.
    async fn deliver_each(
        events: Vec<Result<Event, String>>,
    ) -> (Result<Vec<Delivered>, String>, usize) {
        use futures_util::TryStreamExt;
        let (retry, cancellation) = (RetryConfig::default(), CancellationToken::new());
        let (tx, answers) = flow(false);
        let delivered = delivery(&retry, &cancellation, None)
            .deliver_each(
                futures_util::stream::iter(events),
                std::num::NonZeroUsize::MIN.saturating_add(1),
                Some(&tx),
                |source| source.to_string(),
            )
            .try_collect()
            .await;
        drop(tx);
        (delivered, answers.await.unwrap())
    }

    #[tokio::test]
    async fn each_event_is_delivered() {
        let (delivered, deliveries) =
            deliver_each(vec![Ok(event()), Ok(event()), Ok(event())]).await;

        assert_eq!(delivered, Ok(vec![Delivered::Completed; 3]));
        assert_eq!(deliveries, 3);
    }

    #[tokio::test]
    async fn a_read_error_ends_the_delivery() {
        let (delivered, _) = deliver_each(vec![
            Ok(event()),
            Err("Read failed".to_string()),
            Ok(event()),
        ])
        .await;

        assert_eq!(delivered, Err("Read failed".to_string()));
    }

    #[tokio::test]
    async fn wait_for_completion_succeeds() {
        let (state, rx) = new_completion_channel(1);
        state.signal_completion(None);
        assert!(wait_for_completion(None, rx).await.is_ok());
    }

    #[tokio::test]
    async fn wait_for_completion_reports_a_flow_error() {
        let (state, rx) = new_completion_channel(1);
        state.signal_completion_with_error("Write failed".to_string());
        assert!(matches!(
            wait_for_completion(None, rx).await,
            Err(CompletionFailure::Failed { source }) if source.to_string() == "Write failed"
        ));
    }

    #[tokio::test]
    async fn wait_for_completion_times_out() {
        let (_state, rx) = new_completion_channel(1);
        assert!(matches!(
            wait_for_completion(Some(Duration::from_millis(10)), rx).await,
            Err(CompletionFailure::Timeout { .. })
        ));
    }

    #[tokio::test]
    async fn wait_for_completion_reports_a_dropped_event() {
        let (state, rx) = new_completion_channel(1);
        drop(state);
        assert!(matches!(
            wait_for_completion(None, rx).await,
            Err(CompletionFailure::Dropped)
        ));
    }
}
