//! Per-flow metrics (counters, last-seen timestamps, derived status)
//! sourced from the standard `tracing` events.
//!
//! Counters/status are a separate domain from log/event history — that
//! lives behind [`crate::telemetry::query::LogsStore`]. This module owns
//! the [`MetricsStore`] trait so the admin API and the tracing layer stay
//! backend-agnostic: [`OtlpMetricsStore`] is today's only implementation,
//! and whether its numbers live purely in the local atomic counters or
//! also get pushed to a vendor is OTLP-export config on that one impl,
//! not a second Rust type — the OTel spec standardizes metrics push, so
//! one implementation covers every vendor.
//!
//! A custom [`tracing_subscriber::Layer`] (`flow_activity_layer`) captures
//! every `info!` / `warn!` / `error!` emitted inside a `task.handle` span,
//! walks the parent scope to recover the owning flow/task names, and:
//!
//! 1. Bumps atomic counters + last-seen timestamps on the shared
//!    [`MetricsStore`] behind the admin API.
//! 2. Records the same signal into OpenTelemetry counters so downstream
//!    dashboards see identical numbers.
//!
//! Log body + attributes for the admin UI come from the native
//! `tracing_subscriber::fmt::json()` writer through
//! `flowgen_core::telemetry::query::MemoryLogsStoreWriter` — no re-emit here.
//!
//! The layer lives in `activity_layer.rs`; this module owns the plain-data
//! primitives so it can be depended on without pulling the tracing layer
//! in.

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use opentelemetry::metrics::Counter;
use opentelemetry::KeyValue;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

/// Reason a flow's last activity happened. Mirrors tracing levels the
/// runtime uses so callers can render icons/badges without ad-hoc mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ActivityLevel {
    /// A `task.handle` scope produced an `info!` — normal progress.
    Info,
    /// A `warn!` fired — task still running, but something is off.
    Warning,
    /// An `error!` fired — a handle attempt failed (retry loop may still
    /// be trying); repeated errors bump `errors_total`.
    Error,
}

impl ActivityLevel {
    fn as_str(&self) -> &'static str {
        match self {
            ActivityLevel::Info => "info",
            ActivityLevel::Warning => "warning",
            ActivityLevel::Error => "error",
        }
    }
}

/// Derived status shown per flow on the admin UI. Wins by recency: an
/// error after an info is Error until the next info, and so on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FlowStatus {
    /// No events observed yet.
    Idle,
    /// Last observed event was an `info!` — flow is healthy.
    Ok,
    /// Last observed event was a `warn!`.
    Warn,
    /// Last observed event was an `error!`.
    Error,
}

/// Snapshot of a flow's counters plus the last-seen timestamps in unix
/// milliseconds. `0` means "never seen".
#[derive(Debug, Clone, Serialize)]
pub struct FlowMetricsSnapshot {
    /// Flow name (identifier from `flow.name`).
    pub flow: String,
    /// Sum of successful task.handle info events since process start.
    pub events_total: u64,
    /// Sum of warn events observed inside task.handle scopes.
    pub warnings_total: u64,
    /// Sum of error events observed inside task.handle scopes.
    pub errors_total: u64,
    /// Unix milliseconds of the most recent info event, or `None` when
    /// the flow has never emitted one since the process started.
    pub last_event_at_ms: Option<u64>,
    /// Unix milliseconds of the most recent warn event.
    pub last_warning_at_ms: Option<u64>,
    /// Unix milliseconds of the most recent error event.
    pub last_error_at_ms: Option<u64>,
    /// Derived status (`FlowStatus`) computed from the three timestamps.
    pub status: FlowStatus,
}

impl FlowMetricsSnapshot {
    /// Returns a zeroed snapshot for a flow that has not emitted any
    /// tracked events yet.
    pub fn empty(flow: &str) -> Self {
        Self {
            flow: flow.to_string(),
            events_total: 0,
            warnings_total: 0,
            errors_total: 0,
            last_event_at_ms: None,
            last_warning_at_ms: None,
            last_error_at_ms: None,
            status: FlowStatus::Idle,
        }
    }
}

/// Atomic counter block used per flow. Lives behind an Arc so the tracing
/// layer, the SSE broadcaster and the admin API all point at the same
/// numbers without a global RwLock on every event.
#[derive(Debug, Default)]
pub struct FlowMetrics {
    events_total: AtomicU64,
    warnings_total: AtomicU64,
    errors_total: AtomicU64,
    last_event_at_ms: AtomicU64,
    last_warning_at_ms: AtomicU64,
    last_error_at_ms: AtomicU64,
}

impl FlowMetrics {
    fn record(&self, level: ActivityLevel, ts_ms: u64) {
        match level {
            ActivityLevel::Info => {
                self.events_total.fetch_add(1, Ordering::Relaxed);
                self.last_event_at_ms.store(ts_ms, Ordering::Relaxed);
            }
            ActivityLevel::Warning => {
                self.warnings_total.fetch_add(1, Ordering::Relaxed);
                self.last_warning_at_ms.store(ts_ms, Ordering::Relaxed);
            }
            ActivityLevel::Error => {
                self.errors_total.fetch_add(1, Ordering::Relaxed);
                self.last_error_at_ms.store(ts_ms, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self, flow: String) -> FlowMetricsSnapshot {
        let last_event = read_ts(&self.last_event_at_ms);
        let last_warning = read_ts(&self.last_warning_at_ms);
        let last_error = read_ts(&self.last_error_at_ms);
        FlowMetricsSnapshot {
            flow,
            events_total: self.events_total.load(Ordering::Relaxed),
            warnings_total: self.warnings_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            last_event_at_ms: last_event,
            last_warning_at_ms: last_warning,
            last_error_at_ms: last_error,
            status: derive_status(last_event, last_warning, last_error),
        }
    }
}

fn read_ts(a: &AtomicU64) -> Option<u64> {
    match a.load(Ordering::Relaxed) {
        0 => None,
        ts => Some(ts),
    }
}

// Status "wins by recency": whichever level's timestamp is highest is
// the current status. Ties on warn/error prefer the worse level so a
// misbehaving flow doesn't look Warning when it's actually Error.
fn derive_status(info: Option<u64>, warn: Option<u64>, err: Option<u64>) -> FlowStatus {
    let latest = [
        (info, FlowStatus::Ok),
        (warn, FlowStatus::Warn),
        (err, FlowStatus::Error),
    ];
    let mut winner: Option<(u64, FlowStatus)> = None;
    for (ts, status) in latest {
        match (ts, winner) {
            (Some(t), Some((best, _))) if t > best => winner = Some((t, status)),
            (Some(t), Some((best, prev))) if t == best => {
                let level = match (status, prev) {
                    (FlowStatus::Error, _) | (_, FlowStatus::Error) => FlowStatus::Error,
                    (FlowStatus::Warn, _) | (_, FlowStatus::Warn) => FlowStatus::Warn,
                    _ => status,
                };
                winner = Some((t, level));
            }
            (Some(t), None) => winner = Some((t, status)),
            _ => {}
        }
    }
    match winner {
        Some((_, s)) => s,
        None => FlowStatus::Idle,
    }
}

/// One event recorded by the tracing layer, before it's stamped with
/// flow metrics and published. Grouped into a struct so `record` stays
/// under clippy's arg-count limit as fields are added.
#[derive(Debug)]
pub struct RecordedEvent {
    pub task: Option<String>,
    pub task_type: Option<String>,
    pub level: ActivityLevel,
    pub ts_ms: u64,
    pub message: String,
    pub duration_ms: Option<u64>,
    pub event_id: Option<String>,
}

/// Errors returned by [`MetricsStore`] implementations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum MetricsStoreError {
    #[error("Metrics store backend error: {source}")]
    Backend {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Type alias for metrics store errors.
pub type Error = MetricsStoreError;

/// Backend-agnostic per-flow metrics facade: counters, last-seen
/// timestamps, and derived status. Separate domain from log/event
/// history ([`crate::telemetry::query::LogsStore`]) — a flow's activity
/// panel reads its event list from `LogsStore` and its metrics/status
/// from this trait.
#[async_trait]
pub trait MetricsStore: Debug + Send + Sync {
    /// Records one classified event for `flow`, bumping its counters
    /// and last-seen timestamp for the event's level. Synchronous:
    /// called from the tracing `Layer::on_event` hot path, which must
    /// not block or touch the async runtime.
    fn record(&self, flow: &str, event: RecordedEvent);

    /// Snapshot every flow's current counters.
    async fn snapshot_all(&self) -> Result<Vec<FlowMetricsSnapshot>, Error>;

    /// Snapshot a single flow's counters, or `None` when no event has
    /// ever been recorded for it.
    async fn snapshot(&self, flow: &str) -> Result<Option<FlowMetricsSnapshot>, Error>;

    /// Subscribes to a flow's updated snapshot every time `record` bumps
    /// its counters. Mirrors `LogsStore::tail`'s push-on-write shape so
    /// SSE consumers don't need to poll.
    async fn watch_all(&self) -> Result<BoxStream<'static, FlowMetricsSnapshot>, Error>;
}

/// The one [`MetricsStore`] implementation: local atomic counters plus
/// OpenTelemetry counter emission (`flowgen.flow.events/warnings/errors`).
/// OTLP is a push standard, so this single implementation covers every
/// vendor behind the configured OTel exporter — an in-memory-only setup
/// and a vendor-backed one differ only in exporter config, not in Rust
/// type. `snapshot`/`snapshot_all` read the local counters; a
/// vendor-backed deployment that wants the admin UI's numbers to reflect
/// the vendor's own view would read through the vendor's query API
/// instead (not yet implemented — no second vendor read path exists).
#[derive(Debug)]
pub struct OtlpMetricsStore {
    inner: RwLock<HashMap<String, Arc<FlowMetrics>>>,
    counters: OnceLock<Counters>,
    tx: broadcast::Sender<FlowMetricsSnapshot>,
}

/// Builder for [`OtlpMetricsStore`]. Follows the same shape as
/// `flowgen_nats::cache::CacheBuilder` so consumers stay consistent
/// across the workspace.
#[derive(Default)]
pub struct OtlpMetricsStoreBuilder {}

/// Broadcast channel capacity for `watch_all` subscribers. Small on
/// purpose: subscribers care about the latest snapshot per flow, not a
/// backlog — a slow subscriber sees dropped frames rather than the
/// writer backing up.
const WATCH_CHANNEL_CAPACITY: usize = 256;

impl OtlpMetricsStoreBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> Arc<OtlpMetricsStore> {
        let (tx, _rx) = broadcast::channel(WATCH_CHANNEL_CAPACITY);
        Arc::new(OtlpMetricsStore {
            inner: RwLock::new(HashMap::new()),
            counters: OnceLock::new(),
            tx,
        })
    }
}

#[derive(Debug)]
struct Counters {
    events: Counter<u64>,
    warnings: Counter<u64>,
    errors: Counter<u64>,
}

impl OtlpMetricsStore {
    pub fn builder() -> OtlpMetricsStoreBuilder {
        OtlpMetricsStoreBuilder::new()
    }

    fn slot(&self, flow: &str) -> Arc<FlowMetrics> {
        if let Ok(guard) = self.inner.read() {
            if let Some(m) = guard.get(flow) {
                return Arc::clone(m);
            }
        }
        // First time we see this flow — create + insert atomically.
        let mut guard = match self.inner.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        Arc::clone(
            guard
                .entry(flow.to_string())
                .or_insert_with(|| Arc::new(FlowMetrics::default())),
        )
    }

    fn emit_otel(&self, flow: &str, task: Option<&str>, level: ActivityLevel) {
        let counters = self.counters.get_or_init(|| {
            let meter = opentelemetry::global::meter("flowgen.flow_activity");
            Counters {
                events: meter
                    .u64_counter("flowgen.flow.events")
                    .with_description("Successful task.handle events per flow")
                    .build(),
                warnings: meter
                    .u64_counter("flowgen.flow.warnings")
                    .with_description("Warn-level events per flow")
                    .build(),
                errors: meter
                    .u64_counter("flowgen.flow.errors")
                    .with_description("Error-level events per flow")
                    .build(),
            }
        });
        let mut attrs = vec![KeyValue::new("flow", flow.to_string())];
        if let Some(task) = task {
            attrs.push(KeyValue::new("task", task.to_string()));
        }
        attrs.push(KeyValue::new("level", level.as_str()));
        match level {
            ActivityLevel::Info => counters.events.add(1, &attrs),
            ActivityLevel::Warning => counters.warnings.add(1, &attrs),
            ActivityLevel::Error => counters.errors.add(1, &attrs),
        }
    }
}

#[async_trait]
impl MetricsStore for OtlpMetricsStore {
    /// Bumps local counters synchronously and updates the OTel metrics
    /// counter. The tracing event itself already flows to stdout / the
    /// memory logs writer through the `fmt::json` layer. Publishes the
    /// flow's updated snapshot to `watch_all` subscribers.
    fn record(&self, flow: &str, event: RecordedEvent) {
        let metrics = self.slot(flow);
        metrics.record(event.level, event.ts_ms);
        self.emit_otel(flow, event.task.as_deref(), event.level);
        // No subscribers is the common case (no SSE client connected) and
        // not an error — nothing to do with the send result either way.
        match self.tx.send(metrics.snapshot(flow.to_string())) {
            Ok(_) | Err(broadcast::error::SendError(_)) => {}
        }
    }

    /// Snapshot every flow's current counters. Used by the REST API and
    /// as the initial payload SSE consumers receive on connect.
    async fn snapshot_all(&self) -> Result<Vec<FlowMetricsSnapshot>, Error> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        Ok(guard
            .iter()
            .map(|(flow, metrics)| metrics.snapshot(flow.clone()))
            .collect())
    }

    /// Snapshot a single flow, returning `None` when no event has ever
    /// been recorded for it (so the API can distinguish "unknown" from
    /// "known but idle").
    async fn snapshot(&self, flow: &str) -> Result<Option<FlowMetricsSnapshot>, Error> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        Ok(guard.get(flow).map(|m| m.snapshot(flow.to_string())))
    }

    /// Subscribes to the broadcast channel `record` publishes to. Lagged
    /// subscribers (channel full) drop frames rather than block the
    /// writer, same trade-off as `LogsStore::tail`.
    async fn watch_all(&self) -> Result<BoxStream<'static, FlowMetricsSnapshot>, Error> {
        let rx = self.tx.subscribe();
        let stream = BroadcastStream::new(rx)
            .filter_map(|res| async move { res.ok() })
            .boxed();
        Ok(stream)
    }
}

/// Convenience: unix-epoch milliseconds for `SystemTime::now()`.
pub fn now_ms() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_millis() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_prefers_latest_timestamp() {
        assert_eq!(derive_status(None, None, None), FlowStatus::Idle);
        assert_eq!(derive_status(Some(10), None, None), FlowStatus::Ok);
        assert_eq!(derive_status(Some(10), Some(20), None), FlowStatus::Warn);
        assert_eq!(derive_status(Some(30), Some(20), Some(10)), FlowStatus::Ok);
        assert_eq!(
            derive_status(Some(10), Some(20), Some(30)),
            FlowStatus::Error
        );
    }

    #[tokio::test]
    async fn record_updates_counters_across_levels() {
        let store = OtlpMetricsStore::builder().build();
        store.record(
            "f",
            RecordedEvent {
                task: Some("t".into()),
                task_type: Some("script".into()),
                level: ActivityLevel::Info,
                ts_ms: 100,
                message: "handled".into(),
                duration_ms: None,
                event_id: None,
            },
        );
        store.record(
            "f",
            RecordedEvent {
                task: Some("t".into()),
                task_type: Some("script".into()),
                level: ActivityLevel::Error,
                ts_ms: 200,
                message: "boom".into(),
                duration_ms: None,
                event_id: None,
            },
        );

        let snapshot = store
            .snapshot("f")
            .await
            .unwrap()
            .expect("flow must be registered");
        assert_eq!(snapshot.events_total, 1);
        assert_eq!(snapshot.errors_total, 1);
        assert_eq!(snapshot.last_event_at_ms, Some(100));
        assert_eq!(snapshot.last_error_at_ms, Some(200));
        assert_eq!(snapshot.status, FlowStatus::Error);
    }

    #[tokio::test]
    async fn watch_all_pushes_updated_snapshot_on_record() {
        let store = OtlpMetricsStore::builder().build();
        let mut watch = store.watch_all().await.unwrap();

        store.record(
            "f",
            RecordedEvent {
                task: None,
                task_type: None,
                level: ActivityLevel::Info,
                ts_ms: 100,
                message: "handled".into(),
                duration_ms: None,
                event_id: None,
            },
        );

        let pushed = watch.next().await.expect("watch_all must push a frame");
        assert_eq!(pushed.flow, "f");
        assert_eq!(pushed.events_total, 1);
        assert_eq!(pushed.status, FlowStatus::Ok);
    }
}
