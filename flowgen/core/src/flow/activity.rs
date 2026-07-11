//! Per-flow activity tracking sourced from the standard `tracing` events.
//!
//! A custom [`tracing_subscriber::Layer`] (`flow_activity_layer`) captures
//! every `info!` / `warn!` / `error!` emitted inside a `task.handle` span,
//! walks the parent scope to recover the owning flow/task names, and:
//!
//! 1. Bumps atomic counters + last-seen timestamps on a [`FlowRegistry`]
//!    shared with the admin API.
//! 2. Fans the same event out onto a broadcast channel that feeds the SSE
//!    endpoint the UI subscribes to.
//! 3. Records the same signal into OpenTelemetry counters so downstream
//!    dashboards see identical numbers.
//!
//! The layer lives in `telemetry.rs`; this module owns the plain-data
//! primitives so it can be depended on without pulling the tracing layer
//! in.

use crate::cache::Cache;
use opentelemetry::metrics::Counter;
use opentelemetry::KeyValue;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

/// KV subject prefix all flow-activity events are published under.
/// Keys are of the form `<PREFIX>.<flow>.<task_or__flow>`.
pub const ACTIVITY_PREFIX: &str = "flowgen.activity";

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
    Running,
    /// Last observed event was a `warn!`.
    Warning,
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
        (info, FlowStatus::Running),
        (warn, FlowStatus::Warning),
        (err, FlowStatus::Error),
    ];
    let mut winner: Option<(u64, FlowStatus)> = None;
    for (ts, status) in latest {
        match (ts, winner) {
            (Some(t), Some((best, _))) if t > best => winner = Some((t, status)),
            (Some(t), Some((best, prev))) if t == best => {
                let level = match (status, prev) {
                    (FlowStatus::Error, _) | (_, FlowStatus::Error) => FlowStatus::Error,
                    (FlowStatus::Warning, _) | (_, FlowStatus::Warning) => FlowStatus::Warning,
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

/// Single activity notification broadcast to SSE subscribers.
#[derive(Debug, Clone, Serialize)]
pub struct FlowActivity {
    /// Owning flow name.
    pub flow: String,
    /// Task name if the event happened inside a `task.run` span.
    /// `None` for flow-scoped events that don't carry a task.
    pub task: Option<String>,
    /// Processor type of the emitting task (e.g. `gcp_bigquery_query`).
    /// `None` for events outside a `task.run` scope.
    pub task_type: Option<String>,
    /// Level bucket derived from the tracing event level.
    pub level: ActivityLevel,
    /// Unix milliseconds of the event.
    pub ts_ms: u64,
    /// Human-readable event message (tracing event body / target). Empty
    /// when the event carried no formatted message.
    pub message: String,
    /// Wall-clock duration of the `task.handle` span this event fired in,
    /// in milliseconds. `None` outside a `task.handle` scope (source-task
    /// pings, ambient warnings).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Correlation id of the event this activity was emitted for. Grep target
    /// for cross-referencing with structured logs / OTel traces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// Post-snapshot metrics for the flow — lets SSE consumers update
    /// counters/status without needing a follow-up REST call.
    pub metrics: FlowMetricsSnapshot,
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

/// Shared handle carried around by app and telemetry: aggregates
/// per-flow metrics and publishes activity to the cache-backed KV so
/// SSE subscribers on any replica can replay recent history + live
/// events for a flow they open in the UI.
#[derive(Debug)]
pub struct FlowRegistry {
    inner: RwLock<HashMap<String, Arc<FlowMetrics>>>,
    cache: Arc<dyn Cache>,
    counters: OnceLock<Counters>,
}

/// Builder for [`FlowRegistry`]. Follows the same shape as
/// `flowgen_nats::cache::CacheBuilder` so consumers stay consistent
/// across the workspace.
pub struct FlowRegistryBuilder {
    cache: Option<Arc<dyn Cache>>,
}

impl FlowRegistryBuilder {
    pub fn new() -> Self {
        Self { cache: None }
    }

    pub fn cache(mut self, cache: Arc<dyn Cache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn build(self) -> Arc<FlowRegistry> {
        let cache = self
            .cache
            .expect("FlowRegistryBuilder requires a cache; call .cache(...) before build()");
        Arc::new(FlowRegistry {
            inner: RwLock::new(HashMap::new()),
            cache,
            counters: OnceLock::new(),
        })
    }
}

impl Default for FlowRegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct Counters {
    events: Counter<u64>,
    warnings: Counter<u64>,
    errors: Counter<u64>,
}

impl FlowRegistry {
    pub fn builder() -> FlowRegistryBuilder {
        FlowRegistryBuilder::new()
    }

    /// Called by the tracing layer for every classified event. Bumps
    /// local counters synchronously and fires an async publish onto the
    /// cache — errors are swallowed because activity is best-effort UI
    /// signal, not a correctness-critical write.
    pub fn record(&self, flow: &str, event: RecordedEvent) {
        let metrics = self.slot(flow);
        metrics.record(event.level, event.ts_ms);
        self.emit_otel(flow, event.task.as_deref(), event.level);

        let snapshot = metrics.snapshot(flow.to_string());
        let activity = FlowActivity {
            flow: flow.to_string(),
            task: event.task,
            task_type: event.task_type,
            level: event.level,
            ts_ms: event.ts_ms,
            message: event.message,
            duration_ms: event.duration_ms,
            event_id: event.event_id,
            metrics: snapshot,
        };
        self.publish(activity);
    }

    fn publish(&self, activity: FlowActivity) {
        let cache = Arc::clone(&self.cache);
        let key = activity_key(&activity.flow, activity.task.as_deref());
        let payload = match serde_json::to_vec(&activity) {
            Ok(v) => bytes::Bytes::from(v),
            Err(_) => return,
        };
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => return,
        };
        handle.spawn(async move {
            let _ = cache.put(&key, payload, None).await;
        });
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

    /// Snapshot every flow's current counters. Used by the REST API and
    /// as the initial payload SSE consumers receive on connect.
    pub fn snapshot_all(&self) -> Vec<FlowMetricsSnapshot> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .iter()
            .map(|(flow, metrics)| metrics.snapshot(flow.clone()))
            .collect()
    }

    /// Snapshot a single flow, returning `None` when no event has ever
    /// been recorded for it (so the API can distinguish "unknown" from
    /// "known but idle").
    pub fn snapshot(&self, flow: &str) -> Option<FlowMetricsSnapshot> {
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(flow).map(|m| m.snapshot(flow.to_string()))
    }
}

/// KV key used to publish a single flow-task activity event. The `_flow`
/// sentinel keeps flow-scoped events (no task) in the same namespace so
/// `watch("flowgen.activity.")` sees them alongside task events.
fn activity_key(flow: &str, task: Option<&str>) -> String {
    match task {
        Some(t) => format!("{ACTIVITY_PREFIX}.{flow}.{t}"),
        None => format!("{ACTIVITY_PREFIX}.{flow}._flow"),
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
        assert_eq!(derive_status(Some(10), None, None), FlowStatus::Running);
        assert_eq!(derive_status(Some(10), Some(20), None), FlowStatus::Warning);
        assert_eq!(
            derive_status(Some(30), Some(20), Some(10)),
            FlowStatus::Running
        );
        assert_eq!(
            derive_status(Some(10), Some(20), Some(30)),
            FlowStatus::Error
        );
    }

    #[tokio::test]
    async fn record_updates_counters_and_publishes_to_cache() {
        use crate::cache::{Cache, MemoryCache, WatchEvent};
        use futures_util::StreamExt;

        let cache: Arc<dyn Cache> = Arc::new(MemoryCache::new());
        let reg = FlowRegistry::builder().cache(Arc::clone(&cache)).build();
        let mut stream = cache.watch(ACTIVITY_PREFIX, false).await.unwrap();
        reg.record(
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
        reg.record(
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

        let snapshot = reg.snapshot("f").unwrap();
        assert_eq!(snapshot.events_total, 1);
        assert_eq!(snapshot.errors_total, 1);
        assert_eq!(snapshot.last_event_at_ms, Some(100));
        assert_eq!(snapshot.last_error_at_ms, Some(200));
        assert_eq!(snapshot.status, FlowStatus::Error);

        let first = stream.next().await.unwrap().unwrap();
        let second = stream.next().await.unwrap().unwrap();
        let WatchEvent::Put { key: k1, .. } = first else {
            unreachable!("watch only publishes Put")
        };
        let WatchEvent::Put { key: k2, .. } = second else {
            unreachable!("watch only publishes Put")
        };
        assert_eq!(k1, "flowgen.activity.f.t");
        assert_eq!(k2, "flowgen.activity.f.t");
    }
}
