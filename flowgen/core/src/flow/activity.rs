//! Per-flow activity tracking sourced from the standard `tracing` events.
//!
//! A custom [`tracing_subscriber::Layer`] (`flow_activity_layer`) captures
//! every `info!` / `warn!` / `error!` emitted inside a `task.handle` span,
//! walks the parent scope to recover the owning flow/task names, and:
//!
//! 1. Bumps atomic counters + last-seen timestamps on a [`FlowRegistry`]
//!    shared with the admin API.
//! 2. Records the same signal into OpenTelemetry counters so downstream
//!    dashboards see identical numbers.
//!
//! Log body + attributes for the admin UI come from the native
//! `tracing_subscriber::fmt::json()` writer through
//! `flowgen_core::telemetry::query::MemoryLogsWriter` — no re-emit here.
//!
//! The layer lives in `activity_layer.rs`; this module owns the plain-data
//! primitives so it can be depended on without pulling the tracing layer
//! in.

use opentelemetry::metrics::Counter;
use opentelemetry::KeyValue;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

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

/// Shared handle carried around by app and telemetry: aggregates
/// per-flow metrics and re-emits each classified event through the
/// tracing subscriber so the OTel logs bridge can forward it to
/// whichever telemetry backend is configured. SSE subscribers on any
/// replica then read the same records back through `LogsQuery`.
#[derive(Debug)]
pub struct FlowRegistry {
    inner: RwLock<HashMap<String, Arc<FlowMetrics>>>,
    counters: OnceLock<Counters>,
}

/// Builder for [`FlowRegistry`]. Follows the same shape as
/// `flowgen_nats::cache::CacheBuilder` so consumers stay consistent
/// across the workspace.
#[derive(Default)]
pub struct FlowRegistryBuilder {}

impl FlowRegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> Arc<FlowRegistry> {
        Arc::new(FlowRegistry {
            inner: RwLock::new(HashMap::new()),
            counters: OnceLock::new(),
        })
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
    /// local counters synchronously and updates the OTel metrics
    /// counter. The tracing event itself already flows to stdout / the
    /// memory logs writer through the `fmt::json` layer.
    pub fn record(&self, flow: &str, event: RecordedEvent) {
        let metrics = self.slot(flow);
        metrics.record(event.level, event.ts_ms);
        self.emit_otel(flow, event.task.as_deref(), event.level);
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

    #[test]
    fn record_updates_counters_across_levels() {
        let reg = FlowRegistry::builder().build();
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

        let snapshot = reg.snapshot("f").expect("flow must be registered");
        assert_eq!(snapshot.events_total, 1);
        assert_eq!(snapshot.errors_total, 1);
        assert_eq!(snapshot.last_event_at_ms, Some(100));
        assert_eq!(snapshot.last_error_at_ms, Some(200));
        assert_eq!(snapshot.status, FlowStatus::Error);
    }
}
