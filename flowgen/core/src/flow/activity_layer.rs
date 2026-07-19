//! Tracing layer that turns runtime log events into flow-level activity.
//!
//! Attached to the global subscriber it does two things:
//! - Walks parent spans of each event to find the owning `flow` (from
//!   `flow.run`) and optional `task` (from `task.run`), then calls
//!   `FlowRegistry::record` with the level derived from `tracing::Level`.
//! - INFO events require a `task.handle` scope so ambient boot logs
//!   don't inflate `events_total`; WARN/ERROR count wherever they land
//!   inside a `flow.run` scope (init failures raise the flow status
//!   even before any event handling begins).
//!
//! The layer only *reads* span fields — it never blocks, allocates
//! beyond a single String clone, or calls into the async runtime, so
//! attaching it is safe on the hot path.

use std::sync::Arc;
use std::time::Instant;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::flow::activity::{now_ms, ActivityLevel, FlowRegistry};

const TASK_HANDLE_SPAN: &str = "task.handle";
const FLOW_RUN_SPAN: &str = "flow.run";
const TASK_RUN_SPAN: &str = "task.run";

/// Layer wrapping the shared registry. Cheap to clone via `Arc`.
pub struct FlowActivityLayer {
    registry: Arc<FlowRegistry>,
}

impl FlowActivityLayer {
    /// Wraps the registry so it can be added as a tracing subscriber layer.
    pub fn new(registry: Arc<FlowRegistry>) -> Self {
        Self { registry }
    }
}

impl<S> Layer<S> for FlowActivityLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: Context<'_, S>,
    ) {
        // Capture the `flow` / `task` field values on the spans that
        // carry them so events later can find them via extensions. The
        // built-in registry stores string fields only if we ask for it.
        let name = attrs.metadata().name();
        if !matches!(name, FLOW_RUN_SPAN | TASK_RUN_SPAN | TASK_HANDLE_SPAN) {
            return;
        }
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut ext = span.extensions_mut();
        if name == TASK_HANDLE_SPAN {
            // Stash start time so on_event can compute per-handle duration.
            ext.insert(SpanStart(Instant::now()));
            return;
        }
        let mut visitor = FieldCapture::default();
        attrs.record(&mut visitor);
        if name == FLOW_RUN_SPAN {
            if let Some(flow) = visitor.flow {
                ext.insert(SpanFlow(flow));
            }
        } else if name == TASK_RUN_SPAN {
            if let Some(task) = visitor.task {
                ext.insert(SpanTask(task));
            }
            if let Some(task_type) = visitor.task_type {
                ext.insert(SpanTaskType(task_type));
            }
            // Source-task processors (llm_proxy, mcp_*, http_endpoint) attach
            // their own task.run span from an HTTP handler that has no
            // flow.run in scope. Accept `flow` here so those spans still
            // resolve a flow name for the activity layer.
            if let Some(flow) = visitor.flow {
                ext.insert(SpanFlow(flow));
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(level) = classify(event.metadata().level()) else {
            // debug/trace — ignore.
            return;
        };

        let scope = match ctx.event_scope(event) {
            Some(s) => s,
            None => return,
        };

        let mut in_task_handle = false;
        let mut flow: Option<String> = None;
        let mut task: Option<String> = None;
        let mut task_type: Option<String> = None;
        let mut duration_ms: Option<u64> = None;

        for span in scope.from_root() {
            let name = span.name();
            let ext = span.extensions();
            if name == TASK_HANDLE_SPAN {
                in_task_handle = true;
                if let Some(SpanStart(start)) = ext.get::<SpanStart>() {
                    duration_ms = Some(start.elapsed().as_millis() as u64);
                }
            }
            if let Some(SpanFlow(f)) = ext.get::<SpanFlow>() {
                flow = Some(f.clone());
            }
            if let Some(SpanTask(t)) = ext.get::<SpanTask>() {
                task = Some(t.clone());
            }
            if let Some(SpanTaskType(t)) = ext.get::<SpanTaskType>() {
                task_type = Some(t.clone());
            }
        }

        // Backfill the `duration_ms` field on the `task.handle` span so
        // the JSON formatter includes it in the emitted line's `spans`
        // array. The field is declared as `field::Empty` on every
        // `task.handle` instrument macro.
        if let Some(ms) = duration_ms {
            tracing::Span::current().record("duration_ms", ms);
        }

        let Some(flow) = flow else {
            return;
        };

        let mut msg = MessageCapture::default();
        event.record(&mut msg);

        // Info events count when they either happen inside a task.handle
        // scope (routine per-event processing) or carry an `event.subject`
        // field (source tasks like `generate` produce events from their
        // task.run scope without a handle span). Ambient info logs outside
        // both are dropped so counters stay signal, not boot noise.
        if matches!(level, ActivityLevel::Info) && !in_task_handle && !msg.has_event_subject {
            return;
        }
        let event_id = msg.event_id.clone();
        self.registry.record(
            &flow,
            crate::flow::activity::RecordedEvent {
                task,
                task_type,
                level,
                ts_ms: now_ms(),
                message: msg.into_message(),
                duration_ms,
                event_id,
            },
        );
    }
}

/// Extracts the human-readable message from a tracing event. tracing emits
/// the formatted body under the reserved field name `message` — everything
/// else on the event (structured fields like `error=...`, `flow=...`) is
/// captured separately via span extensions or ignored here.
#[derive(Default)]
struct MessageCapture {
    message: Option<String>,
    /// Structured fields other than `message` — appended to the final
    /// text as `key=value` pairs so consumers see e.g. the underlying
    /// `error=...` cause alongside the human message.
    fields: Vec<(String, String)>,
    has_event_subject: bool,
    event_id: Option<String>,
}

impl MessageCapture {
    fn into_message(self) -> String {
        let mut out = self.message.unwrap_or_default();
        for (k, v) in self.fields {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(&k);
            out.push('=');
            out.push_str(&v);
        }
        out
    }

    fn record_field(&mut self, name: &str, value: String) {
        match name {
            "message" => self.message = Some(value),
            "event.subject" => {
                self.has_event_subject = true;
            }
            "event.id" => self.event_id = Some(value),
            // Span-carried context fields duplicated onto events —
            // already tracked separately, skip them so the message
            // doesn't repeat them.
            "flow" | "task" | "task_id" | "task_type" => {}
            _ => self.fields.push((name.to_string(), value)),
        }
    }
}

impl Visit for MessageCapture {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_field(field.name(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record_field(field.name(), format!("{value:?}"));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_field(field.name(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_field(field.name(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_field(field.name(), value.to_string());
    }
}

fn classify(level: &Level) -> Option<ActivityLevel> {
    match *level {
        Level::ERROR => Some(ActivityLevel::Error),
        Level::WARN => Some(ActivityLevel::Warning),
        Level::INFO => Some(ActivityLevel::Info),
        _ => None,
    }
}

/// Extensions stored on `flow.run` spans so children can look up the
/// flow name without re-parsing fields.
#[derive(Debug, Clone)]
struct SpanFlow(String);

/// Extensions stored on `task.run` spans (task name).
#[derive(Debug, Clone)]
struct SpanTask(String);

/// Extensions stored on `task.run` spans (processor type — e.g. `generate`,
/// `gcp_bigquery_query`). Reported to the UI so users can tell at a glance
/// which processor emitted the event without cross-referencing the YAML.
#[derive(Debug, Clone)]
struct SpanTaskType(String);

/// Wall-clock start of a `task.handle` span, used to compute per-handle
/// duration when the terminal event fires.
#[derive(Debug, Clone)]
struct SpanStart(Instant);

#[derive(Default)]
struct FieldCapture {
    flow: Option<String>,
    task: Option<String>,
    task_type: Option<String>,
}

impl Visit for FieldCapture {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "flow" => self.flow = Some(value.to_string()),
            "task" => self.task = Some(value.to_string()),
            "task_type" => self.task_type = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `#[tracing::instrument(fields(flow = %self.config.flow.name))]`
        // uses the Display formatter, so the value arrives here as debug
        // for us to strip the surrounding quotes tracing writes for
        // string-like values.
        match field.name() {
            "flow" => self.flow = Some(strip_quotes(format!("{:?}", value))),
            "task" => self.task = Some(strip_quotes(format!("{:?}", value))),
            "task_type" => self.task_type = Some(strip_quotes(format!("{:?}", value))),
            _ => {}
        }
    }
}

fn strip_quotes(s: String) -> String {
    let trimmed = s.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::{error, info, info_span, warn};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Registry;

    fn install(reg: Arc<FlowRegistry>) -> tracing::dispatcher::DefaultGuard {
        Registry::default()
            .with(FlowActivityLayer::new(reg))
            .set_default()
    }

    #[test]
    fn captures_events_under_flow_and_task_handle_scopes() {
        let reg = FlowRegistry::builder()
            
            .build();
        let _g = install(Arc::clone(&reg));

        // Simulate the flow.run > task.run > task.handle > event stack.
        let flow_span = info_span!("flow.run", flow = "demo");
        let _flow = flow_span.enter();
        let task_span = info_span!("task.run", task = "step");
        let _task = task_span.enter();
        let handle_span = info_span!("task.handle");
        let _handle = handle_span.enter();

        info!("event.subject=trigger");
        warn!("stale cache");
        error!("timed out");

        let snap = reg.snapshot("demo").expect("recorded");
        assert_eq!(snap.events_total, 1);
        assert_eq!(snap.warnings_total, 1);
        assert_eq!(snap.errors_total, 1);
    }

    #[test]
    fn ignores_info_outside_task_handle_scope() {
        let reg = FlowRegistry::builder()
            
            .build();
        let _g = install(Arc::clone(&reg));
        let flow_span = info_span!("flow.run", flow = "demo");
        let _flow = flow_span.enter();
        let task_span = info_span!("task.run", task = "step");
        let _task = task_span.enter();
        info!("no handle in scope");
        assert!(reg.snapshot("demo").is_none());
    }

    #[test]
    fn counts_info_with_event_subject_outside_task_handle() {
        let reg = FlowRegistry::builder()
            
            .build();
        let _g = install(Arc::clone(&reg));
        let flow_span = info_span!("flow.run", flow = "demo");
        let _flow = flow_span.enter();
        // Source task (e.g. `generate`) emits inside task.run, no handle.
        let task_span = info_span!("task.run", task = "trigger");
        let _task = task_span.enter();
        info!(event.subject = "trigger", event.id = "1");

        let snap = reg.snapshot("demo").expect("recorded");
        assert_eq!(snap.events_total, 1);
    }

    #[test]
    fn counts_init_errors_on_task_run_span() {
        let reg = FlowRegistry::builder()
            
            .build();
        let _g = install(Arc::clone(&reg));
        let flow_span = info_span!("flow.run", flow = "demo");
        let _flow = flow_span.enter();
        let task_span = info_span!("task.run", task = "call_api");
        let _task = task_span.enter();
        error!("Failed to initialize processor");
        warn!("degraded mode");

        let snap = reg.snapshot("demo").expect("recorded");
        assert_eq!(snap.errors_total, 1);
        assert_eq!(snap.warnings_total, 1);
        assert_eq!(snap.events_total, 0);
    }

    #[test]
    fn ignores_events_outside_any_flow_scope() {
        let reg = FlowRegistry::builder()
            
            .build();
        let _g = install(Arc::clone(&reg));
        // Ambient info!() outside flow.run should be silently dropped.
        info!("boot message");
        assert!(reg.snapshot_all().is_empty());
    }
}
