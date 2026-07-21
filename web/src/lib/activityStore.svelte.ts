// Shared SSE + activity buffer for the whole app.
//
// One EventSource per browser session. Pages / components read from the
// exposed accessors instead of opening their own subscriptions — this stops
// the "modal + detail page = two subscriptions replaying history twice"
// double-count, and keeps rAF batching centralized.
//
// The server ships raw `LogRecord` frames on `/api/flows/stream`; the
// classification (task-scope filter, span hoisting, level bucketing)
// happens client-side so the /logs viewer and per-flow activity panel
// use the same helper (`$lib/logRecord`) and never drift.

import { browser } from '$app/environment';
import { apiUrl, type FlowStatus, type LogRecord } from '$lib/api';
import { rafBatch } from '$lib/rafBatch';
import {
	activityLevel,
	extractFieldSummary,
	extractSpanSummary,
	isTaskScoped,
	timestampMs,
	type ActivityLevel,
} from '$lib/logRecord';

export type { ActivityLevel, FlowStatus };

export interface FlowMetricsSnapshot {
	flow: string;
	events_total: number;
	warnings_total: number;
	errors_total: number;
	last_event_at_ms: number | null;
	last_warning_at_ms: number | null;
	last_error_at_ms: number | null;
	status: FlowStatus;
}

export interface FlowActivity {
	flow: string;
	task: string | null;
	task_type: string | null;
	level: ActivityLevel;
	ts_ms: number;
	message: string;
	duration_ms?: number;
	event_id?: string;
	extra?: Array<[string, string]>;
}

// Converts a `LogRecord` into a `FlowActivity` when the record is
// task-scoped and carries a flow identifier. Returns `null` for
// framework/lifecycle logs the per-flow activity panel does not show.
function recordToActivity(record: LogRecord): FlowActivity | null {
	if (!isTaskScoped(record)) return null;
	const span = extractSpanSummary(record);
	if (!span.flow) return null;
	const fields = extractFieldSummary(record);
	const ts_ms = timestampMs(record) ?? 0;
	const activity: FlowActivity = {
		flow: span.flow,
		task: span.task,
		task_type: span.task_type,
		level: activityLevel(record),
		ts_ms,
		message: record.body,
	};
	if (span.duration_ms !== null) activity.duration_ms = span.duration_ms;
	if (fields.event_id) activity.event_id = fields.event_id;
	if (fields.extra.length > 0) activity.extra = fields.extra;
	return activity;
}

// Bumps the in-memory counters used by the flow list. Mirrors the
// server-side `FlowRegistry::record` so the list stays live between
// snapshot refreshes without re-fetching.
function bumpMetrics(activity: FlowActivity) {
	const prev = metricsByFlow[activity.flow] ?? {
		flow: activity.flow,
		events_total: 0,
		warnings_total: 0,
		errors_total: 0,
		last_event_at_ms: null,
		last_warning_at_ms: null,
		last_error_at_ms: null,
		status: 'idle' as FlowStatus,
	};
	const next: FlowMetricsSnapshot = { ...prev };
	switch (activity.level) {
		case 'info':
			next.events_total += 1;
			next.last_event_at_ms = activity.ts_ms;
			next.status = 'ok';
			break;
		case 'warning':
			next.warnings_total += 1;
			next.last_warning_at_ms = activity.ts_ms;
			next.status = 'warn';
			break;
		case 'error':
			next.errors_total += 1;
			next.last_error_at_ms = activity.ts_ms;
			next.status = 'error';
			break;
	}
	metricsByFlow[activity.flow] = next;
}

// Per-flow ring buffer; kept in $state so pages/components can read reactively.
// SvelteMap would work but a plain object keyed by flow name serialises simpler
// and per-flow arrays are what every consumer actually wants.
const buckets = $state<Record<string, FlowActivity[]>>({});
let metricsByFlow = $state<Record<string, FlowMetricsSnapshot>>({});
let started = false;

function ensureSubscription() {
	if (!browser || started) return;
	started = true;

	const flush = rafBatch<FlowActivity>((batch) => {
		// Group by flow so we do one array-copy per flow per frame instead of
		// one per event — the 10k/s stress test hammered this before batching.
		const groups = new Map<string, FlowActivity[]>();
		for (const a of batch) {
			const arr = groups.get(a.flow) ?? [];
			arr.push(a);
			groups.set(a.flow, arr);
			bumpMetrics(a);
		}
		for (const [flow, evts] of groups) {
			const existing = buckets[flow] ?? [];
			buckets[flow] = [...existing, ...evts];
		}
	});

	const sse = new EventSource(apiUrl('api/flows/stream'));
	sse.addEventListener('snapshot', (e) => {
		try {
			const initial = JSON.parse(e.data) as FlowMetricsSnapshot[];
			const merged: Record<string, FlowMetricsSnapshot> = { ...metricsByFlow };
			for (const m of initial) merged[m.flow] = m;
			metricsByFlow = merged;
		} catch {
			// ignore malformed snapshot
		}
	});
	sse.addEventListener('log', (e) => {
		let record: LogRecord;
		try {
			record = JSON.parse(e.data) as LogRecord;
		} catch {
			return;
		}
		const activity = recordToActivity(record);
		if (activity) flush(activity);
	});
}

// Returns a live snapshot of the activity buffer for a single flow.
// Callers pass either a static flow name or a $derived getter — the caller's
// reactive context updates when the underlying $state changes.
export function activitiesFor(flow: string): FlowActivity[] {
	ensureSubscription();
	return buckets[flow] ?? [];
}

// Latest metrics snapshot for a flow (updated with every activity event).
// Used by the flow list to keep status pills / counters live.
export function metricsFor(flow: string): FlowMetricsSnapshot | undefined {
	ensureSubscription();
	return metricsByFlow[flow];
}

// All flows that have seen at least one metrics snapshot. The list page uses
// this to apply live counters to every row in one pass rather than iterating
// per event.
export function allMetrics(): Record<string, FlowMetricsSnapshot> {
	ensureSubscription();
	return metricsByFlow;
}
