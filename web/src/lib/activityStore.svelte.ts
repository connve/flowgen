// Shared SSE + activity buffer for the whole app.
//
// Two independent sources, matching the two backend domains:
// - Metrics/status: one shared EventSource on `/api/flows/stream`, opened
//   once for the whole session (cheap — a handful of counters per flow).
// - Event history + live tail: per-flow, opened lazily the first time a
//   flow's activity is actually read. History comes from
//   `GET /api/logs?flow=X&limit=1000` (the same endpoint and limit the
//   global /logs viewer uses, just scoped), then `/api/logs/stream?flow=X`
//   appends new records — so a flow that already finished emitting still
//   shows its history instead of "No events yet".
//
// The server ships raw `LogRecord` frames; the classification (task-scope
// filter, span hoisting, level bucketing) happens client-side so the
// /logs viewer and per-flow activity panel use the same helper
// (`$lib/logRecord`) and never drift.

import { browser } from '$app/environment';
import { apiUrl, type FlowStatus, type LogRecord } from '$lib/api';
import { LOGS_LIMIT_DEFAULT } from '$lib/logsLimit';
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

// Per-flow ring buffer; kept in $state so pages/components can read reactively.
// SvelteMap would work but a plain object keyed by flow name serialises simpler
// and per-flow arrays are what every consumer actually wants.
const buckets = $state<Record<string, FlowActivity[]>>({});
let metricsByFlow = $state<Record<string, FlowMetricsSnapshot>>({});
let metricsStarted = false;

// Only one flow's Activity panel is ever viewed at a time in practice, so
// only one per-flow EventSource is kept open — opening one per flow ever
// visited exhausts the browser's per-origin HTTP/1.1 connection cap (6 in
// Chrome) and starves plain fetch() calls (e.g. the /logs page's limit
// control) that share the same origin.
let activeFlow: string | null = null;
let activeSource: EventSource | null = null;

// History limit for the per-flow Activity panel. Independent of the
// global /logs page's own limit (`$lib/logsPageState`) — they share only
// the bounds (`$lib/logsLimit`), not the value, since raising one
// shouldn't affect the other view.
let flowActivityLimit = $state(LOGS_LIMIT_DEFAULT);

export function getFlowActivityLimit(): number {
	return flowActivityLimit;
}

// Opens the single shared metrics stream. Independent of any per-flow
// event subscription — metrics/status are read from `MetricsStore` on
// the backend and pushed on every counter change, not derived from the
// event stream client-side.
function ensureMetricsSubscription() {
	if (!browser || metricsStarted) return;
	metricsStarted = true;

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
}

// Fetches a flow's history from `/api/logs?flow=X` (same endpoint the
// global /logs viewer uses, scoped server-side to this flow, capped at
// `flowActivityLimit`) and replaces its bucket.
async function refetchFlowHistory(flow: string) {
	try {
		const res = await fetch(
			apiUrl(`api/logs?flow=${encodeURIComponent(flow)}&limit=${flowActivityLimit}`),
		);
		const records = (await res.json()) as LogRecord[];
		buckets[flow] = records.map(recordToActivity).filter((a): a is FlowActivity => a !== null);
	} catch {
		// History backfill is best-effort — live tail keeps working regardless.
	}
}

// Changes the Activity panel's history limit and re-fetches the active
// flow (if any) so the change is visible immediately, matching how the
// /logs page's own limit control behaves.
export function setFlowActivityLimit(next: number) {
	flowActivityLimit = next;
	if (activeFlow) void refetchFlowHistory(activeFlow);
}

// Opens a flow's live tail (`/api/logs/stream?flow=X`) after backfilling
// its history. Opened lazily the first time `activitiesFor(flow)` is
// called for a flow; switching to a different flow closes the previous
// EventSource first — only one per-flow stream is ever open, so the
// panel doesn't accumulate a forever-open connection for every flow
// visited this session. Re-backfills on every (re)activation rather than
// only the first: the live stream is closed while a flow isn't active,
// so revisiting needs a fresh fetch to catch up on what was emitted in
// between.
async function ensureFlowSubscription(flow: string) {
	if (!browser || activeFlow === flow) return;
	activeSource?.close();
	activeFlow = flow;

	const flush = rafBatch<FlowActivity>((batch) => {
		// Group by flow so we do one array-copy per flow per frame instead of
		// one per event — the 10k/s stress test hammered this before batching.
		const groups = new Map<string, FlowActivity[]>();
		for (const a of batch) {
			const arr = groups.get(a.flow) ?? [];
			arr.push(a);
			groups.set(a.flow, arr);
		}
		for (const [f, evts] of groups) {
			const existing = buckets[f] ?? [];
			buckets[f] = [...existing, ...evts];
		}
	});

	await refetchFlowHistory(flow);

	// The flow may have changed again while the history fetch above was
	// in flight — don't attach a live stream for a flow we've already
	// navigated away from.
	if (activeFlow !== flow) return;

	const sse = new EventSource(apiUrl(`api/logs/stream?flow=${encodeURIComponent(flow)}`));
	activeSource = sse;
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
	void ensureFlowSubscription(flow);
	return buckets[flow] ?? [];
}

// Closes the active per-flow EventSource, if any. Call this from
// `onDestroy` (route navigation) or when a modal/detail view closes — the
// panel only calls `activitiesFor` while something is actually rendered,
// so nothing re-opens the stream on its own once the viewer goes away.
// Without this the connection stays open until a *different* flow is
// viewed, wasting a slot in the browser's small per-origin connection
// pool (HTTP/1.1) that plain fetch() calls elsewhere on the page need.
export function releaseFlowSubscription() {
	activeSource?.close();
	activeSource = null;
	activeFlow = null;
}

// Latest metrics snapshot for a flow (updated with every counter change).
// Used by the flow list to keep status pills / counters live.
export function metricsFor(flow: string): FlowMetricsSnapshot | undefined {
	ensureMetricsSubscription();
	return metricsByFlow[flow];
}

// All flows that have seen at least one metrics snapshot. The list page uses
// this to apply live counters to every row in one pass rather than iterating
// per event.
export function allMetrics(): Record<string, FlowMetricsSnapshot> {
	ensureMetricsSubscription();
	return metricsByFlow;
}
