// Shared SSE + activity buffer for the whole app.
//
// One EventSource per browser session. Pages / components read from the
// exposed accessors instead of opening their own subscriptions — this stops
// the "modal + detail page = two subscriptions replaying history twice"
// double-count, and keeps rAF batching centralized.

import { browser } from '$app/environment';
import { apiUrl, type FlowStatus } from '$lib/api';
import { rafBatch } from '$lib/rafBatch';

export type ActivityLevel = 'info' | 'warning' | 'error';
export type { FlowStatus };

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
	metrics: FlowMetricsSnapshot;
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
			metricsByFlow[a.flow] = a.metrics;
		}
		for (const [flow, evts] of groups) {
			const existing = buckets[flow] ?? [];
			buckets[flow] = [...existing, ...evts];
		}
	});

	const sse = new EventSource(apiUrl('api/flows/stream'));
	sse.addEventListener('activity', (e) => {
		let activity: FlowActivity;
		try {
			activity = JSON.parse(e.data) as FlowActivity;
		} catch {
			return;
		}
		flush(activity);
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
