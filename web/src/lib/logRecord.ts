// Client-side classification and hoisting of `LogRecord` (the raw
// tracing frame the server ships). Both `/logs` (global log viewer) and
// the per-flow activity panel go through this helper so they agree on
// which fields are identity vs metadata, what counts as task-scoped,
// and how to derive display fields.
//
// The server used to do this itself for `/api/flows/stream` and left it
// alone for `/api/logs/stream`; the two paths drifted (subject filtered
// in one, kept in the other). Doing it all client-side keeps the
// server as a dumb transport and gives us a single source of truth.

import type { LogRecord, LogSpan } from '$lib/api';

export type ActivityLevel = 'info' | 'warning' | 'error' | 'debug' | 'trace';

export interface SpanSummary {
	flow: string | null;
	task: string | null;
	task_type: string | null;
	task_id: string | null;
	duration_ms: number | null;
}

export interface FieldSummary {
	event_id: string | null;
	extra: Array<[string, string]>;
}

// Span fields hoisted into overview (`SpanSummary`) so they are not
// re-shown in the raw spans list.
const HOISTED_SPAN_FIELDS = new Set(['flow', 'task', 'task_type', 'task_id', 'duration_ms']);

// Extracts identity fields from the span chain, leaf-to-root (so an
// inner span shadowing an outer span wins).
export function extractSpanSummary(record: LogRecord): SpanSummary {
	const pick = (key: string): string | null => {
		for (let i = record.spans.length - 1; i >= 0; i -= 1) {
			const span = record.spans[i];
			const hit = span.fields.find((f) => f.key === key);
			if (hit) return hit.value;
		}
		return null;
	};
	const durationRaw = pick('duration_ms');
	const duration = durationRaw !== null ? Number(durationRaw) : NaN;
	return {
		flow: pick('flow'),
		task: pick('task'),
		task_type: pick('task_type'),
		task_id: pick('task_id'),
		duration_ms: Number.isFinite(duration) ? duration : null,
	};
}

// Extracts identity fields from event-level attributes and returns the
// rest as `extra` (the display "Attributes" list). Event subject is
// dropped as an internal routing detail — the per-flow activity feed
// keys events by task name, not subject, and the log viewer already
// shows task_name in the header.
export function extractFieldSummary(record: LogRecord): FieldSummary {
	let event_id: string | null = null;
	const extra: Array<[string, string]> = [];
	for (const f of record.fields) {
		if (f.key === 'event.id' || f.key === 'event_id') {
			event_id = f.value;
			continue;
		}
		if (f.key === 'event.subject') continue;
		extra.push([f.key, f.value]);
	}
	return { event_id, extra };
}

// Returns the span chain with hoisted identity fields removed, dropping
// spans left with no fields. Used by the detail drawer to render only
// the spans that carry information not already shown in the overview.
export function nonHoistedSpans(record: LogRecord): LogSpan[] {
	return record.spans
		.map((s) => ({
			name: s.name,
			fields: s.fields.filter((f) => !HOISTED_SPAN_FIELDS.has(f.key)),
		}))
		.filter((s) => s.fields.length > 0);
}

// Returns true when the record was emitted inside a task scope
// (`task.run` or `task.handle` in the span chain). The per-flow
// activity panel only shows these; framework logs (leader election,
// HTTP server startup, reconciler events) live outside task scope and
// belong to `/logs` only.
export function isTaskScoped(record: LogRecord): boolean {
	return record.spans.some((s) => s.name === 'task.run' || s.name === 'task.handle');
}

// Parses the RFC3339 timestamp field to a UNIX epoch ms value. Returns
// `null` for records emitted without a timestamp (rare — the tracing
// JSON writer always adds one, but the type allows `null`).
export function timestampMs(record: LogRecord): number | null {
	if (!record.timestamp) return null;
	const parsed = Date.parse(record.timestamp);
	return Number.isFinite(parsed) ? parsed : null;
}

// Maps the tracing level string to the five-value activity level used by
// the counters and status pills. `warn` renames to `warning` for the
// activity/status vocabulary; the rest pass through unchanged.
export function activityLevel(record: LogRecord): ActivityLevel {
	switch (record.level) {
		case 'error':
			return 'error';
		case 'warn':
			return 'warning';
		case 'debug':
			return 'debug';
		case 'trace':
			return 'trace';
		default:
			return 'info';
	}
}

// Chip CSS classes for a level toggle button, shared by `/logs` and the
// per-flow Activity panel so neither drifts on colors. `inactive` covers
// both "toggled off" and hover states the caller doesn't otherwise style.
export function levelChipClass(level: ActivityLevel, active: boolean): string {
	if (!active) return 'chip-inactive';
	switch (level) {
		case 'error':
			return 'chip-error';
		case 'warning':
			return 'chip-warn';
		case 'info':
			return 'chip-info';
		default:
			return 'chip-neutral';
	}
}

// Text color for a level's icon/badge in the detail drawer and row markers.
export function levelBadgeColor(level: ActivityLevel): string {
	switch (level) {
		case 'error':
			return 'text-error';
		case 'warning':
			return 'text-warning';
		case 'debug':
		case 'trace':
			return 'text-base-content/50';
		default:
			return 'text-primary';
	}
}

// Background color for a level's status dot, shared by `/logs` rows and
// the Activity panel's row markers and detail drawer.
export function levelDotClass(level: ActivityLevel): string {
	switch (level) {
		case 'error':
			return 'bg-error';
		case 'warning':
			return 'bg-warning';
		case 'info':
			return 'bg-primary';
		default:
			return 'bg-base-300';
	}
}

// Display label for a level, capitalized for chip/header text.
export function levelLabel(level: ActivityLevel): string {
	switch (level) {
		case 'warning':
			return 'Warn';
		case 'error':
			return 'Error';
		case 'debug':
			return 'Debug';
		case 'trace':
			return 'Trace';
		default:
			return 'Info';
	}
}
