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
import { m } from '$lib/paraglide/messages.js';

export type ActivityLevel = LogRecord['level'];

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

// Event-level field keys with special handling in `extractFieldSummary`,
// matching the names `flowgen_core::event` writes server-side.
const FIELD_EVENT_ID = 'event.id';
const FIELD_EVENT_ID_LEGACY = 'event_id';
const FIELD_EVENT_SUBJECT = 'event.subject';
// Joined `send_with_logging().context(k, v)` calls — see the comment in
// `extractFieldSummary` for why this needs unpacking.
const FIELD_CONTEXT = 'context';

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
		if (f.key === FIELD_EVENT_ID || f.key === FIELD_EVENT_ID_LEGACY) {
			event_id = f.value;
			continue;
		}
		if (f.key === FIELD_EVENT_SUBJECT) continue;
		// `context` carries `send_with_logging().context(k, v)` calls, joined
		// server-side into one JSON object because `tracing` fields must be
		// static per callsite. Split it back into individual attributes here
		// so e.g. token counts and latency show as their own rows, not one
		// blob string.
		if (f.key === FIELD_CONTEXT) {
			try {
				const parsed: unknown = JSON.parse(f.value);
				if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
					for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
						extra.push([k, typeof v === 'string' ? v : JSON.stringify(v)]);
					}
					continue;
				}
			} catch {
				// Not JSON — fall through and show it as a plain attribute.
			}
		}
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

const LEVEL_ORDER: ActivityLevel[] = ['info', 'warn', 'error', 'debug', 'trace'];

// How a level chip shows: shown by default (`rest`), picked, or hidden (`off`).
export type LevelChip = 'rest' | 'picked' | 'off';

// The levels picked: those shown, unless they are exactly the defaults.
function pickedLevels(
	filter: Record<ActivityLevel, boolean>,
	defaults: Readonly<Record<ActivityLevel, boolean>>
): ActivityLevel[] {
	if (LEVEL_ORDER.every((level) => filter[level] === defaults[level])) return [];
	return LEVEL_ORDER.filter((level) => filter[level]);
}

export function levelChip(
	filter: Record<ActivityLevel, boolean>,
	defaults: Readonly<Record<ActivityLevel, boolean>>,
	level: ActivityLevel
): LevelChip {
	const picked = pickedLevels(filter, defaults);
	if (picked.length === 0) return defaults[level] ? 'rest' : 'off';
	return picked.includes(level) ? 'picked' : 'off';
}

export function hasPickedLevels(
	filter: Record<ActivityLevel, boolean>,
	defaults: Readonly<Record<ActivityLevel, boolean>>
): boolean {
	return pickedLevels(filter, defaults).length > 0;
}

// The filter after a click on `level`: it joins or leaves the picked levels,
// and with none picked the defaults show.
export function clickedLevels(
	filter: Record<ActivityLevel, boolean>,
	defaults: Readonly<Record<ActivityLevel, boolean>>,
	level: ActivityLevel
): Record<ActivityLevel, boolean> {
	const picked = pickedLevels(filter, defaults);
	const next = picked.includes(level)
		? picked.filter((key) => key !== level)
		: [...picked, level];
	if (next.length === 0) return { ...defaults };
	return {
		info: next.includes('info'),
		warn: next.includes('warn'),
		error: next.includes('error'),
		debug: next.includes('debug'),
		trace: next.includes('trace')
	};
}

// Chip CSS classes for a level, shared by `/logs` and the per-flow Activity panel.
export function levelChipClass(level: ActivityLevel, chip: LevelChip): string {
	if (chip === 'off') return 'border-base-300 chip-inactive';
	switch (level) {
		case 'error':
			return chip === 'picked' ? 'chip-error' : 'border-(--color-chip-error)/40 text-(--color-chip-error)';
		case 'warn':
			return chip === 'picked'
				? 'chip-warn'
				: 'border-(--color-chip-warning)/40 text-(--color-chip-warning)';
		case 'info':
			return chip === 'picked'
				? 'chip-success'
				: 'border-(--color-chip-success)/40 text-(--color-chip-success)';
		default:
			return chip === 'picked' ? 'chip-neutral' : 'border-base-300';
	}
}

export function levelVariant(level: ActivityLevel): 'neutral' | 'success' | 'warning' | 'error' {
	switch (level) {
		case 'info':
			return 'success';
		case 'warn':
			return 'warning';
		case 'error':
			return 'error';
		default:
			return 'neutral';
	}
}

// Display label for a level, capitalized for chip/header text.
export function levelLabel(level: ActivityLevel): string {
	switch (level) {
		case 'warn':
			return m.level_warn();
		case 'error':
			return m.level_error();
		case 'debug':
			return m.level_debug();
		case 'trace':
			return m.level_trace();
		default:
			return m.level_info();
	}
}
