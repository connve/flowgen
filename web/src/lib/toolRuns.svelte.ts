// Flow runs an agent's tool calls started, rebuilt from the flows' own logs:
// each task logs the run's `correlation_id` with its `duration_ms`, so the
// chat shows what the flow did and how long it took, as the flow measured it.

import { browser } from '$app/environment';
import { apiUrl, type LogRecord } from '$lib/api';
import { LOGS_LIMIT_DEFAULT } from '$lib/logsLimit';
import type { ToolStep } from '$lib/conversations';
import { extractSpanSummary, timestampMs, type ActivityLevel } from '$lib/logRecord';

/** One task of a run; repeated handling (e.g. per iterated item) folds into `count`. */
export interface RunTask {
	task: string;
	task_type: string | null;
	count: number;
	duration_ms: number;
	level: ActivityLevel;
}

export interface ToolRun {
	tasks: RunTask[];
	/** The last task that logged, for the live line. */
	last: string | null;
	/** The first task that logged an error. */
	failed: string | null;
	warned: boolean;
	started_ms: number;
	ended_ms: number;
}

interface RunRecord {
	task: string;
	task_type: string | null;
	level: ActivityLevel;
	ts_ms: number;
	duration_ms: number;
}

const records = $state<Record<string, RunRecord[]>>({});

let watched = new Map<string, Set<string>>();
let liveFlow: string | null = null;
let liveSource: EventSource | null = null;

function correlationId(record: LogRecord): string | null {
	return record.fields.find((f) => f.key === 'correlation_id')?.value ?? null;
}

function toRunRecord(record: LogRecord): [string, RunRecord] | null {
	const run = correlationId(record);
	if (!run) return null;
	const span = extractSpanSummary(record);
	if (!span.task) return null;
	return [
		run,
		{
			task: span.task,
			task_type: span.task_type,
			level: record.level,
			ts_ms: timestampMs(record) ?? 0,
			duration_ms: span.duration_ms ?? 0
		}
	];
}

function levelsQuery(): string {
	return 'levels=info&levels=warn&levels=error';
}

function recordKey(record: RunRecord): string {
	return `${record.ts_ms}|${record.task}|${record.level}|${record.duration_ms}`;
}

/** Adds the records a run doesn't have yet, as fetched and streamed copies overlap. */
function merge(run: string, found: RunRecord[]) {
	const known = records[run] ?? [];
	const left = new Map<string, number>();
	for (const record of known) left.set(recordKey(record), (left.get(recordKey(record)) ?? 0) + 1);
	const added = found.filter((record) => {
		const key = recordKey(record);
		const count = left.get(key) ?? 0;
		left.set(key, count - 1);
		return count <= 0;
	});
	if (added.length > 0) records[run] = [...known, ...added].sort((a, b) => a.ts_ms - b.ts_ms);
}

async function fetchFlow(flow: string) {
	try {
		const res = await fetch(
			apiUrl(
				`api/logs?flow=${encodeURIComponent(flow)}&limit=${LOGS_LIMIT_DEFAULT}&${levelsQuery()}`
			)
		);
		if (!res.ok) return;
		const found: Record<string, RunRecord[]> = {};
		for (const record of (await res.json()) as LogRecord[]) {
			const hit = toRunRecord(record);
			if (!hit || !watched.get(flow)?.has(hit[0])) continue;
			(found[hit[0]] ??= []).push(hit[1]);
		}
		for (const [run, list] of Object.entries(found)) merge(run, list);
	} catch {
		// Best effort: a step without its run still shows the call itself.
	}
}

function follow(flow: string | null) {
	if (!browser || flow === liveFlow) return;
	liveSource?.close();
	liveSource = null;
	liveFlow = flow;
	if (!flow) return;
	const source = new EventSource(
		apiUrl(`api/logs/stream?flow=${encodeURIComponent(flow)}&${levelsQuery()}`)
	);
	liveSource = source;
	source.addEventListener('open', () => void fetchFlow(flow));
	source.addEventListener('log', (e) => {
		let record: LogRecord;
		try {
			record = JSON.parse(e.data) as LogRecord;
		} catch {
			return;
		}
		const hit = toRunRecord(record);
		if (!hit || !watched.get(flow)?.has(hit[0])) return;
		merge(hit[0], [hit[1]]);
	});
}

/**
 * Loads the runs of a conversation's tool steps, keyed by flow, and follows
 * `live` (the flow of a call still running) until it is null.
 */
export function watchToolRuns(runs: Array<{ flow: string; run: string }>, live: string | null) {
	if (!browser) return;
	const next = new Map<string, Set<string>>();
	for (const { flow, run } of runs) next.set(flow, (next.get(flow) ?? new Set<string>()).add(run));
	const previous = watched;
	watched = next;
	const kept = new Set(runs.map(({ run }) => run));
	for (const run of Object.keys(records)) {
		if (!kept.has(run)) delete records[run];
	}
	for (const [flow, set] of next) {
		const known = previous.get(flow);
		if ([...set].some((run) => !known?.has(run))) void fetchFlow(flow);
	}
	follow(live);
}

/** Stops following a run's flow live and forgets the runs, e.g. when the page goes away. */
export function releaseToolRuns() {
	watchToolRuns([], null);
}

/** The run saved with a finished reply's tool step, or null when none was saved. */
export function savedRun(step: ToolStep): ToolRun | null {
	const saved = step.tasks ?? [];
	if (saved.length === 0) return null;
	const tasks: RunTask[] = saved.map((task) => ({
		task: task.task,
		task_type: task.taskType ?? null,
		count: task.count,
		duration_ms: task.durationMs,
		level: task.level
	}));
	return {
		tasks,
		last: tasks[tasks.length - 1].task,
		failed: tasks.find((task) => task.level === 'error')?.task ?? null,
		warned: tasks.some((task) => task.level === 'warn'),
		started_ms: 0,
		ended_ms: step.durationMs ?? 0
	};
}

/** A run as the flow logged it, or null when its logs are not (or no longer) retained. */
export function toolRun(run: string | undefined): ToolRun | null {
	if (!run) return null;
	const list = records[run];
	if (!list || list.length === 0) return null;
	const tasks: RunTask[] = [];
	const byName = new Map<string, RunTask>();
	let failed: string | null = null;
	let warned = false;
	let started_ms = Infinity;
	let ended_ms = -Infinity;
	for (const record of list) {
		started_ms = Math.min(started_ms, record.ts_ms - record.duration_ms);
		ended_ms = Math.max(ended_ms, record.ts_ms);
		const known = byName.get(record.task);
		if (known) {
			known.count += 1;
			known.duration_ms += record.duration_ms;
			if (rank(record.level) > rank(known.level)) known.level = record.level;
		} else {
			const task = {
				task: record.task,
				task_type: record.task_type,
				count: 1,
				duration_ms: record.duration_ms,
				level: record.level
			};
			byName.set(record.task, task);
			tasks.push(task);
		}
		if (record.level === 'error') failed ??= record.task;
		if (record.level === 'warn') warned = true;
	}
	return {
		tasks,
		last: list[list.length - 1].task,
		failed,
		warned,
		started_ms,
		ended_ms
	};
}

function rank(level: ActivityLevel): number {
	switch (level) {
		case 'error':
			return 2;
		case 'warn':
			return 1;
		default:
			return 0;
	}
}

/** A duration for a status line: `840ms`, `3.2s`, `1m 05s`. */
export function formatDuration(ms: number): string {
	ms = Math.max(0, ms);
	if (ms < 1000) return `${Math.round(ms)}ms`;
	if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
	const minutes = Math.floor(ms / 60_000);
	const seconds = Math.floor((ms % 60_000) / 1000);
	return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
}
