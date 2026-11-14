import { formatDuration } from '$lib/toolRuns.svelte';

/** The message with its `error` / `err` attribute, for generic messages such as "SSE error". */
export function withCause(message: string, attributes: Array<[string, string]> | undefined): string {
	const cause = attributes?.find(([key]) => key === 'error' || key === 'err');
	return cause ? `${message} — ${cause[1].slice(0, 200)}` : message;
}

/** What a log line without a message shows: its processor and duration. */
export function quietLine(taskType: string | null | undefined, durationMs: number | null | undefined): string {
	const parts = [taskType, durationMs == null ? null : formatDuration(durationMs)];
	return parts.filter((part) => part).join(' · ');
}

/** A log line: level dot, when and source, then the message, which wraps under them in a narrow container. */
export const LOG_LINE =
	'grid grid-cols-[0.5rem_5rem_minmax(0,1fr)] items-start gap-x-3 gap-y-0.5 py-1.5 text-xs leading-4 transition-colors @2xl:grid-cols-[0.5rem_5rem_minmax(0,9rem)_minmax(0,1fr)]';

/** The message of a log line, at most two lines. */
export const LOG_MESSAGE =
	'col-span-2 col-start-2 line-clamp-2 font-mono [overflow-wrap:anywhere] @2xl:col-span-1 @2xl:col-start-auto';
