// Module-level state for the /logs page, so it survives SPA navigation
// (switching tabs and back) but resets on a full page reload — same
// lifetime as activityStore's buckets/metricsByFlow. A $state declared
// inside +page.svelte would reset on every remount instead.

import type { LogRecord } from '$lib/api';
import { LOGS_LIMIT_DEFAULT } from '$lib/logsLimit';

let logsLimit = $state(LOGS_LIMIT_DEFAULT);

// Default filters: warn + error only. Info/debug/trace hidden until the
// operator explicitly widens the filter — matches how the Flows page
// leans on error/warning counters over ambient info noise.
export const logsLevels = $state<Record<LogRecord['level'], boolean>>({
	info: false,
	warn: true,
	error: true,
	debug: false,
	trace: false,
});

export function getLogsLimit(): number {
	return logsLimit;
}

export function setLogsLimit(next: number) {
	logsLimit = next;
}
