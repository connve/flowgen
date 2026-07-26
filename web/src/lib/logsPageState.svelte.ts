// Module-level state for the /logs page, so it survives SPA navigation
// (switching tabs and back) but resets on a full page reload — same
// lifetime as activityStore's buckets/metricsByFlow. A $state declared
// inside +page.svelte would reset on every remount instead.

import { LOGS_LIMIT_DEFAULT } from '$lib/logsLimit';

let logsLimit = $state(LOGS_LIMIT_DEFAULT);

export function getLogsLimit(): number {
	return logsLimit;
}

export function setLogsLimit(next: number) {
	logsLimit = next;
}
