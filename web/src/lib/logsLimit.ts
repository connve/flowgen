// Shared constants for any UI that lets an operator cap how many log
// records to fetch: the global /logs viewer and the per-flow Activity
// panel. Each view keeps its own limit value — only the bounds are
// shared, so raising the limit in one doesn't affect the other.

export const LOGS_LIMIT_DEFAULT = 1000;
export const LOGS_LIMIT_MAX = 10000;

export function clampLogsLimit(next: number): number {
	return Math.min(Math.max(1, Math.floor(next)), LOGS_LIMIT_MAX);
}
