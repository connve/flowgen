export const SECOND = 1000;
export const MINUTE = 60 * SECOND;
export const HOUR = 60 * MINUTE;
export const DAY = 24 * HOUR;
export const JUST_NOW = 2 * SECOND;

export function formatRelative(ts: number, now: number = Date.now()): string {
	const delta = Math.max(0, now - ts);
	if (delta < JUST_NOW) return 'just now';
	if (delta < MINUTE) return `${Math.floor(delta / SECOND)}s ago`;
	if (delta < HOUR) return `${Math.floor(delta / MINUTE)}m ago`;
	if (delta < DAY) return `${Math.floor(delta / HOUR)}h ago`;
	return new Date(ts).toLocaleDateString();
}

export function formatAbsolute(ts: number): string {
	const d = new Date(ts);
	const pad = (n: number) => n.toString().padStart(2, '0');
	return (
		`${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
		`${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
	);
}
