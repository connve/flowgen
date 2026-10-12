import { m } from '$lib/paraglide/messages.js';
import { getLocale } from '$lib/paraglide/runtime.js';

export const SECOND = 1000;
export const MINUTE = 60 * SECOND;
export const HOUR = 60 * MINUTE;
export const DAY = 24 * HOUR;
export const JUST_NOW = 2 * SECOND;

let relativeFormat: { locale: string; format: Intl.RelativeTimeFormat } | null = null;

function relativeFormatter(): Intl.RelativeTimeFormat {
	const locale = getLocale();
	if (relativeFormat?.locale !== locale) {
		relativeFormat = { locale, format: new Intl.RelativeTimeFormat(locale, { style: 'narrow' }) };
	}
	return relativeFormat.format;
}

export function formatRelative(ts: number, now: number = Date.now()): string {
	const delta = Math.max(0, now - ts);
	if (delta < JUST_NOW) return m.time_just_now();
	const relative = relativeFormatter();
	if (delta < MINUTE) return relative.format(-Math.floor(delta / SECOND), 'second');
	if (delta < HOUR) return relative.format(-Math.floor(delta / MINUTE), 'minute');
	if (delta < DAY) return relative.format(-Math.floor(delta / HOUR), 'hour');
	return new Date(ts).toLocaleDateString(getLocale());
}

export function formatAbsolute(ts: number): string {
	const d = new Date(ts);
	const pad = (n: number) => n.toString().padStart(2, '0');
	return (
		`${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
		`${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
	);
}
