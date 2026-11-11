import type { IconifyIcon } from '@iconify/svelte';

// Tabler Icons (MIT), bundled so they need no icon request.
function tabler(d: string): IconifyIcon {
	return {
		body: `<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="${d}"/>`,
		width: 24,
		height: 24
	};
}

export const search = tabler('M3 10a7 7 0 1 0 14 0a7 7 0 1 0-14 0m18 11l-6-6');
export const menu = tabler('M4 6h16M4 12h16M4 18h16');
export const chevronRight = tabler('m9 6l6 6l-6 6');
export const close = tabler('M18 6L6 18M6 6l12 12');
export const hash = tabler('M5 9h14M5 15h14M11 4L7 20M17 4l-4 16');
export const fileText = tabler(
	'M14 3v4a1 1 0 0 0 1 1h4M17 21H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h7l5 5v11a2 2 0 0 1-2 2M9 9h1m-1 4h6m-6 4h6'
);
export const github = tabler(
	'M9 19c-4.3 1.4-4.3-2.5-6-3m12 5v-3.5c0-1 .1-1.4-.5-2c2.8-.3 5.5-1.4 5.5-6a4.6 4.6 0 0 0-1.3-3.2a4.2 4.2 0 0 0-.1-3.2s-1.1-.3-3.5 1.3a12.3 12.3 0 0 0-6.2 0C6.5 2.8 5.4 3.1 5.4 3.1a4.2 4.2 0 0 0-.1 3.2A4.6 4.6 0 0 0 4 9.5c0 4.6 2.7 5.7 5.5 6c-.6.6-.6 1.2-.5 2V21'
);
