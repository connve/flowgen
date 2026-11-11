import { browser } from '$app/environment';

// An EventSource open only while the page is visible.
export function visibleEventSource(url: string, listen: (source: EventSource) => void): () => void {
	if (!browser) return () => {};
	let source: EventSource | null = null;
	const sync = () => {
		if (document.visibilityState === 'hidden') {
			source?.close();
			source = null;
		} else if (source === null) {
			source = new EventSource(url);
			listen(source);
		}
	};
	sync();
	document.addEventListener('visibilitychange', sync);
	return () => {
		document.removeEventListener('visibilitychange', sync);
		source?.close();
		source = null;
	};
}
