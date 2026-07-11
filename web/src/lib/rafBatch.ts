// Coalesces high-frequency callbacks into one flush per animation frame.
// SSE bursts (~1000/s) can overwhelm Svelte reactivity if every event
// triggers a re-render — batching amortizes the state churn to ~60Hz.
export function rafBatch<T>(flush: (batch: T[]) => void): (item: T) => void {
	let queue: T[] = [];
	let scheduled = false;
	return (item: T) => {
		queue.push(item);
		if (scheduled) return;
		scheduled = true;
		requestAnimationFrame(() => {
			const batch = queue;
			queue = [];
			scheduled = false;
			flush(batch);
		});
	};
}
