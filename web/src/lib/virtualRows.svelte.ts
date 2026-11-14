import type { Attachment } from 'svelte/attachments';

/** A virtualized list whose rows measure their own height, so a row may wrap. */
export class VirtualRows<T extends object> {
	scrollTop = $state(0);
	viewportHeight = $state(0);

	#items: () => T[];
	#estimate: number;
	#overscan: number;
	#heights = new WeakMap<T, number>();
	#nodes = new WeakMap<Element, T>();
	#measured = $state(0);
	#observer: ResizeObserver | null = null;

	constructor(items: () => T[], estimate: number, overscan = 6) {
		this.#items = items;
		this.#estimate = estimate;
		this.#overscan = overscan;
	}

	#offsets = $derived.by(() => {
		void this.#measured;
		const list = this.#items();
		const out = new Array<number>(list.length + 1);
		out[0] = 0;
		for (let i = 0; i < list.length; i++) {
			out[i + 1] = out[i] + (this.#heights.get(list[i]) ?? this.#estimate);
		}
		return out;
	});

	start = $derived.by(() => Math.max(0, this.#indexAt(this.scrollTop) - this.#overscan));
	end = $derived.by(() =>
		Math.min(
			this.#items().length,
			this.#indexAt(this.scrollTop + this.viewportHeight) + 1 + this.#overscan
		)
	);
	visible = $derived.by(() => this.#items().slice(this.start, this.end));
	topPad = $derived.by(() => this.#offsets[this.start] ?? 0);
	bottomPad = $derived.by(() =>
		Math.max(0, (this.#offsets.at(-1) ?? 0) - (this.#offsets[this.end] ?? 0))
	);

	/** The row at `y` pixels from the top. */
	#indexAt(y: number): number {
		const offsets = this.#offsets;
		let lo = 0;
		let hi = offsets.length - 1;
		while (lo < hi) {
			const mid = (lo + hi + 1) >> 1;
			if (offsets[mid] <= y) lo = mid;
			else hi = mid - 1;
		}
		return lo;
	}

	/** Records the rendered height of `item`'s row. */
	measure(item: T): Attachment<HTMLElement> {
		return (node) => {
			this.#nodes.set(node, item);
			this.#observe().observe(node);
			return () => this.#observer?.unobserve(node);
		};
	}

	#observe(): ResizeObserver {
		this.#observer ??= new ResizeObserver((entries) => {
			let changed = false;
			for (const entry of entries) {
				const item = this.#nodes.get(entry.target);
				const height = (entry.target as HTMLElement).offsetHeight;
				if (!item || height === 0 || this.#heights.get(item) === height) continue;
				this.#heights.set(item, height);
				changed = true;
			}
			if (changed) this.#measured += 1;
		});
		return this.#observer;
	}
}
