// Width of a resizable side pane, remembered per pane in this browser.

import { browser } from '$app/environment';

const DEFAULT_WIDTH = 256;
const MIN_WIDTH = 200;
const MAX_WIDTH = 520;
const KEY_STEP = 16;

function clamp(width: number): number {
	return Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, Math.round(width)));
}

export class PaneWidth {
	width = $state(DEFAULT_WIDTH);
	dragging = $state(false);
	#key: string;

	constructor(key: string) {
		this.#key = `flowgen-${key}-pane-width`;
		if (!browser) return;
		try {
			const saved = Number(localStorage.getItem(this.#key));
			if (saved > 0) this.width = clamp(saved);
		} catch {
			// Storage can be unavailable; the default width still works.
		}
	}

	#save() {
		try {
			localStorage.setItem(this.#key, String(this.width));
		} catch {
			// Not remembered, but the pane keeps its width for this visit.
		}
	}

	start = (event: PointerEvent) => {
		const handle = event.currentTarget as HTMLElement;
		const startX = event.clientX;
		const startWidth = this.width;
		handle.setPointerCapture(event.pointerId);
		this.dragging = true;
		const move = (e: PointerEvent) => {
			this.width = clamp(startWidth + e.clientX - startX);
		};
		const end = () => {
			this.dragging = false;
			handle.removeEventListener('pointermove', move);
			handle.removeEventListener('pointerup', end);
			handle.removeEventListener('pointercancel', end);
			this.#save();
		};
		handle.addEventListener('pointermove', move);
		handle.addEventListener('pointerup', end);
		handle.addEventListener('pointercancel', end);
	};

	reset = () => {
		this.width = DEFAULT_WIDTH;
		this.#save();
	};

	keydown = (event: KeyboardEvent) => {
		if (event.key === 'Home') {
			event.preventDefault();
			this.reset();
			return;
		}
		const step = { ArrowLeft: -KEY_STEP, ArrowRight: KEY_STEP }[event.key];
		if (step === undefined) return;
		event.preventDefault();
		this.width = clamp(this.width + step);
		this.#save();
	};
}
