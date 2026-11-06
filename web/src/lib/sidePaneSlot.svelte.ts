import type { Snippet } from 'svelte';

interface SidePaneSlot {
	/** The page's side pane, rendered by the layout beside the menu so it reaches the top. */
	pane: Snippet | null;
	/** Opens a collapsed pane from the top bar on a phone, where its rail is hidden. */
	expand: { label: string; onclick: () => void } | null;
	/** How many inspectors are open. */
	inspectors: number;
}

export const sidePaneSlot = $state<SidePaneSlot>({ pane: null, expand: null, inspectors: 0 });
