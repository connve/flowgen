<script lang="ts">
	import { m } from '$lib/paraglide/messages.js';
	import type { PaneSize } from '$lib/paneSize.svelte';

	let { pane }: { pane: PaneSize } = $props();

	const EDGE = {
		left: 'inset-y-0 -left-1.5 w-3 cursor-col-resize justify-center',
		right: 'inset-y-0 -right-1.5 w-3 cursor-col-resize justify-center',
		top: 'inset-x-0 -top-1.5 h-3 cursor-row-resize items-center'
	};
</script>

<!-- Drag the pane's inner edge, or use the arrow keys, to resize it; Home or a
     double-click restores the default. The hit area is wider than the line. -->
<button
	type="button"
	aria-label={m.pane_resize()}
	class="group absolute z-10 flex focus-visible:outline-none {EDGE[pane.edge]}"
	onpointerdown={pane.start}
	ondblclick={pane.reset}
	onkeydown={pane.keydown}
>
	<span
		class="transition-colors group-hover:bg-primary/60 group-focus-visible:bg-primary {pane.vertical
			? 'h-px w-full'
			: 'h-full w-px'} {pane.dragging ? 'bg-primary' : ''}"
	></span>
</button>
