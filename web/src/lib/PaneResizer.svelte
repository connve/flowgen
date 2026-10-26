<script lang="ts">
	import { m } from '$lib/paraglide/messages.js';
	import type { PaneWidth } from '$lib/paneWidth.svelte';

	let { pane }: { pane: PaneWidth } = $props();
</script>

<!-- Drag the pane's right edge, or use the arrow keys, to resize it; Home or a
     double-click restores the default. The hit area is wider than the line. -->
<button
	type="button"
	aria-label={m.pane_resize()}
	class="group absolute inset-y-0 -right-1.5 z-10 flex w-3 cursor-col-resize justify-center focus-visible:outline-none"
	onpointerdown={pane.start}
	ondblclick={pane.reset}
	onkeydown={pane.keydown}
>
	<span
		class="h-full w-px transition-colors group-hover:bg-primary/60 group-focus-visible:bg-primary {pane.dragging
			? 'bg-primary'
			: ''}"
	></span>
</button>
