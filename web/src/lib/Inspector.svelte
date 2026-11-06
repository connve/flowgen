<script lang="ts">
	import { untrack, type Snippet } from 'svelte';
	import IconButton from '$lib/IconButton.svelte';
	import { cubicOut } from 'svelte/easing';
	import { slide } from 'svelte/transition';
	import PaneResizer from '$lib/PaneResizer.svelte';
	import { inspectorWidth as pane } from '$lib/paneSize.svelte';
	import { isMobile } from '$lib/mobile';
	import { sidePaneSlot } from '$lib/sidePaneSlot.svelte';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		/** Names the inspected item, e.g. a tool or a log's task. */
		title: string;
		/** Names the panel for assistive tech. */
		label: string;
		onclose: () => void;
		/** Steps back to the list this item was opened from. */
		onback?: () => void;
		/** Names that list on the back button. */
		backLabel?: string;
		/** A line under the title, e.g. the item's status or path. */
		subtitle?: Snippet;
		/** Buttons before the close button. */
		actions?: Snippet;
		children: Snippet;
	}

	let { title, label, onclose, onback, backLabel, subtitle, actions, children }: Props = $props();

	let width = $derived(`min(${pane.size}px, 50vw)`);

	/** On a phone the inspector covers the page, so it acts as a dialog. */
	const covers = isMobile();
	let panel = $state<HTMLElement | null>(null);
	let closeButton = $state<HTMLButtonElement | null>(null);

	$effect(() => {
		untrack(() => (sidePaneSlot.inspectors += 1));
		return () => untrack(() => (sidePaneSlot.inspectors -= 1));
	});

	$effect(() => {
		if (!covers) return;
		const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
		untrack(() => closeButton?.focus());
		return () => opener?.focus();
	});

	function keepFocus(event: KeyboardEvent) {
		if (!covers || event.key !== 'Tab' || !panel) return;
		const focusable = [
			...panel.querySelectorAll<HTMLElement>('a[href], button:not([disabled]), input, textarea, select, [tabindex="0"]')
		];
		const first = focusable[0];
		const last = focusable[focusable.length - 1];
		if (event.shiftKey && document.activeElement === first) {
			event.preventDefault();
			last?.focus();
		} else if (!event.shiftKey && document.activeElement === last) {
			event.preventDefault();
			first?.focus();
		}
	}
</script>

<aside
	bind:this={panel}
	class="relative flex shrink-0 flex-col border-l border-base-200 bg-base-100 max-md:fixed max-md:inset-y-0 max-md:right-0 max-md:z-40 max-md:w-full! max-md:shadow-xl"
	style:width
	role={covers ? 'dialog' : undefined}
	aria-modal={covers ? 'true' : undefined}
	aria-label={label}
	onkeydown={keepFocus}
	transition:slide={{ axis: 'x', duration: 200, easing: cubicOut }}
>
	<PaneResizer {pane} />
	<div class="flex min-h-0 flex-1 flex-col max-md:w-full!" style:width>
		<div class="flex h-16 shrink-0 items-center gap-1 pr-3 {onback ? 'pl-3' : 'pl-6'}">
			{#if onback}
				<IconButton
					icon="tabler:chevron-left"
					label={backLabel ?? m.common_back()}
					tooltip="right"
					onclick={onback}
				/>
			{/if}
			<div class="min-w-0 flex-1">
				<h2 class="truncate text-[0.9375rem] font-semibold">{title}</h2>
				{#if subtitle}
					<div class="mt-0.5 flex min-w-0 items-center gap-1.5 truncate text-xs text-base-content/50">
						{@render subtitle()}
					</div>
				{/if}
			</div>
			{@render actions?.()}
			<IconButton
				bind:ref={closeButton}
				icon="tabler:x"
				label={m.common_close()}
				tooltip="left"
				onclick={onclose}
			/>
		</div>
		<div class="min-h-0 flex-1 space-y-6 overflow-y-auto px-6 pb-6 text-sm">
			{@render children()}
		</div>
	</div>
</aside>
