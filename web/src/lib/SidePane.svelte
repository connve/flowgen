<script lang="ts">
	import Icon from '@iconify/svelte';
	import { untrack, type Snippet } from 'svelte';
	import PaneResizer from '$lib/PaneResizer.svelte';
	import { sidePaneWidth as pane } from '$lib/paneSize.svelte';
	import { PANE_OVER_PAGE } from '$lib/mobile';
	import { sidePaneSlot } from '$lib/sidePaneSlot.svelte';

	interface Props {
		open: boolean;
		/** Names the pane above its content, e.g. "Folders". */
		title: string;
		collapseLabel: string;
		expandLabel: string;
		ontoggle: () => void;
		/** Closes the pane from the backdrop on mobile. */
		onclose: () => void;
		children: Snippet;
		/** Actions kept on the narrow rail while the pane is collapsed. */
		rail?: Snippet;
	}

	let { open, title, collapseLabel, expandLabel, ontoggle, onclose, children, rail }: Props =
		$props();

	/** Below this window width an open inspector folds the pane. */
	const ROOMY_WIDTH = 1280;

	let windowWidth = $state(ROOMY_WIDTH);
	let unfolded = $state(false);
	let folded = $derived(
		open && !unfolded && sidePaneSlot.inspectors > 0 && windowWidth < ROOMY_WIDTH
	);
	let shown = $derived(open && !folded);

	$effect(() => {
		if (sidePaneSlot.inspectors === 0) unfolded = false;
	});

	function toggle() {
		if (folded) unfolded = true;
		else ontoggle();
	}

	$effect(() => {
		sidePaneSlot.pane = aside;
		return () => {
			if (untrack(() => sidePaneSlot.pane) === aside) sidePaneSlot.pane = null;
		};
	});

	$effect(() => {
		sidePaneSlot.expand = shown ? null : { label: expandLabel, onclick: toggle };
		return () => {
			if (untrack(() => sidePaneSlot.expand?.onclick) === toggle) sidePaneSlot.expand = null;
		};
	});
</script>

<svelte:window bind:innerWidth={windowWidth} />

{#if shown}
	<div class="fixed inset-0 z-20 bg-black/30 md:hidden" aria-hidden="true" onclick={onclose}></div>
{/if}

{#snippet aside()}
	<aside
		class="relative flex h-full shrink-0 flex-col border-r border-base-200 bg-base-100 {pane.dragging
			? ''
			: 'transition-[width] duration-200 ease-out'} {shown ? PANE_OVER_PAGE : 'w-14 max-md:hidden'}"
		style:width={shown ? `min(${pane.size}px, 35vw)` : undefined}
	>
		{#if shown}
			<PaneResizer {pane} />
		{/if}
		<div class="flex h-16 shrink-0 items-center {shown ? 'justify-between pl-6 pr-3' : 'justify-center'}">
			{#if shown}
				<span class="truncate text-[0.9375rem] font-semibold">{title}</span>
			{/if}
			<div class="tooltip tooltip-right" data-tip={shown ? collapseLabel : expandLabel}>
				<button
					type="button"
					aria-label={shown ? collapseLabel : expandLabel}
					aria-expanded={shown}
					class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/50 transition-colors hover:bg-base-200 hover:text-base-content"
					onclick={toggle}
				>
					<Icon
						icon={shown ? 'tabler:layout-sidebar-left-collapse' : 'tabler:layout-sidebar-left-expand'}
						class="h-5 w-5"
					/>
				</button>
			</div>
		</div>
		{#if shown}
			<div class="flex min-h-0 flex-1 flex-col">
				{@render children()}
			</div>
		{:else if rail}
			<div class="flex flex-col items-center gap-1">
				{@render rail()}
			</div>
		{/if}
	</aside>
{/snippet}
