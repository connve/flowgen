<script lang="ts">
	import Icon from '@iconify/svelte';
	import { base } from '$app/paths';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		/** Names the whole list, e.g. "All flows". */
		allLabel: string;
		/** The open folder, shown as a breadcrumb after `allLabel`. */
		folder: string | null;
		onselect: (folder: string | null) => void;
		/** The number of items shown, e.g. "26 flows". */
		count: string;
		/** Pending changes, or null when authoring is off. */
		pending: number | null;
		pendingLabel: string;
		newHref: string;
	}

	let { allLabel, folder, onselect, count, pending, pendingLabel, newHref }: Props = $props();

	let segments = $derived(folder ? folder.split('/') : []);
</script>

<!-- Lives in an `@container` column, so it folds to what fits beside an inspector. -->
<div class="flex min-h-8 items-center justify-between gap-3">
	<div class="flex min-w-0 items-baseline gap-1.5 whitespace-nowrap text-[0.9375rem] font-semibold">
		{#if folder}
			<button
				type="button"
				class="shrink-0 text-base-content/50 transition-colors hover:text-base-content"
				onclick={() => onselect(null)}>{allLabel}</button
			>
			{#each segments as segment, i}
				<span class="shrink-0 opacity-40">/</span>
				{#if i < segments.length - 1}
					<button
						type="button"
						class="shrink-0 text-base-content/50 transition-colors hover:text-base-content"
						onclick={() => onselect(segments.slice(0, i + 1).join('/'))}>{segment}</button
					>
				{:else}
					<span class="truncate">{segment}</span>
				{/if}
			{/each}
		{:else}
			<span class="truncate">{allLabel}</span>
		{/if}
		<span class="shrink-0 text-xs font-normal opacity-50 @max-sm:hidden">· {count}</span>
	</div>
	{#if pending !== null}
		<div class="flex shrink-0 items-center gap-2">
			<a
				href="{base}/changes"
				class="btn btn-ghost btn-sm gap-1.5 {pending > 0 ? 'text-attention' : ''}"
				aria-label={pendingLabel}
			>
				<Icon icon="tabler:git-pull-request" class="h-4 w-4" />
				<span class="@max-2xl:hidden">{pendingLabel}</span>
				<span class="tabular-nums @2xl:hidden">{pending}</span>
			</a>
			<a href={newHref} class="btn btn-primary btn-sm gap-1.5">
				<Icon icon="tabler:plus" class="h-4 w-4" />
				{m.common_new()}
			</a>
		</div>
	{/if}
</div>
