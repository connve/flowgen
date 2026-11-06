<script lang="ts" generics="T">
	import Icon from '@iconify/svelte';
	import type { Snippet } from 'svelte';
	import type { TreeNode } from '$lib/tree';

	interface Props {
		tree: TreeNode<T>[];
		/** The selected folder, `null` for all. */
		selected: string | null;
		expanded: ReadonlySet<string>;
		allLabel: string;
		allIcon: string;
		/** Shown after the "all" row's label. */
		allCount?: number;
		expandLabel: (folder: string) => string;
		collapseLabel: (folder: string) => string;
		onselect: (folder: string | null) => void;
		ontoggle: (folder: string) => void;
		/** Shown after a folder's name; its item count when unset. */
		trailing?: Snippet<[TreeNode<T>]>;
	}

	let {
		tree,
		selected,
		expanded,
		allLabel,
		allIcon,
		allCount,
		expandLabel,
		collapseLabel,
		onselect,
		ontoggle,
		trailing
	}: Props = $props();

	function row(isSelected: boolean): string {
		return `flex h-9 w-full min-w-0 items-center gap-2 rounded-md pr-2 text-left text-sm transition-colors ${
			isSelected ? 'bg-base-200 font-medium' : 'hover:bg-base-200/60'
		}`;
	}
</script>

<ul class="space-y-px">
	<li>
		<button
			type="button"
			class={row(selected === null)}
			style="padding-left: 1.75rem"
			onclick={() => onselect(null)}
		>
			<Icon icon={allIcon} class="h-4 w-4 shrink-0 opacity-60" />
			<span class="min-w-0 flex-1 truncate">{allLabel}</span>
			{#if allCount !== undefined}
				<span class="shrink-0 text-xs tabular-nums text-base-content/40">{allCount}</span>
			{/if}
		</button>
	</li>
	{#snippet folders(nodes: TreeNode<T>[])}
		{#each nodes.filter((node) => node.isFolder) as node (node.fullPath)}
			{@const isOpen = expanded.has(node.fullPath)}
			{@const canExpand = (node.children ?? []).some((child) => child.isFolder)}
			{@const indent = node.depth * 0.75}
			<li>
				<div class="relative">
					<button
						type="button"
						class={row(selected === node.fullPath)}
						style="padding-left: {indent + 1.75}rem"
						onclick={() => onselect(node.fullPath)}
					>
						<Icon
							icon={isOpen && canExpand ? 'tabler:folder-open' : 'tabler:folder'}
							class="h-4 w-4 shrink-0 opacity-60"
						/>
						<span class="min-w-0 flex-1 truncate">{node.name}</span>
						{#if trailing}
							{@render trailing(node)}
						{:else if node.fileCount !== undefined}
							<span class="shrink-0 text-xs tabular-nums text-base-content/40">{node.fileCount}</span>
						{/if}
					</button>
					{#if canExpand}
						<button
							type="button"
							class="absolute top-1/2 flex h-5 w-5 -translate-y-1/2 items-center justify-center rounded text-base-content/50 hover:bg-base-300/50 hover:text-base-content"
							style="left: {indent + 0.375}rem"
							aria-label={isOpen ? collapseLabel(node.name) : expandLabel(node.name)}
							aria-expanded={isOpen}
							onclick={() => ontoggle(node.fullPath)}
						>
							<Icon
								icon="tabler:chevron-right"
								class="h-3.5 w-3.5 transition-transform {isOpen ? 'rotate-90' : ''}"
							/>
						</button>
					{/if}
				</div>
				{#if isOpen && canExpand && node.children}
					<ul class="space-y-px">
						{@render folders(node.children)}
					</ul>
				{/if}
			</li>
		{/each}
	{/snippet}
	{@render folders(tree)}
</ul>
