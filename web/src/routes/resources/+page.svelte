<script lang="ts">
	import { onMount } from 'svelte';
	import { SvelteSet } from 'svelte/reactivity';
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import Badge from '$lib/Badge.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import Icon from '@iconify/svelte';
	import { apiUrl } from '$lib/api';

	interface Resource {
		key: string;
		extension: string | null;
		size: number | null;
	}

	interface ResourceContent {
		key: string;
		extension: string | null;
		content: string;
	}

	// Node in the collapsible resource tree — either a folder (with children
	// nested one level deep) or a leaf file.
	interface TreeNode {
		name: string;
		fullPath: string;
		depth: number;
		isFolder: boolean;
		resource?: Resource;
		children?: TreeNode[];
		fileCount?: number;
	}

	let resources = $state<Resource[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let search = $state('');

	// Set of folder paths currently expanded in the tree. Persists across
	// unrelated state changes so navigation feels stable.
	let expanded = $state(new SvelteSet<string>());

	let selected = $state<string | null>(null);
	let selectedContent = $state<ResourceContent | null>(null);
	let selectedLoading = $state(false);
	let selectedError = $state<string | null>(null);

	onMount(async () => {
		try {
			const response = await fetch(apiUrl('api/resources'));
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			resources = await response.json();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load resources';
		} finally {
			loading = false;
		}
	});

	// When searching, we drop folder navigation and show flat matches so users
	// can find things regardless of directory.
	let searchActive = $derived(search.trim().length > 0);

	let flatMatches = $derived.by(() => {
		if (!searchActive) return [];
		const term = search.toLowerCase();
		return resources.filter((r) => r.key.toLowerCase().includes(term));
	});

	// Builds the full tree once from the flat resource list. Renders as
	// nested `<ul>`s that collapse/expand purely client-side.
	function buildTree(items: Resource[]): TreeNode[] {
		const root: TreeNode = { name: '', fullPath: '', depth: -1, isFolder: true, children: [] };
		for (const r of items) {
			const parts = r.key.split('/');
			let cursor = root;
			for (let i = 0; i < parts.length; i++) {
				const name = parts[i];
				const isLeaf = i === parts.length - 1;
				const fullPath = parts.slice(0, i + 1).join('/');
				let child = cursor.children!.find((c) => c.name === name);
				if (!child) {
					child = {
						name,
						fullPath,
						depth: i,
						isFolder: !isLeaf,
						children: isLeaf ? undefined : [],
						resource: isLeaf ? r : undefined
					};
					cursor.children!.push(child);
				}
				cursor = child;
			}
		}
		// Sort each level: folders first, then files, alphabetically.
		function sortRec(node: TreeNode) {
			if (!node.children) return;
			node.children.sort((a, b) => {
				if (a.isFolder !== b.isFolder) return a.isFolder ? -1 : 1;
				return a.name.localeCompare(b.name);
			});
			for (const c of node.children) sortRec(c);
		}
		sortRec(root);
		// Fill in fileCount for folders (recursive descendant leaves).
		function countLeaves(node: TreeNode): number {
			if (!node.isFolder) return 1;
			let n = 0;
			for (const c of node.children ?? []) n += countLeaves(c);
			node.fileCount = n;
			return n;
		}
		countLeaves(root);
		return root.children ?? [];
	}

	let tree = $derived(buildTree(resources));

	function toggle(path: string) {
		if (expanded.has(path)) expanded.delete(path);
		else expanded.add(path);
	}

	// Same anti-flash pattern as the flows page: only surface the modal once
	// we actually have content, keep a cache so repeats paint instantly, and
	// tag each fetch with `pending` so stale responses can't overwrite the
	// current selection.
	const contentCache = new Map<string, ResourceContent>();
	let pendingResource: string | null = null;

	async function openResource(key: string) {
		pendingResource = key;
		const cached = contentCache.get(key);
		if (cached) {
			selected = key;
			selectedContent = cached;
			selectedError = null;
			selectedLoading = false;
		} else {
			selectedLoading = true;
		}
		try {
			const response = await fetch(apiUrl(`api/resources/${key}`));
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			const body: ResourceContent = await response.json();
			contentCache.set(key, body);
			if (pendingResource !== key) return;
			selected = key;
			selectedContent = body;
			selectedError = null;
		} catch (err) {
			if (pendingResource !== key) return;
			selected = key;
			selectedError = err instanceof Error ? err.message : 'Failed to load resource';
		} finally {
			if (pendingResource === key) selectedLoading = false;
		}
	}

	function closeResource() {
		selected = null;
		selectedContent = null;
		selectedError = null;
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape' && selected) closeResource();
	}

	function formatSize(bytes: number | null): string {
		if (bytes === null) return '—';
		if (bytes < 1024) return `${bytes} B`;
		if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} kB`;
		return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
	}
</script>

<svelte:head>
	<title>Flowgen | Resources Overview</title>
</svelte:head>

<svelte:window on:keydown={onKeydown} />

<section class="p-6">
	<div class="mb-4 flex items-center justify-end">
		<label
			class="input input-sm flex items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary"
		>
			<svg
				class="h-4 w-4 opacity-50"
				viewBox="0 0 24 24"
				fill="none"
				stroke="currentColor"
				stroke-width="2"
			>
				<circle cx="11" cy="11" r="8" />
				<path d="M21 21l-4.35-4.35" />
			</svg>
			<input type="text" placeholder="Search resources..." bind:value={search} />
			{#if search}
				<button
					type="button"
					class="opacity-50 hover:opacity-100"
					aria-label="Clear search"
					onclick={() => (search = '')}
				>
					<Icon icon="tabler:x" class="h-6 w-6" />
				</button>
			{/if}
		</label>
	</div>

	{#if loading}
		<div class="flex justify-center py-12">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if error}
		<div class="alert alert-error" role="alert">
			<span>Failed to load resources: {error}</span>
		</div>
	{:else if searchActive}
		{#if flatMatches.length === 0}
			<div
				class="rounded-lg border border-base-200 bg-base-100 p-8 text-center text-sm opacity-70"
			>
				No matches for "{search}".
			</div>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-base-200 bg-base-100">
				<table class="table table-sm w-full bg-base-100">
					<thead class="bg-base-100 text-xs uppercase tracking-wide opacity-60">
						<tr>
							<th>Key</th>
							<th>Type</th>
							<th class="text-right">Size</th>
						</tr>
					</thead>
					<tbody>
						{#each flatMatches as resource (resource.key)}
							<tr
								class="hover cursor-pointer"
								onclick={() => openResource(resource.key)}
								ondblclick={() =>
									goto(`${base}/resources/${resource.key.split('/').map(encodeURIComponent).join('/')}`)}
								onkeydown={(e) => {
									if (e.key === 'Enter' || e.key === ' ') openResource(resource.key);
								}}
								tabindex="0"
								role="button"
							>
								<td class="font-medium">{resource.key}</td>
								<td>
									{#if resource.extension}
										<Badge>{resource.extension}</Badge>
									{:else}
										<span class="opacity-50">—</span>
									{/if}
								</td>
								<td class="whitespace-nowrap text-right text-sm opacity-70">
									{formatSize(resource.size)}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{:else if tree.length === 0}
		<div class="rounded-lg border border-base-200 bg-base-100 p-8 text-center text-sm opacity-70">
			No resources
		</div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-base-200 bg-base-100">
			<table class="table table-sm w-full bg-base-100">
				<thead class="bg-base-100 text-xs uppercase tracking-wide opacity-60">
					<tr>
						<th>Name</th>
						<th>Type</th>
						<th class="text-right">Size</th>
					</tr>
				</thead>
				<tbody>
					{#snippet renderTree(nodes: TreeNode[])}
						{#each nodes as node (node.fullPath)}
							{#if node.isFolder}
								{@const isOpen = expanded.has(node.fullPath)}
								<tr
									class="hover cursor-pointer"
									onclick={() => toggle(node.fullPath)}
									onkeydown={(e) => {
										if (e.key === 'Enter' || e.key === ' ') toggle(node.fullPath);
									}}
									tabindex="0"
									role="button"
								>
									<td>
										<div
											class="flex items-center gap-1.5 font-medium"
											style="padding-left: {node.depth * 1.25}rem"
										>
											{#if isOpen}
												<Icon icon="tabler:chevron-down" class="h-4 w-4 opacity-70" />
												<Icon icon="tabler:folder-open" class="h-4 w-4 opacity-70" />
											{:else}
												<Icon icon="tabler:chevron-right" class="h-4 w-4 opacity-70" />
												<Icon icon="tabler:folder" class="h-4 w-4 opacity-70" />
											{/if}
											<span>{node.name}</span>
										</div>
									</td>
									<td class="text-xs opacity-60">folder</td>
									<td class="whitespace-nowrap text-right text-sm opacity-70">
										{node.fileCount}
										{node.fileCount === 1 ? 'item' : 'items'}
									</td>
								</tr>
								{#if isOpen && node.children}
									{@render renderTree(node.children)}
								{/if}
							{:else if node.resource}
								<tr
									class="hover cursor-pointer"
									onclick={() => node.resource && openResource(node.resource.key)}
									ondblclick={() =>
										node.resource &&
										goto(
											`${base}/resources/${node.resource.key.split('/').map(encodeURIComponent).join('/')}`
										)}
									onkeydown={(e) => {
										if ((e.key === 'Enter' || e.key === ' ') && node.resource)
											openResource(node.resource.key);
									}}
									tabindex="0"
									role="button"
								>
									<td>
										<div
											class="flex items-center gap-1.5 font-medium"
											style="padding-left: {node.depth * 1.25 + 0.75}rem"
										>
											<Icon icon="tabler:file" class="h-4 w-4 opacity-70" />
											<span>{node.name}</span>
										</div>
									</td>
									<td>
										{#if node.resource.extension}
											<Badge>{node.resource.extension}</Badge>
										{:else}
											<span class="opacity-50">—</span>
										{/if}
								</td>
								<td class="whitespace-nowrap text-right text-sm opacity-70">
									{formatSize(node.resource.size)}
								</td>
							</tr>
						{/if}
					{/each}
					{/snippet}
					{@render renderTree(tree)}
				</tbody>
			</table>
		</div>
	{/if}
</section>

{#if selected}
	<div
		class="fixed inset-0 z-40 flex items-center justify-center bg-black/40 p-4"
		role="dialog"
		aria-modal="true"
		aria-label="Resource viewer"
		onclick={(e) => {
			if (e.target === e.currentTarget) closeResource();
		}}
		onkeydown={(e) => {
			if (e.key === 'Escape') closeResource();
		}}
		tabindex="-1"
	>
		<div
			class="flex max-h-[90vh] w-full max-w-4xl flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-lg"
		>
			<div class="flex items-center justify-between border-b border-base-200 px-4 py-2">
				<div class="flex items-center gap-2">
					<span class="font-mono text-sm font-medium leading-none">{selected}</span>
					{#if selectedContent?.extension}
						<Badge>{selectedContent.extension}</Badge>
					{/if}
				</div>
				<div class="flex items-center gap-1">
					{#if selected}
						<div class="tooltip tooltip-left" data-tip="Open full page">
							<a
								href="{base}/resources/{selected.split('/').map(encodeURIComponent).join('/')}"
								class="btn btn-ghost btn-sm btn-circle"
								aria-label="Open full page"
							>
								<Icon icon="tabler:external-link" class="h-6 w-6" />
							</a>
						</div>
					{/if}
					<div class="tooltip tooltip-left" data-tip="Close">
						<button
							type="button"
							class="btn btn-ghost btn-sm btn-circle"
							aria-label="Close"
							onclick={closeResource}
						>
							<Icon icon="tabler:x" class="h-6 w-6" />
						</button>
					</div>
				</div>
			</div>
			<div class="flex items-center justify-end border-b border-base-200 bg-base-100 px-4 py-1">
				<CopyButton text={selectedContent?.content} label="Copy" />
			</div>
			<div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-base-200">
				{#if selectedLoading}
					<div class="flex items-center justify-center py-12">
						<span class="loading loading-spinner loading-md text-primary"></span>
					</div>
				{:else if selectedError}
					<div class="alert alert-error m-4" role="alert">
						<span>{selectedError}</span>
					</div>
				{:else if selectedContent}
					<div class="min-h-0 flex-1 overflow-auto">
						<ResourceViewer
							content={selectedContent.content}
							extension={selectedContent.extension}
						/>
					</div>
				{/if}
			</div>
		</div>
	</div>
{/if}
