<script lang="ts">
	import { onMount } from 'svelte';
	import { SvelteSet } from 'svelte/reactivity';
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import Badge from '$lib/Badge.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import Icon from '@iconify/svelte';
	import { apiUrl, type ResourceSummary as Resource, type ResourceContent } from '$lib/api';
	import { buildTree, type TreeNode } from '$lib/tree';

	let resources = $state<Resource[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let search = $state('');

	// Folder pane state — mirrors the Flows page: collapsible sidebar with
	// per-folder expand/collapse, all persisted in localStorage.
	let expandedFolders = $state(new SvelteSet<string>());
	let selectedFolder = $state<string | null>(null);
	let foldersPaneOpen = $state(true);

	let selected = $state<string | null>(null);
	let selectedContent = $state<ResourceContent | null>(null);
	let selectedLoading = $state(false);
	let selectedError = $state<string | null>(null);

	// Modal breadcrumb parts derived from `selected`.
	let selectedSegments = $derived(selected ? selected.split('/') : []);
	let selectedFolderSegments = $derived(selectedSegments.slice(0, -1));
	let selectedLeaf = $derived(selectedSegments[selectedSegments.length - 1] ?? '');

	onMount(async () => {
		const pane = localStorage.getItem('flowgen-resources-pane');
		if (pane !== null) foldersPaneOpen = pane === '1';
		const exp = localStorage.getItem('flowgen-resources-expanded');
		if (exp) {
			try {
				for (const p of JSON.parse(exp) as string[]) expandedFolders.add(p);
			} catch {
				// ignore corrupt state
			}
		}

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

	function toggleFolder(path: string) {
		if (expandedFolders.has(path)) expandedFolders.delete(path);
		else expandedFolders.add(path);
		localStorage.setItem(
			'flowgen-resources-expanded',
			JSON.stringify(Array.from(expandedFolders)),
		);
	}

	function selectFolder(path: string | null) {
		selectedFolder = path;
	}

	function toggleFoldersPane() {
		foldersPaneOpen = !foldersPaneOpen;
		localStorage.setItem('flowgen-resources-pane', foldersPaneOpen ? '1' : '0');
	}

	function hasSubfolders(node: TreeNode<Resource>): boolean {
		return (node.children ?? []).some((c) => c.isFolder);
	}

	function encodePath(path: string): string {
		return path.split('/').map(encodeURIComponent).join('/');
	}

	let searchActive = $derived(search.trim().length > 0);
	let sidebarTree = $derived(buildTree<Resource>(resources, (r) => r.key));

	let filteredResources = $derived.by(() => {
		if (!searchActive) return resources;
		const term = search.toLowerCase();
		return resources.filter((r) => r.key.toLowerCase().includes(term));
	});

	let visibleResources = $derived.by(() => {
		if (searchActive || selectedFolder === null) return filteredResources;
		const prefix = selectedFolder + '/';
		return filteredResources.filter((r) => r.key.startsWith(prefix));
	});

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
	<title>Resources Overview | Flowgen</title>
</svelte:head>

<svelte:window on:keydown={onKeydown} />

<section class="flex h-[calc(100vh-4rem)]">
	<aside
		class="flex shrink-0 flex-col border-r border-base-200 bg-base-100 transition-[width] duration-200 ease-out {foldersPaneOpen
			? 'w-64'
			: 'w-16'}"
	>
		{#if !foldersPaneOpen}
			<div class="flex flex-1 flex-col items-center py-2">
				<button
					type="button"
					aria-label="Expand folders"
					title="Folders"
					class="relative flex h-10 w-10 items-center justify-center rounded-md bg-base-200 text-primary transition-colors hover:bg-base-200"
					onclick={toggleFoldersPane}
				>
					<span class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"></span>
					<Icon icon="tabler:layout-list" class="h-5 w-5 shrink-0" />
				</button>
			</div>
		{:else}
			<div class="flex-1 overflow-y-auto px-3 py-2">
				<ul class="space-y-0.5 text-sm">
					<li>
						<button
							type="button"
							class="relative flex w-full items-center gap-1.5 h-10 rounded-md px-2 text-left transition-colors {selectedFolder === null
								? 'bg-base-200 font-medium text-primary'
								: 'hover:bg-base-200'}"
							onclick={() => selectFolder(null)}
						>
							{#if selectedFolder === null}
								<span class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"></span>
							{/if}
							<Icon icon="tabler:layout-list" class="h-5 w-5 shrink-0 opacity-70" />
							<span>All resources</span>
							<span class="ml-auto text-xs opacity-50">{resources.length}</span>
						</button>
					</li>
					{#snippet sidebarNodes(nodes: TreeNode<Resource>[])}
						{#each nodes as node (node.fullPath)}
							{#if node.isFolder}
								{@const isOpen = expandedFolders.has(node.fullPath)}
								{@const isSelected = selectedFolder === node.fullPath}
								{@const canExpand = hasSubfolders(node)}
								<li>
									<div class="flex items-center gap-0.5">
										{#if canExpand}
											<button
												type="button"
												class="flex h-6 w-6 shrink-0 items-center justify-center rounded hover:bg-base-200"
												aria-label={isOpen ? 'Collapse' : 'Expand'}
												onclick={() => toggleFolder(node.fullPath)}
											>
												<Icon
													icon={isOpen ? 'tabler:chevron-down' : 'tabler:chevron-right'}
													class="h-3.5 w-3.5 opacity-70"
												/>
											</button>
										{:else}
											<span class="h-6 w-6 shrink-0"></span>
										{/if}
										<button
											type="button"
											class="relative flex flex-1 items-center gap-1.5 h-10 rounded-md px-2 text-left transition-colors {isSelected
												? 'bg-base-200 font-medium text-primary'
												: 'hover:bg-base-200'}"
											style="padding-left: {node.depth * 0.75 + 0.375}rem"
											onclick={() => selectFolder(node.fullPath)}
										>
											{#if isSelected}
												<span class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"></span>
											{/if}
											<Icon
												icon={isOpen && canExpand ? 'tabler:folder-open' : 'tabler:folder'}
												class="h-5 w-5 shrink-0 opacity-70"
											/>
											<span class="truncate">{node.name}</span>
											<span class="ml-auto text-xs opacity-50">{node.fileCount}</span>
										</button>
									</div>
									{#if isOpen && canExpand && node.children}
										<ul class="space-y-0.5">
											{@render sidebarNodes(node.children)}
										</ul>
									{/if}
								</li>
							{/if}
						{/each}
					{/snippet}
					{@render sidebarNodes(sidebarTree)}
				</ul>
			</div>
		{/if}
		<div
			class="flex h-12 shrink-0 items-center border-t border-base-200 {foldersPaneOpen
				? 'justify-end px-3'
				: 'justify-center'}"
		>
			<button
				type="button"
				aria-label={foldersPaneOpen ? 'Collapse folders' : 'Expand folders'}
				class="flex h-10 w-10 items-center justify-center rounded-md text-base-content/70 transition-colors hover:bg-base-200 hover:text-base-content"
				onclick={toggleFoldersPane}
			>
				<Icon
					icon={foldersPaneOpen ? 'tabler:chevron-left' : 'tabler:chevron-right'}
					class="h-5 w-5"
				/>
			</button>
		</div>
	</aside>

	<div class="flex min-w-0 flex-1 flex-col">
		<div class="shrink-0 space-y-3 border-b border-base-200 bg-base-100 px-6 pb-4 pt-6">
			{#if selectedFolder}
				{@const segments = selectedFolder.split('/')}
				<div class="flex items-center gap-1.5 text-sm">
					<button
						type="button"
						class="text-primary hover:underline"
						onclick={() => selectFolder(null)}
					>All resources</button>
					{#each segments as segment, i}
						<span class="opacity-40">/</span>
						{#if i < segments.length - 1}
							<button
								type="button"
								class="font-mono text-primary hover:underline"
								onclick={() => selectFolder(segments.slice(0, i + 1).join('/'))}
							>{segment}</button>
						{:else}
							<span class="font-mono">{segment}</span>
						{/if}
					{/each}
					<span class="text-xs opacity-50">· {visibleResources.length} {visibleResources.length === 1 ? 'item' : 'items'}</span>
				</div>
			{/if}
			<div class="flex items-center justify-end">
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
		</div>

		<div class="min-h-0 flex-1 overflow-y-auto p-6">
		{#if loading}
			<div class="flex justify-center py-12">
				<span class="loading loading-spinner loading-lg text-primary"></span>
			</div>
		{:else if error}
			<div class="alert alert-error" role="alert">
				<span>Failed to load resources: {error}</span>
			</div>
		{:else if visibleResources.length === 0}
			<div class="rounded-lg border border-base-200 bg-base-100 p-8 text-center text-sm opacity-70">
				{searchActive ? `No matches for "${search}".` : 'No resources'}
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
						{#each visibleResources as resource (resource.key)}
							<tr
								class="cursor-pointer transition-colors hover:bg-base-200"
								onclick={() => openResource(resource.key)}
								ondblclick={() => goto(`${base}/resources/${encodePath(resource.key)}`)}
								onkeydown={(e) => {
									if (e.key === 'Enter' || e.key === ' ') openResource(resource.key);
								}}
								tabindex="0"
								role="button"
							>
								<td class="font-mono text-xs">{resource.key}</td>
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
		</div>
	</div>
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
			<div class="flex items-start justify-between border-b border-base-200 px-4 py-3">
				<div class="min-w-0 flex-1">
					<div class="mb-0.5 flex items-center gap-1.5 text-xs">
						<span class="opacity-60">Resources</span>
						{#each selectedFolderSegments as segment}
							<span class="opacity-40">/</span>
							<span class="font-mono opacity-70">{segment}</span>
						{/each}
						<span class="opacity-40">/</span>
						<span class="font-mono">{selectedLeaf}</span>
					</div>
					<div class="text-sm font-medium">{selectedLeaf}</div>
				</div>
				<div class="flex items-center gap-1">
					{#if selected}
						<div class="tooltip tooltip-left" data-tip="Open full page">
							<a
								href="{base}/resources/{encodePath(selected)}"
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
			<div class="flex h-10 shrink-0 items-center justify-between border-b border-base-200 bg-base-100 px-4">
				<span class="text-xs font-medium uppercase opacity-70">
					{selectedContent?.extension ?? 'File'}
				</span>
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
