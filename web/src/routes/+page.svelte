<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import { SvelteSet } from 'svelte/reactivity';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import Badge from '$lib/Badge.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { apiUrl } from '$lib/api';
	import { formatRelative as fmtRelativeMs } from '$lib/time';
	import { activitiesFor, allMetrics, releaseFlowSubscription } from '$lib/activityStore.svelte';
	import Icon from '@iconify/svelte';
	import type { FlowStatus, FlowSummary as Flow, FlowDetail } from '$lib/api';
	import { buildTree, type TreeNode } from '$lib/tree';

	function label(flow: { name: string; display_name?: string | null }): string {
		return flow.display_name ?? flow.name;
	}

	// URL-encode a slash-delimited flow path so it survives fetch and goto.
	function encodePath(path: string): string {
		return path.split('/').map(encodeURIComponent).join('/');
	}

	let flows = $state<Flow[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let nowTick = $state(Date.now());

	let search = $state('');
	let selectedTags = $state<Set<string>>(new Set());
	let statusFilter = $state<Record<FlowStatus, boolean>>({
		idle: true,
		ok: true,
		warn: true,
		error: true,
	});

	let statusCounts = $derived.by(() => {
		const c: Record<FlowStatus, number> = { idle: 0, ok: 0, warn: 0, error: 0 };
		for (const f of flowsView) c[f.status] += 1;
		return c;
	});

	function toggleStatus(s: FlowStatus) {
		statusFilter = { ...statusFilter, [s]: !statusFilter[s] };
	}

	// Unique tag list from the currently-loaded flows. Sorted alphabetically
	// so the chip row is stable across refreshes.
	let allTags = $derived.by(() => {
		const s = new Set<string>();
		for (const f of flows) for (const t of f.tags) s.add(t);
		return [...s].sort((a, b) => a.localeCompare(b));
	});

	// Per-tag counts from the flows visible after status/folder scope but
	// before tag filtering — so the number next to each chip tells the user
	// how many flows they'd get by picking that tag on top of the current
	// filter state.
	let tagCounts = $derived.by(() => {
		const counts: Record<string, number> = {};
		const source = selectedFolder === null
			? flows
			: flows.filter((f) => f.path.startsWith(selectedFolder + '/'));
		for (const f of source) for (const t of f.tags) counts[t] = (counts[t] ?? 0) + 1;
		return counts;
	});

	function toggleTag(tag: string) {
		const next = new Set(selectedTags);
		if (next.has(tag)) next.delete(tag);
		else next.add(tag);
		selectedTags = next;
	}

	let selected = $state<string | null>(null);
	let selectedDetail = $state<FlowDetail | null>(null);
	let selectedLoading = $state(false);
	let selectedError = $state<string | null>(null);
	// Modal activities come straight from the shared store so opening the
	// modal after the detail page has already primed the buffer doesn't
	// re-fetch history (which the old per-page EventSource used to do).
	let modalActivities = $derived(selected ? activitiesFor(selected) : []);

	let tickerId: ReturnType<typeof setInterval> | null = null;

	// Merge live metrics from the shared store onto the initial /api/flows
	// snapshot so the table stays live without each row wiring its own SSE.
	let liveMetrics = $derived(allMetrics());
	let flowsView = $derived(
		flows.map((f) => {
			const m = liveMetrics[f.path];
			if (!m) return f;
			return {
				...f,
				events_total: m.events_total,
				warnings_total: m.warnings_total,
				errors_total: m.errors_total,
				last_event_at: m.last_event_at_ms
					? new Date(m.last_event_at_ms).toISOString()
					: f.last_event_at,
				last_warning_at: m.last_warning_at_ms
					? new Date(m.last_warning_at_ms).toISOString()
					: f.last_warning_at,
				last_error_at: m.last_error_at_ms
					? new Date(m.last_error_at_ms).toISOString()
					: f.last_error_at,
				status: m.status
			};
		}),
	);

	onMount(() => {
		const pane = localStorage.getItem('flowgen-folders-pane');
		if (pane !== null) foldersPaneOpen = pane === '1';
		const exp = localStorage.getItem('flowgen-folders-expanded');
		if (exp) {
			try {
				for (const p of JSON.parse(exp) as string[]) expandedFolders.add(p);
			} catch {
				// ignore corrupt state
			}
		}

		const url = apiUrl('api/flows');
		fetch(url)
			.then((r) => {
				if (!r.ok) throw new Error(`Server responded with HTTP ${r.status} ${r.statusText}`);
				return r.json();
			})
			.then((data) => {
				flows = data;
			})
			.catch((err) => {
				if (err instanceof TypeError) {
					error = `Cannot reach flowgen server at ${url}. Check that the backend is running.`;
				} else {
					error = err instanceof Error ? err.message : String(err);
				}
			})
			.finally(() => {
				loading = false;
			});

		tickerId = setInterval(() => (nowTick = Date.now()), 1000);

		return () => {
			if (tickerId !== null) clearInterval(tickerId);
			releaseFlowSubscription();
		};
	});

	// Cache lets repeat clicks paint instantly and lets the modal open only
	// when the payload is ready, so the user never sees the empty-then-loading
	// flash the DAG + Prism pipeline was doing on first paint.
	const flowCache = new Map<string, FlowDetail>();
	// Tracks the most recent open request so late responses from a
	// previously-clicked flow don't overwrite the current one.
	let pendingFlow: string | null = null;

	async function openFlow(path: string) {
		pendingFlow = path;
		const cached = flowCache.get(path);
		if (cached) {
			selected = path;
			selectedDetail = cached;
			selectedError = null;
			selectedLoading = false;
		} else {
			selectedLoading = true;
		}
		try {
			const response = await fetch(apiUrl(`api/flows/${encodePath(path)}`));
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			const detail: FlowDetail = await response.json();
			flowCache.set(path, detail);
			if (pendingFlow !== path) return;
			selected = path;
			selectedDetail = detail;
			selectedError = null;
		} catch (err) {
			if (pendingFlow !== path) return;
			selected = path;
			selectedError = err instanceof Error ? err.message : 'Failed to load flow';
		} finally {
			if (pendingFlow === path) selectedLoading = false;
		}
	}

	function closeFlow() {
		selected = null;
		selectedDetail = null;
		selectedError = null;
		releaseFlowSubscription();
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape' && selected) closeFlow();
	}

	type SortKey = 'name' | 'status' | 'last_event';
	let sortKey = $state<SortKey>('name');
	let sortDir = $state<'asc' | 'desc'>('asc');

	function toggleSort(key: SortKey) {
		if (sortKey === key) {
			sortDir = sortDir === 'asc' ? 'desc' : 'asc';
		} else {
			sortKey = key;
			sortDir = 'asc';
		}
	}

	// Order used when sorting by Status column — worst first descending.
	const STATUS_RANK: Record<FlowStatus, number> = { error: 3, warn: 2, ok: 1, idle: 0 };

	function compareFlows(a: Flow, b: Flow, key: SortKey): number {
		if (key === 'name') {
			return label(a).localeCompare(label(b), undefined, { sensitivity: 'base' });
		}
		if (key === 'status') {
			return STATUS_RANK[a.status] - STATUS_RANK[b.status];
		}
		// last_event: nulls sort as oldest so active flows bubble up in desc.
		const ta = a.last_event_at ? new Date(a.last_event_at).getTime() : 0;
		const tb = b.last_event_at ? new Date(b.last_event_at).getTime() : 0;
		return ta - tb;
	}

	let filtered = $derived.by(() => {
		const term = search.toLowerCase();
		const requiredTags = selectedTags;
		const matched = flowsView.filter((flow) => {
			if (!statusFilter[flow.status]) return false;
			for (const t of requiredTags) if (!flow.tags.includes(t)) return false;
			if (term.length === 0) return true;
			return (
				flow.path.toLowerCase().includes(term) ||
				flow.name.toLowerCase().includes(term) ||
				(flow.display_name?.toLowerCase().includes(term) ?? false) ||
				(flow.description?.toLowerCase().includes(term) ?? false) ||
				flow.tags.some((tag) => tag.toLowerCase().includes(term))
			);
		});
		const sign = sortDir === 'asc' ? 1 : -1;
		return matched.slice().sort((a, b) => sign * compareFlows(a, b, sortKey));
	});

	// Sidebar tree built from all loaded flows (not `filtered`) so the folder
	// structure is stable while the user filters. `selectedFolder` scopes the
	// main table to one folder; `null` means "all flows".
	let expandedFolders = $state(new SvelteSet<string>());
	let selectedFolder = $state<string | null>(null);
	let foldersPaneOpen = $state(true);

	// Modal breadcrumb parts derived from `selected` (the currently open flow path).
	let selectedSegments = $derived(selected ? selected.split('/') : []);
	let selectedFolders = $derived(selectedSegments.slice(0, -1));
	let selectedLeaf = $derived(selectedSegments[selectedSegments.length - 1] ?? '');
	let searchActive = $derived(search.trim().length > 0);
	let sidebarTree = $derived(buildTree<Flow>(flows, (f) => f.path));

	// Table rows: `filtered` (already respects status/tag/search) further
	// narrowed to `selectedFolder` when set. Search overrides folder scope
	// so hits from any folder surface.
	let visibleFlows = $derived.by(() => {
		if (searchActive || selectedFolder === null) return filtered;
		const prefix = selectedFolder + '/';
		return filtered.filter((f) => f.path.startsWith(prefix));
	});

	function toggleFolder(path: string) {
		if (expandedFolders.has(path)) expandedFolders.delete(path);
		else expandedFolders.add(path);
		localStorage.setItem(
			'flowgen-folders-expanded',
			JSON.stringify(Array.from(expandedFolders)),
		);
	}

	function selectFolder(path: string | null) {
		selectedFolder = path;
	}

	function toggleFoldersPane() {
		foldersPaneOpen = !foldersPaneOpen;
		localStorage.setItem('flowgen-folders-pane', foldersPaneOpen ? '1' : '0');
	}

	// True for folders that contain at least one nested folder — used to
	// hide the chevron on leaf folders where nothing would show up.
	function hasSubfolders(node: TreeNode<Flow>): boolean {
		return (node.children ?? []).some((c) => c.isFolder);
	}

	function formatRelative(ts: string | null, tick: number): string {
		if (!ts) return '—';
		const ms = new Date(ts).getTime();
		if (isNaN(ms)) return ts;
		return fmtRelativeMs(ms, tick);
	}
</script>

<svelte:head>
	<title>Flows Overview | Flowgen</title>
</svelte:head>

<svelte:window on:keydown={onKeydown} />

<section class="flex h-[calc(100vh-4rem)]">
	<aside
		class="flex shrink-0 flex-col border-r border-base-300 bg-base-100 transition-[width] duration-200 ease-out {foldersPaneOpen
			? 'w-64'
			: 'w-16'}"
	>
		{#if !foldersPaneOpen}
			<div class="flex flex-1 flex-col items-center py-2">
				<div class="tooltip tooltip-right" data-tip="Folders">
					<button
						type="button"
						aria-label="Expand folders"
						class="relative flex h-10 w-10 items-center justify-center rounded-md bg-base-200 text-primary transition-colors hover:bg-base-200"
						onclick={toggleFoldersPane}
					>
						<span class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"></span>
						<Icon icon="tabler:layout-list" class="h-5 w-5 shrink-0" />
					</button>
				</div>
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
						<span>All flows</span>
						<span class="ml-auto text-xs opacity-50">{flows.length}</span>
					</button>
				</li>
				{#snippet sidebarNodes(nodes: TreeNode<Flow>[])}
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
										class="relative flex min-w-0 flex-1 items-center gap-1.5 h-10 rounded-md px-2 text-left transition-colors {isSelected
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
										<span
											class="tooltip tooltip-right min-w-0 flex-1 truncate text-left before:max-w-xs before:whitespace-normal before:break-words"
											data-tip={node.name}>{node.name}</span>
										<span class="ml-2 shrink-0 text-xs opacity-50">{node.fileCount}</span>
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
				data-tip={foldersPaneOpen ? 'Collapse folders' : 'Expand folders'}
				class="tooltip {foldersPaneOpen
					? 'tooltip-top'
					: 'tooltip-right'} flex h-10 w-10 items-center justify-center rounded-md text-base-content/70 transition-colors hover:bg-base-200 hover:text-base-content"
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
			>All flows</button>
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
			<span class="text-xs opacity-50">· {visibleFlows.length} {visibleFlows.length === 1 ? 'flow' : 'flows'}</span>
		</div>
	{/if}
	<div class="flex flex-wrap items-center gap-2">
		<div class="flex items-center gap-1">
			<button
				type="button"
				class="chip {statusFilter.idle ? 'chip-neutral' : 'chip-inactive'}"
				aria-pressed={statusFilter.idle}
				onclick={() => toggleStatus('idle')}
			>
				<span>Idle</span>
				<span class="tabular-nums opacity-60">{statusCounts.idle}</span>
			</button>
			<button
				type="button"
				class="chip {statusFilter.ok ? 'chip-info' : 'chip-inactive'}"
				aria-pressed={statusFilter.ok}
				onclick={() => toggleStatus('ok')}
			>
				<span>Ok</span>
				<span class="tabular-nums opacity-60">{statusCounts.ok}</span>
			</button>
			<button
				type="button"
				class="chip {statusFilter.warn ? 'chip-warn' : 'chip-inactive'}"
				aria-pressed={statusFilter.warn}
				onclick={() => toggleStatus('warn')}
			>
				<span>Warn</span>
				<span class="tabular-nums opacity-60">{statusCounts.warn}</span>
			</button>
			<button
				type="button"
				class="chip {statusFilter.error ? 'chip-error' : 'chip-inactive'}"
				aria-pressed={statusFilter.error}
				onclick={() => toggleStatus('error')}
			>
				<span>Error</span>
				<span class="tabular-nums opacity-60">{statusCounts.error}</span>
			</button>
		</div>
		{#if allTags.length > 0}
			<div class="dropdown">
				<button
					type="button"
					tabindex="0"
					class="btn btn-sm border border-base-300 bg-base-100 font-normal hover:bg-base-200"
				>
					<Icon icon="tabler:tag" class="h-4 w-4 opacity-70" />
					<span>Filter by tag</span>
					{#if selectedTags.size > 0}
						<span class="badge badge-sm bg-primary/20 text-primary">{selectedTags.size}</span>
					{/if}
					<Icon icon="tabler:chevron-down" class="h-3.5 w-3.5 opacity-60" />
				</button>
				<div
					tabindex="-1"
					class="dropdown-content z-10 mt-1 max-h-72 w-56 overflow-auto rounded-md border border-base-200 bg-base-100 p-1 shadow-lg"
				>
					{#each allTags as tag}
						{@const count = tagCounts[tag] ?? 0}
						<button
							type="button"
							class="flex w-full items-center gap-2 rounded px-2 py-1 text-left text-sm hover:bg-base-200 {count === 0
								? 'opacity-40'
								: ''}"
							onclick={() => toggleTag(tag)}
						>
							<span
								class="inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded border {selectedTags.has(
									tag,
								)
									? 'border-primary bg-primary text-primary-content'
									: 'border-base-300'}"
							>
								{#if selectedTags.has(tag)}
									<Icon icon="tabler:check" class="h-2.5 w-2.5" />
								{/if}
							</span>
							<span class="truncate">{tag}</span>
							<span class="ml-auto text-xs opacity-60 tabular-nums">{count}</span>
						</button>
					{/each}
				</div>
			</div>
			{#if selectedTags.size > 0}
				<div class="flex flex-wrap items-center gap-1">
					{#each [...selectedTags] as tag}
						<button
							type="button"
							class="flex items-center gap-1 rounded-full border border-primary/50 bg-primary/10 px-2 py-0.5 text-xs"
							onclick={() => toggleTag(tag)}
							aria-label={`Remove ${tag} filter`}
						>
							<span>{tag}</span>
							<Icon icon="tabler:x" class="h-3 w-3 opacity-60" />
						</button>
					{/each}
					<div class="tooltip tooltip-bottom" data-tip="Clear tag filters">
						<button
							type="button"
							class="btn btn-sm btn-ghost btn-circle"
							aria-label="Clear tag filters"
							onclick={() => (selectedTags = new Set())}
						>
							<Icon icon="tabler:trash" class="h-5 w-5" />
						</button>
					</div>
				</div>
			{/if}
		{/if}
		<div class="flex-1"></div>
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
			<input type="text" placeholder="Search flows..." bind:value={search} />
			{#if search}
				<div class="tooltip tooltip-left" data-tip="Clear search">
					<button
						type="button"
						class="opacity-50 hover:opacity-100"
						aria-label="Clear search"
						onclick={() => (search = '')}
					>
						<Icon icon="tabler:x" class="h-5 w-5" />
					</button>
				</div>
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
		<StateMessage tone="oops" title="Failed to load flows" message={error} />
	{:else if filtered.length === 0}
		<StateMessage tone="notice" title="No flows found" message="Nothing matches the current filters yet." />
	{:else}
		<div class="overflow-x-auto rounded-lg border border-base-300 bg-base-100">
			<table class="table table-sm w-full bg-base-100">
				<thead class="bg-base-100 text-xs uppercase tracking-wide opacity-60">
					<tr>
						<th>
							<button
								type="button"
								class="flex items-center gap-1 uppercase tracking-wide"
								onclick={() => toggleSort('name')}
								aria-label="Sort by name"
							>
								<span>Name</span>
								<Icon
									icon={sortKey === 'name'
										? sortDir === 'asc'
											? 'tabler:arrow-up'
											: 'tabler:arrow-down'
										: 'tabler:arrows-sort'}
									class="h-4 w-4 {sortKey === 'name' ? 'opacity-70' : 'opacity-30'}"
								/>
							</button>
						</th>
						<th>Description</th>
						<th>Tags</th>
						<th>
							<button
								type="button"
								class="flex items-center gap-1 uppercase tracking-wide"
								onclick={() => toggleSort('status')}
								aria-label="Sort by status"
							>
								<span>Status</span>
								<Icon
									icon={sortKey === 'status'
										? sortDir === 'asc'
											? 'tabler:arrow-up'
											: 'tabler:arrow-down'
										: 'tabler:arrows-sort'}
									class="h-4 w-4 {sortKey === 'status' ? 'opacity-70' : 'opacity-30'}"
								/>
							</button>
						</th>
						<th>
							<button
								type="button"
								class="flex items-center gap-1 uppercase tracking-wide"
								onclick={() => toggleSort('last_event')}
								aria-label="Sort by last event"
							>
								<span>Last event</span>
								<Icon
									icon={sortKey === 'last_event'
										? sortDir === 'asc'
											? 'tabler:arrow-up'
											: 'tabler:arrow-down'
										: 'tabler:arrows-sort'}
									class="h-4 w-4 {sortKey === 'last_event' ? 'opacity-70' : 'opacity-30'}"
								/>
							</button>
						</th>
					</tr>
				</thead>
				<tbody>
					{#each visibleFlows as flow (flow.path)}
						<tr
							class="cursor-pointer transition-colors hover:bg-base-200"
							onclick={() => openFlow(flow.path)}
							ondblclick={() => goto(`${base}/flows/${encodePath(flow.path)}`)}
							onkeydown={(e) => {
								if (e.key === 'Enter' || e.key === ' ') openFlow(flow.path);
							}}
							tabindex="0"
							role="button"
						>
							<td class="whitespace-nowrap">
								<div class="font-medium">{label(flow)}</div>
								<div class="font-mono text-xs opacity-70">{flow.path}</div>
							</td>
							<td class="max-w-md text-sm">
								{flow.description ?? '—'}
							</td>
							<td>
								{#if flow.tags.length === 0}
									<span class="opacity-50">—</span>
								{:else}
									<div class="flex flex-wrap gap-1">
										{#each flow.tags as tag}
											<button
												type="button"
												class="rounded-full border px-2 py-0.5 text-xs transition-colors {selectedTags.has(tag)
													? 'border-primary/50 bg-primary/10'
													: 'border-base-300 opacity-70 hover:opacity-100'}"
												aria-pressed={selectedTags.has(tag)}
												onclick={(e) => {
													e.stopPropagation();
													toggleTag(tag);
												}}
											>
												{tag}
											</button>
										{/each}
									</div>
								{/if}
							</td>
							<td>
								{#if flow.status === 'ok'}
									<Badge variant="success">Ok</Badge>
								{:else if flow.status === 'warn'}
									<Badge variant="warning">Warn</Badge>
								{:else if flow.status === 'error'}
									<Badge variant="error">Error</Badge>
								{:else}
									<Badge>Idle</Badge>
								{/if}
							</td>
							<td class="whitespace-nowrap text-sm opacity-70">
								{formatRelative(flow.last_event_at, nowTick)}
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
		class="fixed inset-0 z-40 flex items-center justify-center bg-black/70 p-4"
		role="dialog"
		aria-modal="true"
		aria-label="Flow YAML viewer"
		onclick={(e) => {
			if (e.target === e.currentTarget) closeFlow();
		}}
		onkeydown={(e) => {
			if (e.key === 'Escape') closeFlow();
		}}
		tabindex="-1"
	>
		<div
			class="flex h-[90vh] w-full max-w-[95vw] flex-col overflow-hidden rounded-lg border border-base-300 bg-base-100 shadow-2xl ring-1 ring-base-content/10"
		>
			<div class="flex items-start justify-between border-b border-base-200 px-4 py-3">
				<div class="min-w-0 flex-1">
					<div class="mb-0.5 flex items-center gap-1.5 text-xs">
						<span class="opacity-60">Flows</span>
						{#each selectedFolders as segment}
							<span class="opacity-40">/</span>
							<span class="font-mono opacity-70">{segment}</span>
						{/each}
						<span class="opacity-40">/</span>
						<span class="font-mono">{selectedLeaf}</span>
					</div>
					<div class="text-sm font-medium">
						{selectedDetail?.display_name ?? selectedLeaf}
					</div>
				</div>
				<div class="flex items-center gap-1">
					<div class="tooltip tooltip-left" data-tip="Open full page">
						<a
							href="{base}/flows/{encodePath(selected)}"
							class="btn btn-ghost btn-sm btn-circle"
							aria-label="Open full page"
						>
							<Icon icon="tabler:external-link" class="h-5 w-5" />
						</a>
					</div>
					<div class="tooltip tooltip-left" data-tip="Close">
						<button
							type="button"
							class="btn btn-ghost btn-sm btn-circle"
							aria-label="Close"
							onclick={closeFlow}
						>
							<Icon icon="tabler:x" class="h-5 w-5" />
						</button>
					</div>
				</div>
			</div>
			<div class="flex min-h-0 flex-1 overflow-hidden">
				{#if selectedLoading}
					<div class="flex flex-1 items-center justify-center py-12">
						<span class="loading loading-spinner loading-md text-primary"></span>
					</div>
				{:else if selectedError}
					<div class="flex-1">
						<StateMessage tone="oops" title="Failed to load flow" message={selectedError} />
					</div>
				{:else if selectedDetail}
					<FlowInspector yaml={selectedDetail.yaml} activities={modalActivities} />
				{/if}
			</div>
		</div>
	</div>
{/if}
