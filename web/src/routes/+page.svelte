<script lang="ts">
	import { base } from '$app/paths';
	import { goto, pushState, replaceState } from '$app/navigation';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import { SvelteSet } from 'svelte/reactivity';
	import FlowDag from '$lib/dag/FlowDag.svelte';
	import Inspector from '$lib/Inspector.svelte';
	import StatusDot from '$lib/StatusDot.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { apiUrl, encodePath, errorMessage, failure } from '$lib/api';
	import { formatRelative as fmtRelativeMs } from '$lib/time';
	import { activitiesFor, allMetrics, releaseFlowSubscription } from '$lib/activityStore.svelte';
	import Icon from '@iconify/svelte';
	import { m } from '$lib/paraglide/messages.js';
	import FolderTree from '$lib/FolderTree.svelte';
	import ListHeader from '$lib/ListHeader.svelte';
	import SidePane from '$lib/SidePane.svelte';
	import { isMobile } from '$lib/mobile';
	import { pageTitle } from '$lib/pageTitle';
	import { connectorByName } from '$lib/connectors';
	import type {
		AuthoringTarget,
		ChangeSummary,
		Connection,
		FlowStatus,
		FlowSummary as Flow,
		FlowDetail
	} from '$lib/api';
	import { buildTree } from '$lib/tree';
	import IconButton from '$lib/IconButton.svelte';
	import {
		authoringEnabled,
		authoringTargets,
		editUrl,
		flowFilePath,
		pendingChanges,
		targetOf
	} from '$lib/changes';

	function label(flow: { name: string; display_name?: string | null }): string {
		return flow.display_name ?? flow.name;
	}

	let flows = $state<Flow[]>([]);
	let connection = $state<Connection | null>(null);
	let scopedFlows = $derived(
		connection === null ? flows : flows.filter((f) => connection?.flows.includes(f.path)),
	);
	let pending = $state<ChangeSummary[]>([]);
	let authoring = $state(false);
	let targets = $state<AuthoringTarget[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let nowTick = $state(Date.now());

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

	// Sorted so the chip row is stable across refreshes.
	let allTags = $derived.by(() => {
		const s = new Set<string>();
		for (const f of scopedFlows) for (const t of f.tags) s.add(t);
		return [...s].sort((a, b) => a.localeCompare(b));
	});

	// Per-tag counts from the flows visible after status/folder scope but
	// before tag filtering — so the number next to each chip tells the user
	// how many flows they'd get by picking that tag on top of the current
	// filter state.
	let tagCounts = $derived.by(() => {
		const counts: Record<string, number> = {};
		const source = selectedFolder === null
			? scopedFlows
			: scopedFlows.filter((f) => f.path.startsWith(selectedFolder + '/'));
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

	let tickerId: ReturnType<typeof setInterval> | null = null;

	// Merge live metrics from the shared store onto the initial /api/flows
	// snapshot so the table stays live without each row wiring its own SSE.
	let liveMetrics = $derived(allMetrics());
	let flowsView = $derived(
		scopedFlows.map((f) => {
			const metrics = liveMetrics[f.path];
			if (!metrics) return f;
			return {
				...f,
				events_total: metrics.events_total,
				warnings_total: metrics.warnings_total,
				errors_total: metrics.errors_total,
				last_event_at: metrics.last_event_at_ms
					? new Date(metrics.last_event_at_ms).toISOString()
					: f.last_event_at,
				last_warning_at: metrics.last_warning_at_ms
					? new Date(metrics.last_warning_at_ms).toISOString()
					: f.last_warning_at,
				last_error_at: metrics.last_error_at_ms
					? new Date(metrics.last_error_at_ms).toISOString()
					: f.last_error_at,
				status: metrics.status
			};
		}),
	);

	onMount(() => {
		const pane = localStorage.getItem('flowgen-folders-pane');
		if (pane !== null) foldersPaneOpen = pane === '1';
		if (isMobile()) foldersPaneOpen = false;
		const exp = localStorage.getItem('flowgen-folders-expanded');
		if (exp) {
			try {
				for (const p of JSON.parse(exp) as string[]) expandedFolders.add(p);
			} catch {
				// ignore corrupt state
			}
		}

		pendingChanges().then((changes) => (pending = changes));
		authoringEnabled().then((enabled) => (authoring = enabled));
		authoringTargets().then((found) => (targets = found));

		load<Flow[]>('api/flows')
			.then((found) => {
				flows = found;
			})
			.catch((err) => {
				error = errorMessage(err, m.flows_load_failed());
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

	async function load<T>(path: string): Promise<T> {
		const response = await fetch(apiUrl(path));
		if (!response.ok) throw await failure(response);
		return response.json();
	}

	// Shallow routing leaves `page.url` as is, so opening a preview keeps the filter.
	let connectionId = $derived(page.url.searchParams.get('connection'));
	let connectionLoading = $state(false);

	$effect(() => {
		const id = connectionId;
		if (id === null) {
			connection = null;
			return;
		}
		let current = true;
		connectionLoading = true;
		loadConnection(id)
			.then((found) => {
				if (current) connection = found;
			})
			.catch((err) => {
				if (current) error = errorMessage(err, m.flows_connection_load_failed());
			})
			.finally(() => {
				if (current) connectionLoading = false;
			});
		return () => {
			current = false;
		};
	});

	// A link to a connection no loaded flow uses any more shows every flow.
	async function loadConnection(id: string): Promise<Connection | null> {
		const response = await fetch(apiUrl(`api/connections/${encodeURIComponent(id)}`));
		if (response.status === 404) {
			clearConnection();
			return null;
		}
		if (!response.ok) throw await failure(response);
		return response.json();
	}

	function clearConnection() {
		goto(`${base}/`, { replaceState: true, noScroll: true, keepFocus: true });
	}

	// Cache lets repeat clicks paint instantly.
	const flowCache = new Map<string, FlowDetail>();
	// Tracks the most recent open request so late responses from a
	// previously-clicked flow don't overwrite the current one.
	let pendingFlow: string | null = null;

	async function openFlow(path: string) {
		pendingFlow = path;
		const show = page.state.flow ? replaceState : pushState;
		show(`${base}/flows/${encodePath(path)}`, { flow: path });
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
			if (response.status === 404) throw new Error(m.flow_not_found({ path }));
			if (!response.ok) throw await failure(response);
			const detail: FlowDetail = await response.json();
			flowCache.set(path, detail);
			if (pendingFlow !== path) return;
			selected = path;
			selectedDetail = detail;
			selectedError = null;
		} catch (err) {
			if (pendingFlow !== path) return;
			selected = path;
			selectedError = errorMessage(err, m.flow_load_failed({ path }));
		} finally {
			if (pendingFlow === path) selectedLoading = false;
		}
	}

	function closeFlow() {
		if (page.state.flow) history.back();
		else clearFlow();
	}

	// Back closes the preview: the shallow history entry it pushed is gone.
	$effect(() => {
		if (!page.state.flow && (selected || selectedLoading)) clearFlow();
	});

	function clearFlow() {
		pendingFlow = null;
		selectedLoading = false;
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
		const requiredTags = selectedTags;
		const matched = flowsView.filter((flow) => {
			if (!statusFilter[flow.status]) return false;
			for (const t of requiredTags) if (!flow.tags.includes(t)) return false;
			return true;
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
	let selectedLeaf = $derived(selectedSegments[selectedSegments.length - 1] ?? '');	let sidebarTree = $derived(buildTree<Flow>(scopedFlows, (f) => f.path));

	// Table rows: `filtered` (already respects status and tags) further
	// narrowed to `selectedFolder` when set.
	let visibleFlows = $derived.by(() => {
		if (selectedFolder === null) return filtered;
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
		closePaneOnMobile();
	}

	// On mobile the pane covers the list, so it closes once a folder is picked.
	function closePaneOnMobile() {
		if (isMobile()) foldersPaneOpen = false;
	}

	function toggleFoldersPane() {
		foldersPaneOpen = !foldersPaneOpen;
		localStorage.setItem('flowgen-folders-pane', foldersPaneOpen ? '1' : '0');
	}

	function formatRelative(ts: string | null, tick: number): string {
		if (!ts) return '—';
		const ms = new Date(ts).getTime();
		if (isNaN(ms)) return ts;
		return fmtRelativeMs(ms, tick);
	}
</script>

<svelte:head>
	<title>{pageTitle(m.nav_flows())}</title>
</svelte:head>

<svelte:window on:keydown={onKeydown} />

<section class="relative flex h-[calc(100vh-4rem)]">
	<SidePane
		open={foldersPaneOpen}
		title={m.flows_folders()}
		collapseLabel={m.flows_folders_collapse()}
		expandLabel={m.flows_folders_expand()}
		ontoggle={toggleFoldersPane}
		onclose={closePaneOnMobile}
	>
		<div class="min-h-0 flex-1 overflow-y-auto px-3 pb-3">
			<FolderTree
				tree={sidebarTree}
				selected={selectedFolder}
				expanded={expandedFolders}
				allLabel={m.flows_all()}
				allIcon="tabler:layout-list"
				allCount={scopedFlows.length}
				expandLabel={(folder) => m.flows_folder_expand({ folder })}
				collapseLabel={(folder) => m.flows_folder_collapse({ folder })}
				onselect={selectFolder}
				ontoggle={toggleFolder}
			/>
		</div>
	</SidePane>

	<div class="@container flex min-w-0 flex-1 flex-col">
	<div class="shrink-0 space-y-3 bg-base-100 px-6 pb-4 pt-6">
	<ListHeader
		allLabel={m.flows_all()}
		folder={selectedFolder}
		onselect={selectFolder}
		count={m.flows_count({ count: visibleFlows.length })}
		pending={authoring ? pending.length : null}
		pendingLabel={m.flows_pending_changes({ count: pending.length })}
		newHref="{base}/edit?new=flow{selectedFolder ? `&folder=${encodePath(selectedFolder)}` : ''}"
	/>
	<div class="flex flex-wrap items-center gap-2">
		<div class="flex items-center gap-1">
			<button
				type="button"
				class="chip {statusFilter.idle ? 'chip-neutral' : 'chip-inactive'}"
				aria-pressed={statusFilter.idle}
				onclick={() => toggleStatus('idle')}
			>
				<span>{m.flows_status_idle()}</span>
				{#if statusFilter.idle}
					<span class="tabular-nums opacity-60">{statusCounts.idle}</span>
				{/if}
			</button>
			<button
				type="button"
				class="chip {statusFilter.ok ? 'chip-success' : 'chip-inactive'}"
				aria-pressed={statusFilter.ok}
				onclick={() => toggleStatus('ok')}
			>
				<span>{m.flows_status_ok()}</span>
				{#if statusFilter.ok}
					<span class="tabular-nums opacity-60">{statusCounts.ok}</span>
				{/if}
			</button>
			<button
				type="button"
				class="chip {statusFilter.warn ? 'chip-warn' : 'chip-inactive'}"
				aria-pressed={statusFilter.warn}
				onclick={() => toggleStatus('warn')}
			>
				<span>{m.flows_status_warn()}</span>
				{#if statusFilter.warn}
					<span class="tabular-nums opacity-60">{statusCounts.warn}</span>
				{/if}
			</button>
			<button
				type="button"
				class="chip {statusFilter.error ? 'chip-error' : 'chip-inactive'}"
				aria-pressed={statusFilter.error}
				onclick={() => toggleStatus('error')}
			>
				<span>{m.flows_status_error()}</span>
				{#if statusFilter.error}
					<span class="tabular-nums opacity-60">{statusCounts.error}</span>
				{/if}
			</button>
			{#if connection}
				{@const connector = connectorByName(connection.connector)}
				<button
					type="button"
					class="chip bg-base-200 hover:bg-base-300/60"
					onclick={clearConnection}
					aria-label={m.flows_connection_filter_remove({ connection: connector.label })}
				>
					<img src={connector.iconUrl} alt="" class="h-3.5 w-3.5 shrink-0" />
					<span>{connector.label}</span>
					<span class="tabular-nums opacity-60">{scopedFlows.length}</span>
					<Icon icon="tabler:x" class="h-3 w-3 shrink-0 opacity-60" />
				</button>
			{/if}
		</div>
		{#if allTags.length > 0}
			<div class="dropdown">
				<button
					type="button"
					tabindex="0"
					class="chip chip-neutral hover:bg-base-300/60"
				>
					<span>{m.flows_tag_filter()}</span>
					{#if selectedTags.size > 0}
						<span class="tabular-nums opacity-60">{selectedTags.size}</span>
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
							class="chip chip-neutral hover:bg-base-300/60"
							onclick={() => toggleTag(tag)}
							aria-label={m.flows_tag_filter_remove({ tag })}
						>
							<span>{tag}</span>
							<Icon icon="tabler:x" class="h-3 w-3 opacity-60" />
						</button>
					{/each}
					<button
						type="button"
						class="btn btn-ghost btn-xs font-normal text-base-content/60"
						onclick={() => (selectedTags = new Set())}
					>
						{m.flows_tag_filters_clear()}
					</button>
				</div>
			{/if}
		{/if}
	</div>
	</div>

	<div class="flex min-h-0 flex-1 flex-col overflow-y-auto px-6 pb-6">
	{#if loading || connectionLoading}
		<div class="flex justify-center py-12">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if error}
		<StateMessage tone="oops" title={m.flows_load_failed()} message={error} />
	{:else if flows.length === 0}
		<StateMessage
			tone="notice"
			title={m.flows_empty_title()}
			message={authoring ? m.flows_empty_message_authoring() : m.flows_empty_message()}
		/>
	{:else if visibleFlows.length === 0}
		<StateMessage tone="notice" title={m.flows_no_match_title()} message={m.flows_no_match_message()} />
	{:else}
		<div class="-mx-6 shrink-0 overflow-x-auto px-6">
			<table class="table table-sm w-full bg-base-100">
				<thead class="bg-base-100 text-xs font-medium text-base-content/50">
					<tr>
						<th>
							<button
								type="button"
								class="flex items-center gap-1"
								onclick={() => toggleSort('name')}
								aria-label={m.flows_sort_by_name()}
							>
								<span>{m.flows_column_name()}</span>
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
						<th class="@max-5xl:hidden">{m.flows_column_description()}</th>
						<th class="@max-3xl:hidden">{m.flows_column_tags()}</th>
						<th>
							<button
								type="button"
								class="flex items-center gap-1"
								onclick={() => toggleSort('status')}
								aria-label={m.flows_sort_by_status()}
							>
								<span>{m.flows_column_status()}</span>
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
						<th class="@max-md:hidden">
							<button
								type="button"
								class="flex items-center gap-1"
								onclick={() => toggleSort('last_event')}
								aria-label={m.flows_sort_by_last_event()}
							>
								<span>{m.flows_column_last_event()}</span>
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
							class="group cursor-pointer transition-colors hover:bg-base-200 {selected === flow.path
								? 'selected bg-base-200'
								: ''}"
							onclick={() => openFlow(flow.path)}
							ondblclick={() => goto(`${base}/flows/${encodePath(flow.path)}`)}
							onkeydown={(e) => {
								if (e.key === 'Enter' || e.key === ' ') openFlow(flow.path);
							}}
							tabindex="0"
							role="button"
						>
							<td class="w-full max-w-0">
								<div class="truncate font-medium">{label(flow)}</div>
								<div class="truncate text-xs text-base-content/50">{flow.path}</div>
							</td>
							<td class="min-w-64 max-w-md text-sm @max-5xl:hidden">
								<span class="line-clamp-2">{flow.description ?? '—'}</span>
							</td>
							<td class="min-w-48 @max-3xl:hidden">
								{#if flow.tags.length === 0}
									<span class="opacity-50">—</span>
								{:else}
									<div class="flex flex-wrap gap-1">
										{#each flow.tags as tag}
											<button
												type="button"
												class="rounded-full px-2 py-0.5 text-xs transition-colors {selectedTags.has(tag)
													? 'bg-base-300/70'
													: 'bg-base-200 text-base-content/70 group-hover:bg-base-300/50 group-focus-visible:bg-base-300/50 group-[.selected]:bg-base-300/50 hover:text-base-content'}"
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
									<StatusDot variant="success" label={m.flows_status_ok()} />
								{:else if flow.status === 'warn'}
									<StatusDot variant="warning" label={m.flows_status_warn()} />
								{:else if flow.status === 'error'}
									<StatusDot variant="error" label={m.flows_status_error()} />
								{:else}
									<StatusDot label={m.flows_status_idle()} />
								{/if}
							</td>
							<td class="whitespace-nowrap text-sm opacity-70 @max-md:hidden">
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

	{#if selected}
		{@const path = selected}
		<Inspector
			title={selectedDetail?.display_name ?? selectedLeaf}
			label={m.flow_preview_label()}
			onclose={closeFlow}
		>
			{#snippet subtitle()}
				<span class="truncate">{path}</span>
			{/snippet}
			{#snippet actions()}
				{#if targetOf(targets, flowFilePath(path))}
					<IconButton icon="tabler:pencil" label={m.common_edit()} href={editUrl(base, flowFilePath(path))} />
				{/if}
				<IconButton
					icon="tabler:arrow-up-right"
					label={m.flow_open_full_page()}
					href="{base}/flows/{encodePath(path)}"
				/>
			{/snippet}
			{#if selectedLoading}
				<div class="flex justify-center py-12">
					<span class="loading loading-spinner loading-md text-primary"></span>
				</div>
			{:else if selectedError}
				<StateMessage tone="oops" title={m.flow_load_failed_title()} message={selectedError} />
			{:else if selectedDetail}
				<div class="-mx-6 -mb-6 h-[calc(100vh-8rem)]">
					<FlowDag
						yaml={selectedDetail.yaml}
						activities={activitiesFor(path)}
						onNodeClick={(task) => goto(`${base}/flows/${encodePath(path)}?task=${encodeURIComponent(task)}`)}
					/>
				</div>
			{/if}
		</Inspector>
	{/if}
</section>
