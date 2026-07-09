<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import Badge from '$lib/Badge.svelte';
	import { apiUrl } from '$lib/api';
	import { formatRelative as fmtRelativeMs } from '$lib/time';
	import Icon from '@iconify/svelte';

	type FlowStatus = 'idle' | 'running' | 'warning' | 'error';

	interface Flow {
		name: string;
		display_name: string | null;
		description: string | null;
		tags: string[];
		require_leader_election: boolean;
		task_count: number;
		source: string;
		started_at: string | null;
		last_event_at: string | null;
		last_warning_at: string | null;
		last_error_at: string | null;
		events_total: number;
		warnings_total: number;
		errors_total: number;
		status: FlowStatus;
	}

	interface FlowDetail {
		name: string;
		display_name: string | null;
		yaml: string;
	}

	interface FlowActivity {
		flow: string;
		task: string | null;
		task_type: string | null;
		level: 'info' | 'warning' | 'error';
		ts_ms: number;
		message: string;
		metrics: {
			flow: string;
			events_total: number;
			warnings_total: number;
			errors_total: number;
			last_event_at_ms: number | null;
			last_warning_at_ms: number | null;
			last_error_at_ms: number | null;
			status: FlowStatus;
		};
	}

	function label(flow: { name: string; display_name: string | null }): string {
		return flow.display_name ?? flow.name;
	}

	let flows = $state<Flow[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let nowTick = $state(Date.now());

	let search = $state('');

	let selected = $state<string | null>(null);
	let selectedDetail = $state<FlowDetail | null>(null);
	let selectedLoading = $state(false);
	let selectedError = $state<string | null>(null);
	let modalActivities = $state<FlowActivity[]>([]);
	const activityHistory = new Map<string, FlowActivity[]>();

	let sse: EventSource | null = null;
	let tickerId: ReturnType<typeof setInterval> | null = null;

	function applyActivity(a: FlowActivity) {
		const idx = flows.findIndex((f) => f.name === a.flow);
		if (idx === -1) return;
		const current = flows[idx];
		flows[idx] = {
			...current,
			events_total: a.metrics.events_total,
			warnings_total: a.metrics.warnings_total,
			errors_total: a.metrics.errors_total,
			last_event_at: a.metrics.last_event_at_ms
				? new Date(a.metrics.last_event_at_ms).toISOString()
				: current.last_event_at,
			last_warning_at: a.metrics.last_warning_at_ms
				? new Date(a.metrics.last_warning_at_ms).toISOString()
				: current.last_warning_at,
			last_error_at: a.metrics.last_error_at_ms
				? new Date(a.metrics.last_error_at_ms).toISOString()
				: current.last_error_at,
			status: a.metrics.status
		};
	}

	onMount(() => {
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

		sse = new EventSource(apiUrl('api/flows/stream'));
		sse.addEventListener('activity', (e) => {
			let activity: FlowActivity;
			try {
				activity = JSON.parse(e.data) as FlowActivity;
			} catch {
				return;
			}
			applyActivity(activity);
			const bucket = activityHistory.get(activity.flow) ?? [];
			bucket.push(activity);
			activityHistory.set(activity.flow, bucket);
			if (selected === activity.flow) {
				modalActivities = [...modalActivities, activity];
			}
		});

		tickerId = setInterval(() => (nowTick = Date.now()), 1000);

		return () => {
			sse?.close();
			if (tickerId !== null) clearInterval(tickerId);
		};
	});

	// Cache lets repeat clicks paint instantly and lets the modal open only
	// when the payload is ready, so the user never sees the empty-then-loading
	// flash the DAG + Prism pipeline was doing on first paint.
	const flowCache = new Map<string, FlowDetail>();
	// Tracks the most recent open request so late responses from a
	// previously-clicked flow don't overwrite the current one.
	let pendingFlow: string | null = null;

	async function openFlow(name: string) {
		pendingFlow = name;
		modalActivities = [...(activityHistory.get(name) ?? [])];
		const cached = flowCache.get(name);
		if (cached) {
			selected = name;
			selectedDetail = cached;
			selectedError = null;
			selectedLoading = false;
		} else {
			selectedLoading = true;
		}
		try {
			const response = await fetch(apiUrl(`api/flows/${encodeURIComponent(name)}`));
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			const detail: FlowDetail = await response.json();
			flowCache.set(name, detail);
			if (pendingFlow !== name) return;
			selected = name;
			selectedDetail = detail;
			selectedError = null;
		} catch (err) {
			if (pendingFlow !== name) return;
			selected = name;
			selectedError = err instanceof Error ? err.message : 'Failed to load flow';
		} finally {
			if (pendingFlow === name) selectedLoading = false;
		}
	}

	function closeFlow() {
		selected = null;
		selectedDetail = null;
		selectedError = null;
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
	const STATUS_RANK: Record<FlowStatus, number> = { error: 3, warning: 2, running: 1, idle: 0 };

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
		const matched = flows.filter(
			(flow) =>
				flow.name.toLowerCase().includes(term) ||
				(flow.display_name?.toLowerCase().includes(term) ?? false) ||
				(flow.description?.toLowerCase().includes(term) ?? false) ||
				flow.tags.some((tag) => tag.toLowerCase().includes(term))
		);
		const sign = sortDir === 'asc' ? 1 : -1;
		return matched.slice().sort((a, b) => sign * compareFlows(a, b, sortKey));
	});

	function formatRelative(ts: string | null, tick: number): string {
		if (!ts) return '—';
		const ms = new Date(ts).getTime();
		if (isNaN(ms)) return ts;
		return fmtRelativeMs(ms, tick);
	}
</script>

<svelte:head>
	<title>Flowgen | Flows Overview</title>
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
			<input type="text" placeholder="Search flows..." bind:value={search} />
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
			<span>Failed to load flows: {error}</span>
		</div>
	{:else if filtered.length === 0}
		<div class="rounded-lg border border-base-200 bg-base-100 p-8 text-center text-sm opacity-70">
			No flows found
		</div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-base-200 bg-base-100">
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
					{#each filtered as flow (flow.name)}
						<tr
							class="hover cursor-pointer"
							onclick={() => openFlow(flow.name)}
							ondblclick={() => goto(`${base}/flows/${encodeURIComponent(flow.name)}`)}
							onkeydown={(e) => {
								if (e.key === 'Enter' || e.key === ' ') openFlow(flow.name);
							}}
							tabindex="0"
							role="button"
						>
							<td class="whitespace-nowrap">
								<div class="font-medium">{label(flow)}</div>
								{#if flow.display_name}
									<div class="font-mono text-[10px] opacity-50">{flow.name}</div>
								{/if}
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
											<Badge>{tag}</Badge>
										{/each}
									</div>
								{/if}
							</td>
							<td>
								{#if flow.status === 'running'}
									<Badge variant="success">Running</Badge>
								{:else if flow.status === 'warning'}
									<Badge variant="warning">Warning</Badge>
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
</section>

{#if selected}
	<div
		class="fixed inset-0 z-40 flex items-center justify-center bg-black/40 p-4"
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
			class="flex h-[90vh] w-full max-w-[95vw] flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-lg"
		>
			<div class="flex items-center justify-between border-b border-base-200 px-4 py-2">
				<div class="flex items-center gap-2">
					<span class="text-sm font-medium leading-none">
						{selectedDetail?.display_name ?? selected}
					</span>
					{#if selectedDetail?.display_name}
						<span class="font-mono text-xs leading-none opacity-50">{selected}</span>
					{/if}
					<Badge>flow</Badge>
				</div>
				<div class="flex items-center gap-1">
					<div class="tooltip tooltip-left" data-tip="Open full page">
						<a
							href="{base}/flows/{encodeURIComponent(selected)}"
							class="btn btn-ghost btn-sm btn-circle"
							aria-label="Open full page"
						>
							<Icon icon="tabler:external-link" class="h-6 w-6" />
						</a>
					</div>
					<div class="tooltip tooltip-left" data-tip="Close">
						<button
							type="button"
							class="btn btn-ghost btn-sm btn-circle"
							aria-label="Close"
							onclick={closeFlow}
						>
							<Icon icon="tabler:x" class="h-6 w-6" />
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
					<div class="alert alert-error m-4" role="alert">
						<span>{selectedError}</span>
					</div>
				{:else if selectedDetail}
					<FlowInspector yaml={selectedDetail.yaml} activities={modalActivities} />
				{/if}
			</div>
		</div>
	</div>
{/if}
