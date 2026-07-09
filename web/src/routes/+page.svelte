<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import Badge from '$lib/Badge.svelte';
	import { apiUrl } from '$lib/api';
	import Icon from '@iconify/svelte';

	type FlowStatus = 'ok' | 'error' | 'unknown';

	interface Flow {
		name: string;
		display_name: string | null;
		description: string | null;
		tags: string[];
		require_leader_election: boolean;
		task_count: number;
		source: string;
		last_run: string | null;
		status: FlowStatus;
	}

	interface FlowDetail {
		name: string;
		display_name: string | null;
		yaml: string;
	}

	function label(flow: { name: string; display_name: string | null }): string {
		return flow.display_name ?? flow.name;
	}

	let flows = $state<Flow[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);

	let search = $state('');

	let selected = $state<string | null>(null);
	let selectedDetail = $state<FlowDetail | null>(null);
	let selectedLoading = $state(false);
	let selectedError = $state<string | null>(null);

	onMount(async () => {
		try {
			const response = await fetch(apiUrl('api/flows'));
			if (!response.ok) {
				throw new Error(`HTTP ${response.status}`);
			}
			flows = await response.json();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load flows';
		} finally {
			loading = false;
		}
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

	let sortDir = $state<'asc' | 'desc'>('asc');

	function toggleSort() {
		sortDir = sortDir === 'asc' ? 'desc' : 'asc';
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
		return matched
			.slice()
			.sort((a, b) => sign * label(a).localeCompare(label(b), undefined, { sensitivity: 'base' }));
	});

	function formatLastRun(last_run: string | null): string {
		if (!last_run) return '—';
		const d = new Date(last_run);
		return isNaN(d.getTime()) ? last_run : d.toLocaleString();
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
			No flows found.
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
								onclick={toggleSort}
								aria-label="Sort by name {sortDir === 'asc' ? 'descending' : 'ascending'}"
							>
								<span>Name</span>
								{#if sortDir === 'asc'}
									<Icon icon="tabler:arrow-up" class="h-5 w-5 opacity-70" />
								{:else}
									<Icon icon="tabler:arrow-down" class="h-5 w-5 opacity-70" />
								{/if}
							</button>
						</th>
						<th>Description</th>
						<th>Tags</th>
						<th>Status</th>
						<th>Last run</th>
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
								{#if flow.status === 'ok'}
									<Badge variant="success">Running</Badge>
								{:else if flow.status === 'error'}
									<Badge variant="error">Error</Badge>
								{:else}
									<Badge>Idle</Badge>
								{/if}
							</td>
							<td class="whitespace-nowrap text-sm opacity-70">
								{formatLastRun(flow.last_run)}
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
			class="flex max-h-[90vh] w-full max-w-[95vw] flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-lg"
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
					<a
						href="{base}/flows/{encodeURIComponent(selected)}"
						class="btn btn-ghost btn-sm btn-circle"
						aria-label="Open full page"
						title="Open full page"
					>
						<Icon icon="tabler:external-link" class="h-6 w-6" />
					</a>
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
					<FlowInspector yaml={selectedDetail.yaml} />
				{/if}
			</div>
		</div>
	</div>
{/if}
