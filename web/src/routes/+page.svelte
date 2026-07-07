<script lang="ts">
	import { base } from '$app/paths';
	import { onMount } from 'svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import X from 'lucide-svelte/icons/x';
	import MoreHorizontal from 'lucide-svelte/icons/more-horizontal';
	import ArrowUpRight from 'lucide-svelte/icons/arrow-up-right';

	type FlowStatus = 'ok' | 'error' | 'unknown';

	interface Flow {
		name: string;
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
		yaml: string;
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
			const response = await fetch('api/flows');
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

	async function openFlow(name: string) {
		selected = name;
		selectedDetail = null;
		selectedError = null;
		selectedLoading = true;
		try {
			const response = await fetch(`api/flows/${encodeURIComponent(name)}`);
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			selectedDetail = await response.json();
		} catch (err) {
			selectedError = err instanceof Error ? err.message : 'Failed to load flow';
		} finally {
			selectedLoading = false;
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

	let filtered = $derived(
		flows.filter((flow) => {
			const term = search.toLowerCase();
			return (
				flow.name.toLowerCase().includes(term) ||
				(flow.description?.toLowerCase().includes(term) ?? false) ||
				flow.tags.some((tag) => tag.toLowerCase().includes(term))
			);
		})
	);

	function formatLastRun(last_run: string | null): string {
		if (!last_run) return '—';
		const d = new Date(last_run);
		return isNaN(d.getTime()) ? last_run : d.toLocaleString();
	}
</script>

<svelte:head>
	<title>Flowgen | CONNVE</title>
</svelte:head>

<svelte:window on:keydown={onKeydown} />

<section class="p-6">
	<div class="mb-4 flex items-center justify-end">
		<label class="input input-bordered input-sm flex items-center gap-2 bg-base-100">
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
						<th>Name</th>
						<th>Description</th>
						<th>Tags</th>
						<th>Status</th>
						<th>Last run</th>
						<th class="text-right">Actions</th>
					</tr>
				</thead>
				<tbody>
					{#each filtered as flow (flow.name)}
						<tr
							class="hover cursor-pointer"
							onclick={() => openFlow(flow.name)}
							onkeydown={(e) => {
								if (e.key === 'Enter' || e.key === ' ') openFlow(flow.name);
							}}
							tabindex="0"
							role="button"
						>
							<td class="whitespace-nowrap font-medium">{flow.name}</td>
							<td class="max-w-md text-sm">
								{flow.description ?? '—'}
							</td>
							<td>
								{#if flow.tags.length === 0}
									<span class="opacity-50">—</span>
								{:else}
									<div class="flex flex-wrap gap-1">
										{#each flow.tags as tag}
											<span class="badge badge-outline badge-sm">{tag}</span>
										{/each}
									</div>
								{/if}
							</td>
							<td>
								{#if flow.status === 'ok'}
									<span class="inline-flex items-center gap-1.5 text-xs" title="Running">
										<span class="inline-block h-2.5 w-2.5 rounded-full bg-success"></span>
										<span>Ok</span>
									</span>
								{:else if flow.status === 'error'}
									<span
										class="inline-flex items-center gap-1.5 text-xs"
										title="Supervisor exited"
									>
										<span class="inline-block h-2.5 w-2.5 rounded-full bg-error"></span>
										<span>Error</span>
									</span>
								{:else}
									<span
										class="inline-flex items-center gap-1.5 text-xs opacity-70"
										title="No status yet"
									>
										<span class="inline-block h-2.5 w-2.5 rounded-full bg-base-300"></span>
										<span>Unknown</span>
									</span>
								{/if}
							</td>
							<td class="whitespace-nowrap text-sm opacity-70">
								{formatLastRun(flow.last_run)}
							</td>
							<td onclick={(e) => e.stopPropagation()}>
								<div class="flex justify-end">
									<div class="dropdown dropdown-end">
										<button
											type="button"
											class="btn btn-ghost btn-sm btn-circle"
											aria-label="Actions"
											aria-haspopup="menu"
										>
											<MoreHorizontal class="h-4 w-4" />
										</button>
										<ul
											class="menu dropdown-content menu-sm z-10 mt-1 w-44 rounded-lg border border-base-200 bg-base-100 p-1 shadow-lg"
											role="menu"
										>
											<li>
												<a
													href="{base}/flows/{encodeURIComponent(flow.name)}"
													role="menuitem"
												>
													<ArrowUpRight class="h-4 w-4" />
													Open details
												</a>
											</li>
										</ul>
									</div>
								</div>
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
		onclick={closeFlow}
		onkeydown={(e) => {
			if (e.key === 'Escape') closeFlow();
		}}
		tabindex="-1"
	>
		<div
			class="flex max-h-[90vh] w-full max-w-6xl flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-lg"
			role="document"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => e.stopPropagation()}
			tabindex="-1"
		>
			<div class="flex items-center justify-between border-b border-base-200 px-4 py-3">
				<div class="flex items-center gap-2">
					<span class="font-mono text-sm">{selected}</span>
					<span class="badge badge-outline badge-sm">yaml</span>
				</div>
				<button
					type="button"
					class="btn btn-ghost btn-sm btn-circle"
					aria-label="Close"
					onclick={closeFlow}
				>
					<X class="h-4 w-4" />
				</button>
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
				{:else if selectedDetail}
					<div class="min-h-0 flex-1 overflow-auto">
						<ResourceViewer content={selectedDetail.yaml} extension="yaml" />
					</div>
				{/if}
			</div>
		</div>
	</div>
{/if}
