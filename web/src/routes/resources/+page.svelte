<script lang="ts">
	import { onMount } from 'svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import X from 'lucide-svelte/icons/x';

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

	let resources = $state<Resource[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let search = $state('');

	let selected = $state<string | null>(null);
	let selectedContent = $state<ResourceContent | null>(null);
	let selectedLoading = $state(false);
	let selectedError = $state<string | null>(null);

	onMount(async () => {
		try {
			const response = await fetch('api/resources');
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			resources = await response.json();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load resources';
		} finally {
			loading = false;
		}
	});

	let filtered = $derived(
		resources.filter((r) => {
			const term = search.toLowerCase();
			return r.key.toLowerCase().includes(term);
		})
	);

	async function openResource(key: string) {
		selected = key;
		selectedContent = null;
		selectedError = null;
		selectedLoading = true;
		try {
			const response = await fetch(`api/resources/${key}`);
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			selectedContent = await response.json();
		} catch (err) {
			selectedError = err instanceof Error ? err.message : 'Failed to load resource';
		} finally {
			selectedLoading = false;
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
	<title>Resources | CONNVE</title>
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
			<input type="text" placeholder="Search resources..." bind:value={search} />
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
	{:else if filtered.length === 0}
		<div class="rounded-lg border border-base-200 bg-base-100 p-8 text-center text-sm opacity-70">
			No resources found.
		</div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-base-200 bg-base-100">
			<table class="table bg-base-100">
				<thead class="bg-base-100 text-xs uppercase tracking-wide opacity-60">
					<tr>
						<th>Key</th>
						<th>Type</th>
						<th class="text-right">Size</th>
					</tr>
				</thead>
				<tbody>
					{#each filtered as resource (resource.key)}
						<tr
							class="hover cursor-pointer"
							onclick={() => openResource(resource.key)}
							onkeydown={(e) => {
								if (e.key === 'Enter' || e.key === ' ') openResource(resource.key);
							}}
							tabindex="0"
							role="button"
						>
							<td class="font-medium">{resource.key}</td>
							<td>
								{#if resource.extension}
									<span class="badge badge-outline badge-sm">{resource.extension}</span>
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
</section>

{#if selected}
	<div
		class="fixed inset-0 z-40 flex items-center justify-center bg-black/40 p-4"
		role="dialog"
		aria-modal="true"
		aria-label="Resource viewer"
		onclick={closeResource}
		onkeydown={(e) => {
			if (e.key === 'Escape') closeResource();
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
					{#if selectedContent?.extension}
						<span class="badge badge-outline badge-sm">{selectedContent.extension}</span>
					{/if}
				</div>
				<button
					type="button"
					class="btn btn-ghost btn-sm btn-circle"
					aria-label="Close"
					onclick={closeResource}
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
