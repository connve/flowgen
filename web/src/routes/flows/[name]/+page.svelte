<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import ArrowLeft from 'lucide-svelte/icons/arrow-left';
	import ResourceViewer from '$lib/ResourceViewer.svelte';

	interface FlowDetail {
		name: string;
		yaml: string;
	}

	let detail = $state<FlowDetail | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);

	let flowName = $derived(page.params.name);

	onMount(async () => {
		try {
			const response = await fetch(`../api/flows/${encodeURIComponent(flowName)}`);
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			detail = await response.json();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load flow';
		} finally {
			loading = false;
		}
	});
</script>

<svelte:head>
	<title>{flowName} | Flowgen</title>
</svelte:head>

<section class="p-6">
	<div class="mb-4 flex items-center gap-3">
		<a href="{base}/" class="btn btn-ghost btn-sm gap-1">
			<ArrowLeft class="h-4 w-4" />
			Flows
		</a>
		<h1 class="font-mono text-lg font-medium">{flowName}</h1>
	</div>

	{#if loading}
		<div class="flex justify-center py-12">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if error}
		<div class="alert alert-error" role="alert">
			<span>Failed to load flow: {error}</span>
		</div>
	{:else if detail}
		<div class="grid gap-6 lg:grid-cols-2">
			<div class="rounded-lg border border-base-200 bg-base-100">
				<div class="border-b border-base-200 px-4 py-2 text-xs uppercase tracking-wide opacity-60">
					DAG
				</div>
				<div class="p-6 text-sm opacity-60">
					DAG visualization is not implemented yet — coming after per-event tracking.
				</div>
			</div>
			<div class="overflow-hidden rounded-lg border border-base-200 bg-base-100">
				<div class="border-b border-base-200 px-4 py-2 text-xs uppercase tracking-wide opacity-60">
					flow.yaml
				</div>
				<div class="max-h-[70vh] overflow-auto bg-base-200">
					<ResourceViewer content={detail.yaml} extension="yaml" />
				</div>
			</div>
		</div>
	{/if}
</section>
