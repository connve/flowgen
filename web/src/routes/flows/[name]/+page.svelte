<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import Icon from '@iconify/svelte';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import { apiUrl } from '$lib/api';
	import { activitiesFor } from '$lib/activityStore.svelte';

	interface FlowDetail {
		name: string;
		display_name: string | null;
		yaml: string;
	}

	let detail = $state<FlowDetail | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);

	let flowName = $derived(page.params.name ?? '');
	let activities = $derived(activitiesFor(flowName));

	onMount(() => {
		fetch(apiUrl(`api/flows/${encodeURIComponent(flowName)}`))
			.then((r) => {
				if (!r.ok) throw new Error(`HTTP ${r.status}`);
				return r.json();
			})
			.then((data) => {
				detail = data;
			})
			.catch((err) => {
				error = err instanceof Error ? err.message : 'Failed to load flow';
			})
			.finally(() => {
				loading = false;
			});
	});
</script>

<svelte:head>
	<title>{detail?.display_name ?? flowName} | Flowgen</title>
</svelte:head>

<section class="p-6">
	<div class="mb-4 flex items-center gap-3">
		<a href="{base}/" class="btn btn-ghost btn-sm gap-1">
			<Icon icon="tabler:arrow-left" class="h-6 w-6" />
			Flows
		</a>
		<div class="flex items-baseline gap-2">
			<h1 class="text-lg font-medium">{detail?.display_name ?? flowName}</h1>
			{#if detail?.display_name}
				<span class="font-mono text-xs opacity-50">{flowName}</span>
			{/if}
		</div>
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
		<div class="flex h-[calc(100vh-10rem)] flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100">
			<FlowInspector yaml={detail.yaml} {activities} />
		</div>
	{/if}
</section>
