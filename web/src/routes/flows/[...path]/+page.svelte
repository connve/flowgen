<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import { apiUrl, type FlowDetail } from '$lib/api';
	import { activitiesFor } from '$lib/activityStore.svelte';

	let detail = $state<FlowDetail | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);

	let flowPath = $derived(page.params.path ?? '');
	let activities = $derived(activitiesFor(flowPath));
	// Split path into (folder segments, leaf) so the breadcrumb can render
	// folder segments as visual context and the leaf as the current page.
	let pathSegments = $derived(flowPath.split('/'));
	let folderSegments = $derived(pathSegments.slice(0, -1));
	let leafName = $derived(pathSegments[pathSegments.length - 1] ?? '');

	onMount(() => {
		fetch(apiUrl(`api/flows/${flowPath.split('/').map(encodeURIComponent).join('/')}`))
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
	<title>{detail?.display_name ?? flowPath} | Flowgen</title>
</svelte:head>

<section class="p-6">
	<div class="mb-1 flex items-center gap-1.5 text-sm">
		<a href="{base}/" class="text-primary hover:underline">Flows</a>
		{#each folderSegments as segment}
			<span class="opacity-40">/</span>
			<span class="font-mono opacity-70">{segment}</span>
		{/each}
		<span class="opacity-40">/</span>
		<span class="font-mono">{leafName}</span>
	</div>
	<h1 class="mb-4 text-lg font-medium">{detail?.display_name ?? leafName}</h1>

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
