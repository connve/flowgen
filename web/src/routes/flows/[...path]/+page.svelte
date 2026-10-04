<script lang="ts">
	import { base } from '$app/paths';
	import { onDestroy } from 'svelte';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import EditButton from '$lib/EditButton.svelte';
	import PendingChanges from '$lib/PendingChanges.svelte';
	import { activitiesFor, releaseFlowSubscription } from '$lib/activityStore.svelte';
	import { editUrl, flowFilePath, targetOf } from '$lib/changes';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let editHref = $derived(
		targetOf(data.targets, flowFilePath(data.identity))
			? editUrl(base, flowFilePath(data.identity))
			: null
	);
	let activities = $derived(activitiesFor(data.identity));
	let pathSegments = $derived(data.identity.split('/'));
	let folderSegments = $derived(pathSegments.slice(0, -1));
	let leafName = $derived(pathSegments[pathSegments.length - 1] ?? '');

	onDestroy(() => {
		releaseFlowSubscription();
	});
</script>

<svelte:head>
	<title>{data.detail.display_name ?? data.identity} | Flowgen</title>
</svelte:head>

<section class="p-6">
	<div class="mb-4">
		<div class="flex min-h-8 items-center justify-between gap-2">
			<div class="flex items-center gap-1.5 text-sm">
				<a href="{base}/" class="text-primary hover:underline">Flows</a>
				{#each folderSegments as segment, i (i)}
					<span class="opacity-40">/</span>
					<span class="font-mono opacity-70">{segment}</span>
				{/each}
				<span class="opacity-40">/</span>
				<span class="font-mono">{leafName}</span>
			</div>
			<div class="flex items-center gap-1">
				{#if data.targets.length > 0}
					<EditButton href={editHref} />
				{/if}
			</div>
		</div>
		{#if data.detail.display_name}
			<h1 class="text-lg font-medium">{data.detail.display_name}</h1>
		{/if}
	</div>

	<PendingChanges changes={data.pending} />

	<div class="flex h-[calc(100vh-10rem)] flex-col overflow-hidden rounded-lg border border-base-300 bg-base-100">
		<FlowInspector yaml={data.detail.yaml} {activities} />
	</div>
</section>
