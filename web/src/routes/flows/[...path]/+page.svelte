<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onDestroy, onMount } from 'svelte';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import Icon from '@iconify/svelte';
	import { apiUrl, type AuthoringTarget, type ChangeSummary, type FlowDetail } from '$lib/api';
	import { activitiesFor, releaseFlowSubscription } from '$lib/activityStore.svelte';
	import {
		authoringTargets,
		editUrl,
		flowFilePath,
		pendingChanges,
		targetOf,
		touchesFlow
	} from '$lib/changes';

	let detail = $state<FlowDetail | null>(null);
	let pending = $state<ChangeSummary[]>([]);
	let targets = $state<AuthoringTarget[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);

	let flowPath = $derived(page.params.path ?? '');
	let editable = $derived(targetOf(targets, flowFilePath(flowPath)) !== undefined);
	let activities = $derived(activitiesFor(flowPath));
	// Split path into (folder segments, leaf) so the breadcrumb can render
	// folder segments as visual context and the leaf as the current page.
	let pathSegments = $derived(flowPath.split('/'));
	let folderSegments = $derived(pathSegments.slice(0, -1));
	let leafName = $derived(pathSegments[pathSegments.length - 1] ?? '');

	$effect(() => {
		const identity = flowPath;
		pendingChanges().then((changes) => {
			if (identity === flowPath) pending = changes.filter((change) => touchesFlow(change, identity));
		});
	});

	onMount(() => {
		authoringTargets().then((found) => (targets = found));
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

	onDestroy(() => {
		releaseFlowSubscription();
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
	<div class="mb-4 flex items-center justify-between gap-2">
		<h1 class="text-lg font-medium">{detail?.display_name ?? leafName}</h1>
		{#if targets.length > 0 && detail}
			{#if editable}
				<a href={editUrl(base, flowFilePath(flowPath))} class="btn btn-ghost btn-sm">
					<Icon icon="tabler:pencil" class="h-4 w-4" />
					Edit
				</a>
			{:else}
				<div class="tooltip tooltip-left" data-tip="Managed in the repository">
					<button type="button" class="btn btn-ghost btn-sm" disabled>
						<Icon icon="tabler:pencil" class="h-4 w-4" />
						Edit
					</button>
				</div>
			{/if}
		{/if}
	</div>

	{#each pending as change (change.id)}
		<a
			href="{base}/changes/{encodeURIComponent(change.id)}"
			class="mb-3 flex items-center gap-2 rounded-lg border border-warning/40 bg-warning/5 px-3 py-2 text-sm hover:bg-warning/10"
		>
			<Icon icon="tabler:git-pull-request" class="h-4 w-4 text-warning" />
			<span>Pending change: {change.title}</span>
			<span class="text-xs opacity-60">by {change.proposedBy}</span>
			<span class="ml-auto text-primary">Review</span>
		</a>
	{/each}

	{#if loading}
		<div class="flex justify-center py-12">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if error}
		<StateMessage tone="oops" title="Failed to load flow" message={error} />
	{:else if detail}
		<div class="flex h-[calc(100vh-10rem)] flex-col overflow-hidden rounded-lg border border-base-300 bg-base-100">
			<FlowInspector yaml={detail.yaml} {activities} />
		</div>
	{/if}
</section>
