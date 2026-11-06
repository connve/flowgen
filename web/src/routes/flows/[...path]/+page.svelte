<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onDestroy } from 'svelte';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import FlowDetails from '$lib/flow/FlowDetails.svelte';
	import type { Activity } from '$lib/flow/ActivityDetails.svelte';
	import EditButton from '$lib/EditButton.svelte';
	import PendingChanges from '$lib/PendingChanges.svelte';
	import { activitiesFor, releaseFlowSubscription } from '$lib/activityStore.svelte';
	import { editUrl, flowFilePath, targetOf } from '$lib/changes';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';
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
	let resource = $state<string | null>(null);
	let activity = $state<Activity | null>(null);

	onDestroy(() => {
		releaseFlowSubscription();
	});
</script>

<svelte:head>
	<title>{pageTitle(data.detail.display_name ?? data.identity, m.nav_flows())}</title>
</svelte:head>

<section class="flex h-[calc(100vh-4rem)] min-w-0">
<div class="flex min-w-0 flex-1 flex-col p-6">
	<div class="mb-4 shrink-0">
		<div class="flex min-h-8 items-center justify-between gap-2">
			<div class="min-w-0">
				<div class="flex items-center gap-1 text-xs text-base-content/50">
					<a href="{base}/" class="hover:text-base-content">{m.flow_breadcrumb_flows()}</a>
					{#each folderSegments as segment, i (i)}
						<span class="opacity-60">/</span>
						<span>{segment}</span>
					{/each}
				</div>
				<h1 class="truncate text-[0.9375rem] font-semibold">{data.detail.display_name ?? leafName}</h1>
			</div>
			<div class="flex items-center gap-1">
				{#if data.targets.length > 0}
					<EditButton href={editHref} />
				{/if}
			</div>
		</div>
	</div>

	<PendingChanges changes={data.pending} />

	<div class="flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100">
		<FlowInspector
			yaml={data.detail.yaml}
			{activities}
			bind:resource
			bind:activity
			focusTask={page.url.searchParams.get('task')}
		/>
	</div>
</div>
<FlowDetails bind:resource bind:activity />
</section>
