<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import Icon from '@iconify/svelte';
	import {
		apiUrl,
		type AuthoringTarget,
		type ChangeSummary,
		type ResourceContent
	} from '$lib/api';
	import {
		authoringTargets,
		editUrl,
		pendingChanges,
		targetOf,
		touchesResource
	} from '$lib/changes';

	let content = $state<ResourceContent | null>(null);
	let targets = $state<AuthoringTarget[]>([]);
	let pending = $state<ChangeSummary[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);

	let resourceKey = $derived(page.params.key ?? '');
	let keySegments = $derived(resourceKey.split('/'));
	let folderSegments = $derived(keySegments.slice(0, -1));
	let leafName = $derived(keySegments[keySegments.length - 1] ?? '');
	let editable = $derived(targetOf(targets, `resources/${resourceKey}`) !== undefined);

	$effect(() => {
		const key = resourceKey;
		pendingChanges().then((changes) => {
			if (key === resourceKey) pending = changes.filter((change) => touchesResource(change, key));
		});
	});

	onMount(async () => {
		authoringTargets().then((found) => (targets = found));
		try {
			const response = await fetch(apiUrl(`api/resources/${resourceKey}`));
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			content = await response.json();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load resource';
		} finally {
			loading = false;
		}
	});
</script>

<svelte:head>
	<title>{resourceKey} | Flowgen</title>
</svelte:head>

<section class="p-6">
	<div class="mb-4">
		<div class="mb-1 flex items-center gap-1.5 text-sm">
			<a href="{base}/resources" class="text-primary hover:underline">Resources</a>
			{#each folderSegments as segment}
				<span class="opacity-40">/</span>
				<span class="font-mono opacity-70">{segment}</span>
			{/each}
			<span class="opacity-40">/</span>
			<span class="font-mono">{leafName}</span>
		</div>
		<div class="flex items-center justify-between gap-2">
			<h1 class="text-lg font-medium">{leafName}</h1>
			{#if targets.length > 0 && content}
				{#if editable}
					<a href={editUrl(base, `resources/${resourceKey}`)} class="btn btn-ghost btn-sm">
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
		<StateMessage tone="oops" title="Failed to load resource" message={error} />
	{:else if content}
		<div
			class="flex h-[calc(100vh-10rem)] flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100"
		>
			<div class="flex h-10 shrink-0 items-center justify-between border-b border-base-200 bg-base-100 px-4">
				<span class="text-xs font-medium uppercase opacity-70">
					{content.extension ?? 'File'}
				</span>
				<CopyButton text={content.content} label="Copy" />
			</div>
			<div class="min-h-0 flex-1 overflow-auto bg-base-200">
				<ResourceViewer content={content.content} extension={content.extension} />
			</div>
		</div>
	{/if}
</section>
