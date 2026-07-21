<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import { apiUrl, type ResourceContent } from '$lib/api';

	let content = $state<ResourceContent | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);

	let resourceKey = $derived(page.params.key ?? '');
	let keySegments = $derived(resourceKey.split('/'));
	let folderSegments = $derived(keySegments.slice(0, -1));
	let leafName = $derived(keySegments[keySegments.length - 1] ?? '');

	onMount(async () => {
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
		<h1 class="text-lg font-medium">{leafName}</h1>
	</div>

	{#if loading}
		<div class="flex justify-center py-12">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if error}
		<div class="alert alert-error" role="alert">
			<span>Failed to load resource: {error}</span>
		</div>
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
