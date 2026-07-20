<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import Icon from '@iconify/svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import Badge from '$lib/Badge.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import { apiUrl, type ResourceContent } from '$lib/api';

	let content = $state<ResourceContent | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);

	let resourceKey = $derived(page.params.key ?? '');

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
	<div class="mb-4 flex items-center justify-between gap-3">
		<div class="flex items-center gap-3">
			<a href="{base}/resources" class="btn btn-ghost btn-sm gap-1">
				<Icon icon="tabler:arrow-left" class="h-6 w-6" />
				Resources
			</a>
			<h1 class="font-mono text-sm font-medium">{resourceKey}</h1>
			{#if content?.extension}
				<Badge>{content.extension}</Badge>
			{/if}
		</div>
		<CopyButton text={content?.content} label="Copy" />
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
			class="h-[calc(100vh-10rem)] overflow-auto rounded-lg border border-base-200 bg-base-200"
		>
			<ResourceViewer content={content.content} extension={content.extension} />
		</div>
	{/if}
</section>
