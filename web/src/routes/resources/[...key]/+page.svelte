<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import Icon from '@iconify/svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import Badge from '$lib/Badge.svelte';
	import { apiUrl } from '$lib/api';

	interface ResourceContent {
		key: string;
		extension: string | null;
		content: string;
	}

	let content = $state<ResourceContent | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let copied = $state(false);
	let copyTimeout: ReturnType<typeof setTimeout> | null = null;

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

	async function copyContent() {
		if (!content) return;
		try {
			await navigator.clipboard.writeText(content.content);
			copied = true;
			if (copyTimeout) clearTimeout(copyTimeout);
			copyTimeout = setTimeout(() => (copied = false), 1500);
		} catch {
			// Clipboard refused — silent no-op.
		}
	}
</script>

<svelte:head>
	<title>Flowgen | {resourceKey}</title>
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
		<button
			type="button"
			class="btn btn-ghost btn-sm gap-1"
			aria-label={copied ? 'Copied' : 'Copy'}
			onclick={copyContent}
			disabled={!content}
		>
			{#if copied}
				<Icon icon="tabler:check" class="h-6 w-6 text-primary" />
				<span class="text-primary">Copied</span>
			{:else}
				<Icon icon="tabler:copy" class="h-6 w-6" />
				<span>Copy</span>
			{/if}
		</button>
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
