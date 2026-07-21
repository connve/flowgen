<script lang="ts">
	import { onMount } from 'svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import { apiUrl, type ConfigInfo } from '$lib/api';

	let yaml = $state('');
	let loading = $state(true);
	let error = $state<string | null>(null);

	onMount(async () => {
		try {
			const res = await fetch(apiUrl('api/config'));
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const body = (await res.json()) as ConfigInfo;
			yaml = body.yaml;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load config';
		} finally {
			loading = false;
		}
	});
</script>

<svelte:head>
	<title>System | Flowgen</title>
</svelte:head>

<section class="flex h-[calc(100vh-4rem)] min-w-0 flex-col overflow-hidden">
	<div class="shrink-0 border-b border-base-200 bg-base-100 px-6 pb-4 pt-6">
		<div class="flex items-center gap-1.5 text-sm">
			<span>System</span>
			<span class="opacity-40">/</span>
			<span class="font-mono">config</span>
		</div>
	</div>

	{#if loading}
		<div class="flex flex-1 items-center justify-center">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if error}
		<div class="p-6">
			<div class="alert alert-error" role="alert">
				<span>Failed to load config: {error}</span>
			</div>
		</div>
	{:else}
		<div class="flex min-h-0 flex-1 flex-col p-6">
			<div
				class="flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border border-base-300 bg-base-100"
			>
				<div class="flex h-10 shrink-0 items-center justify-between border-b border-base-200 bg-base-100 px-4">
					<span class="text-xs font-medium uppercase opacity-70">YAML</span>
					<CopyButton text={yaml} label="Copy" />
				</div>
				<div class="min-h-0 flex-1 overflow-auto bg-base-200">
					<ResourceViewer content={yaml} extension="yaml" />
				</div>
			</div>
		</div>
	{/if}
</section>
