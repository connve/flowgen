<script lang="ts">
	import { onMount } from 'svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { apiUrl, errorMessage, failure, type ConfigInfo } from '$lib/api';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';

	let yaml = $state('');
	let loading = $state(true);
	let error = $state<string | null>(null);

	onMount(async () => {
		try {
			const res = await fetch(apiUrl('api/config'));
			if (!res.ok) throw await failure(res);
			const body = (await res.json()) as ConfigInfo;
			yaml = body.yaml;
		} catch (err) {
			error = errorMessage(err, m.common_config_unavailable());
		} finally {
			loading = false;
		}
	});
</script>

<svelte:head>
	<title>{pageTitle(m.nav_system())}</title>
</svelte:head>

<section class="flex h-[calc(100vh-4rem)] min-w-0 flex-col overflow-hidden">
	<div class="shrink-0 border-b border-base-200 bg-base-100 px-6 pb-4 pt-6">
		<div class="flex items-center gap-1.5 text-sm">
			<span>{m.system_breadcrumb()}</span>
			<span class="opacity-40">/</span>
			<span class="font-mono">config</span>
		</div>
	</div>

	{#if loading}
		<div class="flex flex-1 items-center justify-center">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if error}
		<StateMessage tone="oops" title={m.system_config_load_failed_title()} message={error} />
	{:else}
		<div class="flex min-h-0 flex-1 flex-col p-6">
			<div
				class="flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border border-base-300 bg-base-100"
			>
				<div class="flex h-10 shrink-0 items-center justify-between border-b border-base-200 bg-base-100 px-4">
					<span class="text-xs font-medium uppercase opacity-70">YAML</span>
					<CopyButton text={yaml} label={m.common_copy()} />
				</div>
				<div class="min-h-0 flex-1 overflow-auto bg-base-200">
					<ResourceViewer content={yaml} extension="yaml" />
				</div>
			</div>
		</div>
	{/if}
</section>
