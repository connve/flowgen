<script lang="ts">
	import { onMount } from 'svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import { CopyButton, Loading, StateMessage } from '@connve/ui';
	import AppConfigFields from '$lib/AppConfigFields.svelte';
	import ConfigViewSwitch from '$lib/ConfigViewSwitch.svelte';
	import { configView } from '$lib/configView.svelte';
	import { errorMessage, getConfig } from '$lib/api';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';

	let yaml = $state('');
	let loading = $state(true);
	let error = $state<string | null>(null);

	onMount(async () => {
		try {
			yaml = (await getConfig()).data.yaml;
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

<section class="flex h-[calc(100vh-4rem)] min-w-0 flex-col overflow-clip">
	<div class="shrink-0 bg-base-100 px-6 pb-4 pt-6">
		<h1 class="flex min-h-8 items-center text-[0.9375rem] font-semibold">{m.system_breadcrumb()}</h1>
	</div>

	{#if loading}
		<Loading />
	{:else if error}
		<StateMessage tone="oops" title={m.system_config_load_failed_title()} message={error} />
	{:else}
		<div class="flex min-h-0 flex-1 flex-col px-6 pb-6">
			<div
				class="flex min-h-0 flex-1 flex-col overflow-clip rounded-lg border border-base-200 bg-base-100"
			>
				<div class="flex h-10 shrink-0 items-center justify-between bg-base-100 px-4">
					<span class="text-xs font-medium text-base-content/50">{m.system_config_heading()}</span>
					<div class="flex items-center gap-2">
						<ConfigViewSwitch />
						<CopyButton text={yaml} label={m.common_copy()} />
					</div>
				</div>
				<div
					class="min-h-0 flex-1 overflow-auto {configView.value === 'fields' ? 'bg-base-100' : 'bg-base-200'}"
				>
					{#if configView.value === 'fields'}
						<AppConfigFields {yaml} />
					{:else}
						<ResourceViewer content={yaml} extension="yaml" />
					{/if}
				</div>
			</div>
		</div>
	{/if}
</section>
