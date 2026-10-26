<script lang="ts">
	import { page } from '$app/state';
	import StateMessage from '$lib/StateMessage.svelte';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';
</script>

<svelte:head>
	<title>{pageTitle(String(page.status))}</title>
</svelte:head>

<div class="flex h-full flex-col p-6">
	{#if page.status === 404}
		<StateMessage
			title={m.common_not_found()}
			message={page.error?.message ?? m.common_not_found_message()}
		/>
	{:else}
		<StateMessage
			tone="oops"
			message={page.error?.message ?? m.error_page_status({ status: page.status })}
		/>
	{/if}
</div>
