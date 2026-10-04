<script lang="ts">
	import { page } from '$app/state';
	import StateMessage from '$lib/StateMessage.svelte';
</script>

<svelte:head>
	<title>{page.status} | Flowgen</title>
</svelte:head>

<div class="flex h-full flex-col p-6">
	{#if page.status === 404}
		<StateMessage title="Not found" message={page.error?.message ?? 'Nothing lives at this address.'} />
	{:else}
		<StateMessage tone="oops" message={page.error?.message ?? `HTTP ${page.status}`} />
	{/if}
</div>
