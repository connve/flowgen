<script lang="ts">
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';

	onMount(() => {
		goto('/docs/getting-started/why-flowgen', { replaceState: true });
	});
</script>

<svelte:head>
	<meta http-equiv="refresh" content="0; url=/docs/getting-started/why-flowgen" />
	<link rel="canonical" href="/docs/getting-started/why-flowgen" />
</svelte:head>
