<script lang="ts">
	import { base } from '$app/paths';
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';

	onMount(() => {
		goto(`${base}/getting-started/why-flowgen`, { replaceState: true });
	});
</script>

<svelte:head>
	<meta http-equiv="refresh" content="0; url=/docs/flowgen/getting-started/why-flowgen" />
	<link rel="canonical" href="/docs/flowgen/getting-started/why-flowgen" />
</svelte:head>
