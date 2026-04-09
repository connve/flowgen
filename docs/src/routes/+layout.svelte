<script lang="ts">
	import '../app.css';

	let { children } = $props();
</script>

<svelte:head>
	<title>Flowgen Documentation</title>
	<meta name="description" content="Flowgen — open-source data activation engine written in Rust" />
</svelte:head>

{@render children()}
