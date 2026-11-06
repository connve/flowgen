<script lang="ts">
	import type { Snippet } from 'svelte';

	let { title, children }: { title?: string; children: Snippet } = $props();
</script>

<section class="space-y-2">
	{#if title}
		<h3 class="text-xs font-medium text-base-content/50">{title}</h3>
	{/if}
	{@render children()}
</section>
