<script lang="ts">
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import type { ChangeSummary } from '$lib/api';

	let { changes }: { changes: ChangeSummary[] } = $props();
</script>

{#each changes as change (change.id)}
	<a
		href="{base}/changes/{encodeURIComponent(change.id)}"
		class="mb-3 flex items-center gap-2 rounded-lg border border-warning/40 bg-warning/5 px-3 py-2 text-sm hover:bg-warning/10"
	>
		<Icon icon="tabler:git-pull-request" class="h-4 w-4 text-attention" />
		<span>Pending change: {change.title}</span>
		<span class="text-xs opacity-60">by {change.proposedBy}</span>
		<span class="ml-auto text-primary">Review</span>
	</a>
{/each}
