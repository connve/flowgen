<script lang="ts">
	import Icon from '@iconify/svelte';
	import { actor } from '$lib/changes';
	import { m } from '$lib/paraglide/messages.js';

	let { label }: { label: string } = $props();

	let who = $derived(actor(label));
</script>

{#if who === null}
	<span class="opacity-60">{m.change_actor_anonymous()}</span>
{:else if who.agent}
	<span class="tooltip inline-flex items-center gap-1" data-tip={m.change_actor_agent()}>
		<Icon icon="tabler:robot" class="h-3.5 w-3.5" aria-label={m.change_actor_agent()} />
		{who.name}
	</span>
{:else}
	{who.name}
{/if}
