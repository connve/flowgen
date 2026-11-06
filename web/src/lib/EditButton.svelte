<script lang="ts">
	import Icon from '@iconify/svelte';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		// Editor link; without one the button is disabled.
		href: string | null;
		tooltip?: 'left' | 'bottom';
	}

	let { href, tooltip = 'bottom' }: Props = $props();
</script>

{#if href}
	<a {href} class="btn btn-ghost btn-sm gap-1.5">
		<Icon icon="tabler:pencil" class="h-4 w-4" />
		{m.common_edit()}
	</a>
{:else}
	<div
		class="tooltip {tooltip === 'left' ? 'tooltip-left' : 'tooltip-bottom'}"
		data-tip={m.edit_button_read_only()}
	>
		<button type="button" class="btn btn-ghost btn-sm gap-1.5" disabled>
			<Icon icon="tabler:pencil" class="h-4 w-4" />
			{m.common_edit()}
		</button>
	</div>
{/if}
