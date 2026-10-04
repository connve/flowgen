<script lang="ts">
	import Icon from '@iconify/svelte';

	interface Props {
		// Editor link; without one the button is disabled.
		href: string | null;
		tooltip?: 'left' | 'bottom';
	}

	let { href, tooltip = 'bottom' }: Props = $props();
</script>

<div
	class="tooltip {tooltip === 'left' ? 'tooltip-left' : 'tooltip-bottom'}"
	data-tip={href ? 'Edit' : 'Managed in the repository'}
>
	{#if href}
		<a {href} class="btn btn-ghost btn-sm btn-circle" aria-label="Edit">
			<Icon icon="tabler:pencil" class="h-5 w-5" />
		</a>
	{:else}
		<button type="button" class="btn btn-ghost btn-sm btn-circle" aria-label="Edit" disabled>
			<Icon icon="tabler:pencil" class="h-5 w-5" />
		</button>
	{/if}
</div>
