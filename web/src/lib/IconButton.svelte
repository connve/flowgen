<script lang="ts">
	import Icon from '@iconify/svelte';

	interface Props {
		icon: string;
		label: string;
		/** A link when set, otherwise a button. */
		href?: string;
		onclick?: () => void;
		tooltip?: 'left' | 'right' | 'bottom';
		ref?: HTMLButtonElement | null;
	}

	let { icon, label, href, onclick, tooltip = 'bottom', ref = $bindable(null) }: Props = $props();

	const CLASS =
		'flex h-8 w-8 items-center justify-center rounded-md text-base-content/50 transition-colors hover:bg-base-200 hover:text-base-content';
	const TOOLTIP = { left: 'tooltip-left', right: 'tooltip-right', bottom: 'tooltip-bottom' };
</script>

<div class="tooltip {TOOLTIP[tooltip]}" data-tip={label}>
	{#if href}
		<a {href} class={CLASS} aria-label={label}>
			<Icon {icon} class="h-5 w-5" />
		</a>
	{:else}
		<button bind:this={ref} type="button" class={CLASS} aria-label={label} {onclick}>
			<Icon {icon} class="h-5 w-5" />
		</button>
	{/if}
</div>
