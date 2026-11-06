<script lang="ts">
	import Icon from '@iconify/svelte';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		text: string | null | undefined;
		label?: string;
		size?: 'xs' | 'sm';
		tooltip?: 'left' | 'top' | 'bottom';
	}

	let { text, label = m.common_copy(), size = 'sm', tooltip = 'left' }: Props = $props();

	const TOOLTIP = { left: 'tooltip-left', top: 'tooltip-top', bottom: 'tooltip-bottom' };

	let copied = $state(false);
	let timeout: ReturnType<typeof setTimeout> | null = null;

	async function onclick() {
		if (!text) return;
		try {
			await navigator.clipboard.writeText(text);
			copied = true;
			if (timeout) clearTimeout(timeout);
			timeout = setTimeout(() => (copied = false), 1500);
		} catch {
			// Clipboard refused — silent no-op.
		}
	}

	let btnSize = $derived(size === 'xs' ? 'h-6 w-6' : 'h-8 w-8');
	let iconSize = $derived(size === 'xs' ? 'h-4 w-4' : 'h-5 w-5');
</script>

<div
	class="tooltip {TOOLTIP[tooltip]}"
	data-tip={copied ? m.common_copied() : label}
>
	<button
		type="button"
		class="flex items-center justify-center rounded-md text-base-content/50 transition-colors hover:bg-base-200 hover:text-base-content disabled:opacity-40 {btnSize}"
		aria-label={copied ? m.common_copied() : label}
		{onclick}
		disabled={!text}
	>
		{#if copied}
			<Icon icon="tabler:check" class="{iconSize} text-primary" />
		{:else}
			<Icon icon="tabler:copy" class={iconSize} />
		{/if}
	</button>
</div>
