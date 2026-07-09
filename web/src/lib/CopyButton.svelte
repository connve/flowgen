<script lang="ts">
	import Icon from '@iconify/svelte';

	interface Props {
		text: string | null | undefined;
		label?: string;
		size?: 'xs' | 'sm';
	}

	let { text, label = 'Copy', size = 'sm' }: Props = $props();

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

	let btnSize = $derived(size === 'xs' ? 'btn-xs' : 'btn-sm');
	let iconSize = $derived(size === 'xs' ? 'h-4 w-4' : 'h-6 w-6');
</script>

<div class="tooltip tooltip-left" data-tip={copied ? 'Copied' : label}>
	<button
		type="button"
		class="btn btn-ghost btn-circle {btnSize}"
		aria-label={copied ? 'Copied' : label}
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
