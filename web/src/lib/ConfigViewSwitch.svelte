<script lang="ts">
	import Icon from '@iconify/svelte';
	import { configView } from '$lib/configView.svelte';
	import { m } from '$lib/paraglide/messages.js';

	let source = $derived(configView.value === 'yaml');
	let label = $derived(source ? m.config_hide_source() : m.config_show_source());
</script>

<div class="tooltip tooltip-bottom" data-tip={label}>
	<button
		type="button"
		class="flex h-8 w-8 items-center justify-center rounded-md transition-colors hover:bg-base-200 hover:text-base-content {source
			? 'bg-base-200 text-base-content'
			: 'text-base-content/50'}"
		aria-label={label}
		aria-pressed={source}
		onclick={() => (configView.value = source ? 'fields' : 'yaml')}
	>
		<Icon icon="tabler:code" class="h-5 w-5" />
	</button>
</div>
