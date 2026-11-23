<script lang="ts">
	import YamlFields from '$lib/YamlFields.svelte';
	import type { FieldChange } from '$lib/changePreview';
	import type { Mark } from '$lib/dag/parseFlow';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		mark?: Mark;
		fields: FieldChange[];
	}

	let { mark, fields }: Props = $props();

	let whole = $derived(
		Object.fromEntries(
			fields.flatMap((field) => {
				const shown = mark === 'removed' ? field.before : field.after;
				return shown === undefined ? [] : [[field.key, shown]];
			})
		)
	);
</script>

{#if !mark}
	<p class="text-base-content/60">{m.change_task_unchanged()}</p>
{:else if mark === 'added'}
	<YamlFields entries={whole} />
{:else if mark === 'removed'}
	<div class="line-through decoration-base-content/30 opacity-60">
		<YamlFields entries={whole} />
	</div>
{:else}
	<div class="space-y-3">
		{#each fields as field (field.key)}
			<div class="space-y-0.5">
				{#if field.before !== undefined}
					<div class="rounded-md bg-error/10 px-2 py-1">
						<YamlFields entries={{ [field.key]: field.before }} />
					</div>
				{/if}
				{#if field.after !== undefined}
					<div class="rounded-md bg-primary/10 px-2 py-1">
						<YamlFields entries={{ [field.key]: field.after }} />
					</div>
				{/if}
			</div>
		{/each}
	</div>
{/if}
