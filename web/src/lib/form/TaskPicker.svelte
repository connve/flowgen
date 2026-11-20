<script lang="ts">
	import { connectorTaskTypes, loadConnectors } from '$lib/connectors';
	import { m } from '$lib/paraglide/messages.js';
	import { onMount } from 'svelte';

	interface Props {
		/** The task types the schema knows; others are left out. */
		known: Set<string>;
		onpick: (taskType: string) => void;
		onclose: () => void;
	}

	let { known, onpick, onclose }: Props = $props();

	let query = $state('');
	let loaded = $state(false);
	let search = $state<HTMLInputElement | null>(null);

	onMount(async () => {
		await loadConnectors(fetch);
		loaded = true;
		search?.focus();
	});

	let groups = $derived.by(() => {
		if (!loaded) return [];
		const needle = query.trim().toLowerCase();
		return connectorTaskTypes()
			.map(({ connector, taskTypes }) => ({
				connector,
				taskTypes: taskTypes.filter(
					(taskType) =>
						known.has(taskType) &&
						(!needle || taskType.includes(needle) || connector.label.toLowerCase().includes(needle))
				)
			}))
			.filter((group) => group.taskTypes.length > 0);
	});
</script>

<div
	class="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-4"
	role="dialog"
	aria-modal="true"
	aria-label={m.form_add_task()}
	tabindex="-1"
	onclick={(e) => {
		if (e.target === e.currentTarget) onclose();
	}}
	onkeydown={(e) => {
		if (e.key !== 'Escape') return;
		e.stopPropagation();
		onclose();
	}}
>
	<div class="flex max-h-[80vh] w-full max-w-md flex-col gap-3 rounded-xl bg-base-100 p-5 shadow-2xl">
		<h2 class="text-[0.9375rem] font-semibold">{m.form_add_task()}</h2>
		<label class="input input-sm flex w-full items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary">
			<input
				bind:this={search}
				type="search"
				class="grow"
				placeholder={m.form_task_type_search()}
				aria-label={m.form_task_type_search()}
				bind:value={query}
			/>
		</label>
		<div class="min-h-0 flex-1 space-y-3 overflow-y-auto">
			{#each groups as group (group.connector.name)}
				<div>
					<div class="mb-1 flex items-center gap-2 text-xs font-medium text-base-content/50">
						<img src={group.connector.iconUrl} alt="" class="h-4 w-4" />
						{group.connector.label}
					</div>
					{#each group.taskTypes as taskType (taskType)}
						<button
							type="button"
							class="flex h-9 w-full items-center gap-1.5 rounded-md px-2 font-mono text-sm hover:bg-base-200"
							onclick={() => onpick(taskType)}
						>
							{taskType}
						</button>
					{/each}
				</div>
			{/each}
		</div>
	</div>
</div>
