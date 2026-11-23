<script lang="ts">
	import { SearchPalette } from '@connve/ui';
	import { connectorTaskTypes, loadConnectors, type Connector } from '$lib/connectors';
	import { fieldLabel } from '$lib/YamlFields.svelte';
	import { m } from '$lib/paraglide/messages.js';
	import { onMount } from 'svelte';

	interface Props {
		/** The task types the schema knows; others are left out. */
		known: Set<string>;
		onpick: (taskType: string) => void;
		onclose: () => void;
	}

	let { known, onpick, onclose }: Props = $props();

	type Choice = { taskType: string; label: string; connector: Connector };

	let query = $state('');
	let loaded = $state(false);

	onMount(async () => {
		await loadConnectors(fetch);
		loaded = true;
	});

	function labelOf(taskType: string, connector: Connector): string {
		const prefix = `${connector.name}_`;
		return fieldLabel(taskType.startsWith(prefix) ? taskType.slice(prefix.length) : taskType);
	}

	let groups = $derived.by((): [string, Choice[]][] => {
		if (!loaded) return [];
		const needle = query.trim().toLowerCase();
		return connectorTaskTypes()
			.map(({ connector, taskTypes }): [string, Choice[]] => [
				connector.label,
				taskTypes
					.filter((taskType) => known.has(taskType))
					.map((taskType) => ({ taskType, label: labelOf(taskType, connector), connector }))
					.filter(
						(choice) =>
							!needle ||
							choice.taskType.includes(needle) ||
							choice.label.toLowerCase().includes(needle) ||
							connector.label.toLowerCase().includes(needle)
					)
			])
			.filter(([, choices]) => choices.length > 0);
	});
</script>

{#snippet row(choice: Choice)}
	<img src={choice.connector.iconUrl} alt="" class="h-4 w-4 shrink-0" />
	<span class="truncate">{choice.label}</span>
	<span class="ml-auto truncate pl-2 font-mono text-xs text-base-content/50">{choice.taskType}</span>
{/snippet}

<SearchPalette
	bind:open={() => true, (next) => !next && onclose()}
	bind:query
	{groups}
	{row}
	loading={!loaded}
	label={m.form_add_task()}
	placeholder={m.form_task_type_search()}
	key={(choice) => choice.taskType}
	onchoose={(choice) => onpick(choice.taskType)}
/>
