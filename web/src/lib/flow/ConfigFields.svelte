<script lang="ts">
	import { load } from 'js-yaml';
	import YamlFields, { isFields, type Fields } from '$lib/YamlFields.svelte';
	import { connectorFor } from '$lib/connectors';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		yaml: string;
		onResourceClick?: (key: string) => void;
	}

	let { yaml, onResourceClick }: Props = $props();

	interface Task {
		type: string;
		name: string;
		fields: Fields;
	}

	interface Parsed {
		description: string | null;
		tags: unknown[];
		settings: Fields;
		tasks: Task[];
	}

	function parse(source: string): Parsed | null {
		let doc: unknown;
		try {
			doc = load(source);
		} catch {
			return null;
		}
		if (!isFields(doc) || !isFields(doc.flow)) return null;
		const { labels, tasks, ...settings } = doc.flow;
		const { description, tags } = isFields(labels) ? labels : {};
		const items = Array.isArray(tasks) ? tasks : [];
		return {
			description: typeof description === 'string' ? description : null,
			tags: Array.isArray(tags) ? tags : [],
			settings,
			tasks: items.flatMap((item) => {
				if (!isFields(item)) return [];
				return Object.entries(item).map(([type, config]) => {
					const { name, ...fields } = isFields(config) ? config : {};
					return { type, name: String(name ?? type), fields };
				});
			})
		};
	}

	let parsed = $derived(parse(yaml));
</script>

{#if parsed === null}
	<p class="px-6 py-4 text-sm text-base-content/60">{m.config_fields_unreadable()}</p>
{:else}
	<div class="space-y-8 px-6 py-5 text-sm">
		{#if parsed.description || parsed.tags.length > 0 || Object.keys(parsed.settings).length > 0}
			<section class="space-y-3">
				<h3 class="text-xs font-medium text-base-content/50">{m.flow_config_flow_section()}</h3>
				{#if parsed.description}
					<p class="text-base-content/80">{parsed.description}</p>
				{/if}
				{#if parsed.tags.length > 0}
					<span class="flex flex-wrap gap-1">
						{#each parsed.tags as tag, i (i)}
							<span class="chip-sm chip-neutral">{String(tag)}</span>
						{/each}
					</span>
				{/if}
				{#if Object.keys(parsed.settings).length > 0}
					<YamlFields entries={parsed.settings} {onResourceClick} />
				{/if}
			</section>
		{/if}
		{#each parsed.tasks as task, i (i)}
			{@const connector = connectorFor(task.type)}
			<section class="space-y-3">
				<h3 id="task-{task.name}" class="flex items-center gap-2 rounded">
					<img src={connector.iconUrl} alt="" class="h-4 w-4 shrink-0" />
					<span class="font-mono font-semibold">{task.name}</span>
					<span class="text-xs text-base-content/50">{task.type}</span>
				</h3>
				{#if Object.keys(task.fields).length > 0}
					<YamlFields entries={task.fields} {onResourceClick} />
				{/if}
			</section>
		{/each}
	</div>
{/if}
