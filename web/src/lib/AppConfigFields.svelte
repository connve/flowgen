<script lang="ts">
	import { load } from 'js-yaml';
	import YamlFields, { fieldLabel, isFields, type Fields } from '$lib/YamlFields.svelte';
	import { m } from '$lib/paraglide/messages.js';

	let { yaml }: { yaml: string } = $props();

	interface Section {
		title: string;
		entries: Fields;
	}

	/** One section per top-level object; top-level values gather under "General". */
	function sections(source: string): Section[] | null {
		let doc: unknown;
		try {
			doc = load(source);
		} catch {
			return null;
		}
		if (!isFields(doc)) return null;
		const general: Fields = {};
		const groups: Section[] = [];
		for (const [key, value] of Object.entries(doc)) {
			if (isFields(value)) groups.push({ title: fieldLabel(key), entries: value });
			else general[key] = value;
		}
		return Object.keys(general).length > 0
			? [{ title: m.config_fields_general(), entries: general }, ...groups]
			: groups;
	}

	let parsed = $derived(sections(yaml));
</script>

{#if parsed === null}
	<p class="px-6 py-4 text-sm text-base-content/60">{m.config_fields_unreadable()}</p>
{:else}
	<div class="space-y-8 px-6 py-5 text-sm">
		{#each parsed as section (section.title)}
			<section class="space-y-3">
				<h3 class="font-semibold">{section.title}</h3>
				{#if Object.keys(section.entries).length > 0}
					<YamlFields entries={section.entries} />
				{/if}
			</section>
		{/each}
	</div>
{/if}
