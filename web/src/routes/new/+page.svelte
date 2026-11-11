<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import { Badge, Inspector, InspectorSection, Loading, StateMessage } from '@connve/ui';
	import { encodePath, proposeChange, type HubTemplate } from '$lib/api';
	import { addToDraft, authoringTargets, draftChange, pendingChanges, targetOf } from '$lib/changes';
	import { catalogsOf, type Catalog } from '$lib/hubs';
	import { renderMarkdown } from '$lib/markdown';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';
	import { onMount } from 'svelte';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let catalogs = $state<Catalog[]>([]);
	let loading = $state(true);
	// The selected hub's id.
	let source = $state<string | null>(null);
	let templates = $derived(catalogs.find((catalog) => catalog.hub.id === source)?.templates ?? []);
	let previewedSlug = $state<string | null>(null);
	let previewed = $derived(templates.find((t) => t.slug === previewedSlug) ?? null);
	let using = $state(false);
	let useError = $state<string | null>(null);

	let editorHref = $derived(
		`${base}/edit?new=${data.kind}${data.folder ? `&folder=${encodePath(data.folder)}` : ''}`
	);

	onMount(async () => {
		catalogs = await catalogsOf(data.hubs);
		source = catalogs[0]?.hub.id ?? null;
		loading = false;
	});

	function select(hub: string) {
		source = hub;
		previewedSlug = null;
	}

	// A template's files join the user's pending change, or become a new one,
	// validated like any other, and the change opens for review.
	async function useTemplate(template: HubTemplate) {
		using = true;
		useError = null;
		try {
			const [pending, targets] = await Promise.all([pendingChanges(), authoringTargets()]);
			const first = template.files[0];
			const draft = first ? draftChange(pending, targetOf(targets, first.path)?.name) : undefined;
			let change = draft ? await addToDraft(draft.id, template.files) : null;
			if (change === null) {
				const { data } = await proposeChange({
					body: { title: m.hub_template_change_title({ name: template.name }), files: template.files }
				});
				change = data;
			}
			await goto(`${base}/changes/${encodeURIComponent(change.id)}`);
		} catch (error) {
			useError = error instanceof Error ? error.message : String(error);
		} finally {
			using = false;
		}
	}
</script>

<svelte:head><title>{pageTitle(m.hub_heading())}</title></svelte:head>

<div class="flex h-[calc(100vh-4rem)] min-w-0">
<div class="flex min-h-0 min-w-0 flex-1 flex-col">
	<div class="shrink-0 bg-base-100 px-6 pb-4 pt-6">
		<div class="flex items-center gap-1 text-xs text-base-content/50">
			<a href={editorHref} class="hover:text-base-content"
				>{data.kind === 'resource' ? m.editor_new_resource() : m.editor_new_flow()}</a
			>
		</div>
		<h1 class="text-[0.9375rem] font-semibold">{m.hub_heading()}</h1>
	</div>

	<div class="@container flex min-h-0 flex-1 flex-col">
	<div class="flex min-h-0 flex-1 gap-6 px-6 pb-6 @max-md:flex-col">
		{#if catalogs.length > 1}
			<nav class="w-48 shrink-0 @max-md:w-full" aria-label={m.hub_sources()}>
				<ul class="space-y-0.5">
					{#each catalogs as { hub } (hub.id)}
						<li>
							<button
								type="button"
								class="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-sm transition-colors hover:bg-base-200 {source ===
								hub.id
									? 'bg-base-200 font-medium'
									: ''}"
								onclick={() => select(hub.id)}
							>
								<Icon icon="tabler:building-store" class="h-4 w-4 opacity-60" />
								<span class="truncate">{hub.name}</span>
							</button>
						</li>
					{/each}
				</ul>
			</nav>
		{/if}

		<div class="min-w-0 flex-1 overflow-y-auto">
			{#if loading}
				<Loading />
			{:else if catalogs.length === 0}
				<StateMessage tone="notice" title={m.hub_none_title()} message={m.hub_none_message()}>
					<a href={editorHref} class="btn btn-primary btn-sm">{m.hub_start_blank()}</a>
				</StateMessage>
			{:else}
				<ul class="space-y-0.5">
					{#each templates as template (template.slug)}
						<li>
							<button
								type="button"
								class="flex w-full flex-col gap-1 rounded-md px-3 py-2.5 text-left transition-colors hover:bg-base-200 {previewedSlug ===
								template.slug
									? 'bg-base-200'
									: ''}"
								onclick={() => (previewedSlug = template.slug)}
							>
								<span class="flex items-center gap-2">
									<span class="truncate text-sm font-medium">{template.name}</span>
									{#if template.category}
										<Badge>{template.category}</Badge>
									{/if}
								</span>
								{#if template.description}
									<span class="line-clamp-2 text-xs text-base-content/60">{template.description}</span>
								{/if}
							</button>
						</li>
					{/each}
				</ul>
			{/if}
		</div>
	</div>
	</div>
</div>

{#if previewed}
	<Inspector title={previewed.name} label={m.hub_template_details()} onclose={() => (previewedSlug = null)}>
		{#snippet actions()}
			<button
				type="button"
				class="btn btn-primary btn-sm"
				disabled={using || previewed.files.length === 0}
				onclick={() => previewed && useTemplate(previewed)}
			>
				{#if using}
					<span class="loading loading-spinner loading-xs"></span>
				{/if}
				{m.hub_template_use()}
			</button>
		{/snippet}
		{#if useError}
			<div class="rounded-lg border border-error/40 bg-error/5 p-3 text-sm">{useError}</div>
		{/if}
		{#if previewed.description}
			<p class="text-sm">{previewed.description}</p>
		{/if}
		{#if previewed.tags.length > 0}
			<div class="flex flex-wrap gap-1">
				{#each previewed.tags as tag (tag)}
					<Badge>{tag}</Badge>
				{/each}
			</div>
		{/if}
		<InspectorSection title={m.hub_template_files()}>
			<ul class="space-y-1 font-mono text-xs">
				{#each previewed.files as file (file.path)}
					<li class="[overflow-wrap:anywhere]">{file.path}</li>
				{/each}
			</ul>
		</InspectorSection>
		{#if previewed.readme}
			<InspectorSection title={m.hub_template_readme()}>
				<div class="prose prose-sm max-w-none text-sm">
					<!-- eslint-disable-next-line svelte/no-at-html-tags -->
					{@html renderMarkdown(previewed.readme)}
				</div>
			</InspectorSection>
		{/if}
	</Inspector>
{/if}
</div>
