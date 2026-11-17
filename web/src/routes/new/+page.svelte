<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import { Badge, Inspector, InspectorSection, Loading, StateMessage } from '@connve/ui';
	import { encodePath, proposeChange, type ChangeProposal, type HubTemplate } from '$lib/api';
	import { baseContent, pendingChanges } from '$lib/changes';
	import { connectorFor, type Connector } from '$lib/connectors';
	import { load } from 'js-yaml';
	import { SvelteMap } from 'svelte/reactivity';
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
	let uninstalling = $state(false);
	let cancelButton = $state<HTMLButtonElement | null>(null);

	$effect(() => {
		if (uninstalling) cancelButton?.focus();
	});

	let editorHref = $derived(
		`${base}/edit?new=${data.kind}${data.folder ? `&folder=${encodePath(data.folder)}` : ''}`
	);

	const Installed = {
		Pending: 'pending',
		Added: 'added',
		Update: 'update',
		Incomplete: 'incomplete'
	} as const;
	type Installed = (typeof Installed)[keyof typeof Installed];
	type Status = { state: Installed; missing: HubTemplate['files'] };
	let installed = new SvelteMap<string, Status>();
	let status = $derived(previewed && source ? installed.get(keyOf(source, previewed)) : undefined);

	function keyOf(hub: string, template: HubTemplate): string {
		return `${hub}/${template.slug}`;
	}

	async function statusOf(template: HubTemplate, proposed: Set<string>): Promise<Status | null> {
		const paths = [...template.files.map((file) => file.path), ...(template.installedFiles ?? [])];
		if (paths.some((path) => proposed.has(path))) return { state: Installed.Pending, missing: [] };
		if (!template.installedDigest) return null;
		if (template.installedDigest !== template.digest) return { state: Installed.Update, missing: [] };
		const contents = await Promise.all(template.files.map((file) => baseContent(file.path)));
		const missing = template.files.filter((_, i) => contents[i] === null);
		return { state: missing.length > 0 ? Installed.Incomplete : Installed.Added, missing };
	}

	onMount(async () => {
		const [loaded, pending] = await Promise.all([catalogsOf(data.hubs), pendingChanges()]);
		const proposed = new Set(pending.flatMap((change) => change.paths));
		const statuses = await Promise.all(
			loaded.flatMap(({ hub, templates }) =>
				templates.map(async (template) => ({
					key: keyOf(hub.id, template),
					status: await statusOf(template, proposed).catch(() => null)
				}))
			)
		);
		for (const { key, status } of statuses) {
			if (status) installed.set(key, status);
		}
		catalogs = loaded;
		source = loaded[0]?.hub.id ?? null;
		loading = false;
	});

	// The connectors of the tasks in a template's flows, as the flows list shows them.
	function connectorsOf(template: HubTemplate): Connector[] {
		const found = new Map<string, Connector>();
		for (const file of template.files) {
			if (!file.path.startsWith('flows/') || !file.content) continue;
			let tasks: unknown;
			try {
				tasks = (load(file.content) as { flow?: { tasks?: unknown } })?.flow?.tasks;
			} catch {
				continue;
			}
			if (!Array.isArray(tasks)) continue;
			for (const task of tasks) {
				const taskType = task && typeof task === 'object' ? Object.keys(task)[0] : undefined;
				if (!taskType) continue;
				const connector = connectorFor(taskType);
				if (connector.name !== 'core') found.set(connector.name, connector);
			}
		}
		return [...found.values()];
	}

	function select(hub: string) {
		source = hub;
		preview(null);
	}

	function preview(slug: string | null) {
		previewedSlug = slug;
		useError = null;
	}

	async function propose(body: ChangeProposal) {
		using = true;
		useError = null;
		try {
			const { data: change } = await proposeChange({ body });
			await goto(`${base}/changes/${encodeURIComponent(change.id)}`);
		} catch (error) {
			useError = error instanceof Error ? error.message : String(error);
		} finally {
			using = false;
		}
	}

	// A template becomes its own change, which records it in flowgen.lock once
	// committed; an update also deletes the files this version no longer has.
	function useTemplate(template: HubTemplate, hub: string) {
		const title =
			status?.state === Installed.Update
				? m.hub_template_change_title_update({ name: template.name })
				: m.hub_template_change_title({ name: template.name });
		const paths = new Set(template.files.map((file) => file.path));
		const gone = (template.installedFiles ?? []).filter((path) => !paths.has(path));
		return propose({
			title,
			files: [...template.files, ...gone.map((path) => ({ path, content: null }))],
			template: { hub, slug: template.slug, digest: template.digest }
		});
	}

	function restore(template: HubTemplate, missing: HubTemplate['files']) {
		return propose({ title: m.hub_template_change_title_restore({ name: template.name }), files: missing });
	}

	// Deletes the files flowgen.lock holds for the template and drops it from flowgen.lock.
	function uninstall(template: HubTemplate, hub: string) {
		return propose({
			title: m.hub_template_change_title_uninstall({ name: template.name }),
			files: (template.installedFiles ?? []).map((path) => ({ path, content: null })),
			template: { hub, slug: template.slug, digest: template.digest, removed: true }
		});
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
				<StateMessage tone="notice" title={m.hub_none_title()}>
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
								onclick={() => preview(template.slug)}
							>
								<span class="flex items-center gap-2">
									{#each connectorsOf(template) as connector (connector.name)}
										<img src={connector.iconUrl} alt="" class="h-4 w-4 shrink-0" />
									{/each}
									<span class="truncate text-sm font-medium">{template.name}</span>
									{#if template.category}
										<Badge>{template.category}</Badge>
									{/if}
									{#if source}
										{@const state = installed.get(keyOf(source, template))?.state}
										{#if state === Installed.Added}
											<Badge variant="success">{m.hub_template_added()}</Badge>
										{:else if state === Installed.Pending}
											<Badge>{m.hub_template_pending()}</Badge>
										{:else if state === Installed.Update}
											<Badge variant="warning">{m.hub_template_update_available()}</Badge>
										{:else if state === Installed.Incomplete}
											<Badge variant="warning">{m.hub_template_incomplete()}</Badge>
										{/if}
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

{#if previewed && source}
	{@const template = previewed}
	{@const hub = source}
	<Inspector title={template.name} label={m.hub_template_details()} onclose={() => preview(null)}>
		{#snippet actions()}
			{#if status?.state === Installed.Pending}
				<button type="button" class="btn btn-sm" disabled>{m.hub_template_pending()}</button>
			{:else if status?.state === Installed.Added}
				<button type="button" class="btn btn-sm" disabled>{m.hub_template_added()}</button>
			{:else}
				<button
					type="button"
					class="btn btn-primary btn-sm"
					disabled={using || template.files.length === 0}
					onclick={() =>
						status?.state === Installed.Incomplete
							? restore(template, status.missing)
							: useTemplate(template, hub)}
				>
					{#if using}
						<span class="loading loading-spinner loading-xs"></span>
					{/if}
					{#if status?.state === Installed.Update}
						{m.hub_template_update()}
					{:else if status?.state === Installed.Incomplete}
						{m.hub_template_restore()}
					{:else}
						{m.hub_template_use()}
					{/if}
				</button>
			{/if}
			{#if status && status.state !== Installed.Pending && (template.installedFiles ?? []).length > 0}
				<div class="dropdown dropdown-end">
					<button
						type="button"
						tabindex="0"
						class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content"
						aria-label={m.editor_more_actions()}
					>
						<Icon icon="tabler:dots" class="h-5 w-5" />
					</button>
					<ul
						tabindex="-1"
						class="menu dropdown-content z-10 mt-1 w-56 rounded-md border border-base-200 bg-base-100 p-1 text-sm shadow-lg"
					>
						<li>
							<button type="button" class="text-error" disabled={using} onclick={() => {
									useError = null;
									uninstalling = true;
								}}>
								<Icon icon="tabler:trash" class="h-4 w-4" />
								{m.hub_template_uninstall()}
							</button>
						</li>
					</ul>
				</div>
			{/if}
		{/snippet}
		{#if useError && !uninstalling}
			<div class="rounded-lg border border-error/40 bg-error/5 p-3 text-sm">{useError}</div>
		{/if}
		{#if template.description}
			<p class="text-sm">{template.description}</p>
		{/if}
		{#if template.tags.length > 0}
			<div class="flex flex-wrap gap-1">
				{#each template.tags as tag (tag)}
					<Badge>{tag}</Badge>
				{/each}
			</div>
		{/if}
		<InspectorSection title={m.hub_template_files()}>
			<ul class="space-y-1 font-mono text-xs">
				{#each template.files as file (file.path)}
					{@const missing = status?.missing.includes(file)}
					<li class="flex items-center gap-2">
						<span class="[overflow-wrap:anywhere] {missing ? 'text-base-content/50' : ''}">{file.path}</span>
						{#if missing}
							<Badge variant="warning">{m.hub_template_file_missing()}</Badge>
						{/if}
					</li>
				{/each}
			</ul>
		</InspectorSection>
		{#if template.readme}
			<InspectorSection title={m.hub_template_readme()}>
				<div class="prose prose-sm max-w-none text-sm">
					<!-- eslint-disable-next-line svelte/no-at-html-tags -->
					{@html renderMarkdown(template.readme)}
				</div>
			</InspectorSection>
		{/if}
	</Inspector>

	{#if uninstalling}
		<div
			class="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-4"
			role="dialog"
			aria-modal="true"
			aria-label={m.hub_template_uninstall()}
			tabindex="-1"
			onclick={(e) => {
				if (e.target === e.currentTarget && !using) uninstalling = false;
			}}
			onkeydown={(e) => {
				if (e.key !== 'Escape' || using) return;
				e.stopPropagation();
				uninstalling = false;
			}}
		>
			<div class="w-full max-w-sm space-y-4 rounded-xl bg-base-100 p-5 shadow-2xl">
				<h2 class="text-[0.9375rem] font-semibold">
					{m.hub_template_uninstall_title({ name: template.name })}
				</h2>
				<ul class="space-y-1 font-mono text-xs text-base-content/70">
					{#each template.installedFiles ?? [] as path (path)}
						<li class="[overflow-wrap:anywhere]">{path}</li>
					{/each}
				</ul>
				{#if useError}
					<p class="text-sm text-error">{useError}</p>
				{/if}
				<div class="flex justify-end gap-2">
					<button
						type="button"
						class="btn btn-ghost btn-sm"
						bind:this={cancelButton}
						disabled={using}
						onclick={() => (uninstalling = false)}
					>
						{m.common_cancel()}
					</button>
					<button
						type="button"
						class="btn btn-error btn-sm"
						disabled={using}
						onclick={() => uninstall(template, hub)}
					>
						{m.hub_template_uninstall()}
					</button>
				</div>
			</div>
		</div>
	{/if}
{/if}
</div>
