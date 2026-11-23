<script lang="ts">
	import { onMount } from 'svelte';
	import { SvelteSet } from 'svelte/reactivity';
	import { base } from '$app/paths';
	import { goto, pushState, replaceState } from '$app/navigation';
	import { page } from '$app/state';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import {
		CopyButton,
		FolderTree,
		IconButton,
		Inspector,
		ListHeader,
		Loading,
		SidePane,
		StateMessage,
		buildTree,
		isMobile
	} from '@connve/ui';
	import Icon from '@iconify/svelte';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';
	import { deploymentsStream } from '$lib/urls';
	import { visibleEventSource } from '$lib/visibleEventSource';
	import {
		encodePath,
		errorMessage,
		getResource,
		isStatus,
		listResources,
		type AuthoringTarget,
		type ResourceSummary as Resource,
		type ResourceContent
	} from '$lib/api';
	import {
		authoringEnabled,
		authoringTargets,
		editUrl,
		targetOf
	} from '$lib/changes';

	let resources = $state<Resource[]>([]);
	let authoring = $state(false);
	let targets = $state<AuthoringTarget[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);

	// Folder pane state — mirrors the Flows page: collapsible sidebar with
	// per-folder expand/collapse, all persisted in localStorage.
	let expandedFolders = $state(new SvelteSet<string>());
	let selectedFolder = $state<string | null>(null);
	let foldersPaneOpen = $state(true);

	let selected = $state<string | null>(null);
	let selectedContent = $state<ResourceContent | null>(null);
	let selectedLoading = $state(false);
	let selectedError = $state<string | null>(null);

	let selectedSegments = $derived(selected ? selected.split('/') : []);
	let selectedLeaf = $derived(selectedSegments[selectedSegments.length - 1] ?? '');

	onMount(async () => {
		const pane = localStorage.getItem('flowgen-resources-pane');
		if (pane !== null) foldersPaneOpen = pane === '1';
		if (isMobile()) foldersPaneOpen = false;
		const exp = localStorage.getItem('flowgen-resources-expanded');
		if (exp) {
			try {
				for (const p of JSON.parse(exp) as string[]) expandedFolders.add(p);
			} catch {
				// ignore corrupt state
			}
		}

		authoringEnabled().then((enabled) => (authoring = enabled));
		authoringTargets().then((found) => (targets = found));

		try {
			resources = (await listResources()).data;
		} catch (err) {
			error = errorMessage(err, m.resources_load_failed_reason());
		} finally {
			loading = false;
		}
	});

	// Loads the list again each time a resource changes, such as a deployed change.
	onMount(() =>
		visibleEventSource(deploymentsStream(), (source) =>
			source.addEventListener('deployment', () => {
				listResources().then(({ data }) => (resources = data), () => undefined);
			})
		)
	);

	function toggleFolder(path: string) {
		if (expandedFolders.has(path)) expandedFolders.delete(path);
		else expandedFolders.add(path);
		localStorage.setItem(
			'flowgen-resources-expanded',
			JSON.stringify(Array.from(expandedFolders)),
		);
	}

	function selectFolder(path: string | null) {
		selectedFolder = path;
		closePaneOnMobile();
	}

	// On mobile the pane covers the list, so it closes once a folder is picked.
	function closePaneOnMobile() {
		if (isMobile()) foldersPaneOpen = false;
	}

	function toggleFoldersPane() {
		foldersPaneOpen = !foldersPaneOpen;
		localStorage.setItem('flowgen-resources-pane', foldersPaneOpen ? '1' : '0');
	}


	let sidebarTree = $derived(buildTree<Resource>(resources, (r) => r.key));

	let visibleResources = $derived.by(() => {
		if (selectedFolder === null) return resources;
		const prefix = selectedFolder + '/';
		return resources.filter((r) => r.key.startsWith(prefix));
	});

	// A cache paints repeats instantly; `pending` keeps stale responses from overwriting the selection.
	const contentCache = new Map<string, ResourceContent>();
	let pendingResource: string | null = null;

	async function openResource(key: string) {
		pendingResource = key;
		const show = page.state.resource ? replaceState : pushState;
		show(`${base}/resources/${encodePath(key)}`, { resource: key });
		const cached = contentCache.get(key);
		if (cached) {
			selected = key;
			selectedContent = cached;
			selectedError = null;
			selectedLoading = false;
		} else {
			selectedLoading = true;
		}
		try {
			const body = await getResource({ path: { key } }).then(
				({ data }) => data,
				(err) => {
					throw isStatus(err, 404) ? new Error(m.resource_not_found({ key })) : err;
				}
			);
			contentCache.set(key, body);
			if (pendingResource !== key) return;
			selected = key;
			selectedContent = body;
			selectedError = null;
		} catch (err) {
			if (pendingResource !== key) return;
			selected = key;
			selectedError = errorMessage(err, m.resource_load_failed_named({ key }));
		} finally {
			if (pendingResource === key) selectedLoading = false;
		}
	}

	function closeResource() {
		if (page.state.resource) history.back();
		else clearResource();
	}

	// Back closes the preview: the shallow history entry it pushed is gone.
	$effect(() => {
		if (!page.state.resource && (selected || selectedLoading)) clearResource();
	});

	function clearResource() {
		pendingResource = null;
		selectedLoading = false;
		selected = null;
		selectedContent = null;
		selectedError = null;
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape' && selected) closeResource();
	}

	function formatSize(bytes: number | null): string {
		if (bytes === null) return '—';
		if (bytes < 1024) return `${bytes} B`;
		if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} kB`;
		return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
	}
</script>

<svelte:head>
	<title>{pageTitle(m.nav_resources())}</title>
</svelte:head>

<svelte:window on:keydown={onKeydown} />

<section class="relative flex h-[calc(100vh-4rem)]">
	<SidePane
		open={foldersPaneOpen}
		title={m.resources_folders()}
		collapseLabel={m.resources_collapse_folders()}
		expandLabel={m.resources_expand_folders()}
		ontoggle={toggleFoldersPane}
		onclose={closePaneOnMobile}
	>
		<div class="min-h-0 flex-1 overflow-y-auto px-3 pb-3">
			<FolderTree
				tree={sidebarTree}
				selected={selectedFolder}
				expanded={expandedFolders}
				allLabel={m.resources_all()}
				allIcon="tabler:layout-list"
				allCount={resources.length}
				expandLabel={() => m.resources_expand_folder()}
				collapseLabel={() => m.resources_collapse_folder()}
				onselect={selectFolder}
				ontoggle={toggleFolder}
			/>
		</div>
	</SidePane>

	<div class="@container flex min-w-0 flex-1 flex-col">
		<div class="shrink-0 space-y-3 bg-base-100 px-6 pb-4 pt-6">
			<ListHeader
				allLabel={m.resources_all()}
				folder={selectedFolder}
				onselect={selectFolder}
				count={m.resources_item_count({ count: visibleResources.length })}
				newHref={authoring
					? `${base}/edit?new=resource${selectedFolder ? `&folder=${encodePath(selectedFolder)}` : ''}`
					: null}
			/>
		</div>

		<div class="flex min-h-0 flex-1 flex-col overflow-y-auto px-6 pb-6">
		{#if loading}
			<Loading />
		{:else if error}
			<StateMessage tone="oops" title={m.resources_load_failed()} message={error} />
		{:else if visibleResources.length === 0}
			<StateMessage
				tone="notice"
				title={m.resources_empty_title()}
				message={authoring ? undefined : m.resources_empty_message()}
			/>
		{:else}
			<div class="-mx-6 shrink-0 overflow-x-auto px-6">
				<table class="table table-sm w-full bg-base-100">
					<thead class="bg-base-100 text-xs font-medium text-base-content/50">
						<tr>
							<th>{m.resources_column_key()}</th>
							<th>{m.resources_column_type()}</th>
							<th class="text-right">{m.resources_column_size()}</th>
						</tr>
					</thead>
					<tbody>
						{#each visibleResources as resource (resource.key)}
							<tr
								class="cursor-pointer transition-colors hover:bg-base-200 {selected === resource.key
									? 'bg-base-200'
									: ''}"
								onclick={() => openResource(resource.key)}
								ondblclick={() => goto(`${base}/resources/${encodePath(resource.key)}`)}
								onkeydown={(e) => {
									if (e.key === 'Enter' || e.key === ' ') openResource(resource.key);
								}}
								tabindex="0"
								role="button"
							>
								<td class="w-full max-w-0"><div class="truncate">{resource.key}</div></td>
								<td>
									{#if resource.extension}
										<span class="text-xs text-base-content/50">{resource.extension}</span>
									{:else}
										<span class="opacity-50">—</span>
									{/if}
								</td>
								<td class="whitespace-nowrap text-right text-sm opacity-70">
									{formatSize(resource.size)}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
		</div>
	</div>

	{#if selected}
		{@const key = selected}
		<Inspector title={selectedLeaf} label={m.resource_viewer_label()} onclose={closeResource}>
			{#snippet subtitle()}
				<span class="truncate">{key}</span>
			{/snippet}
			{#snippet actions()}
				<CopyButton text={selectedContent?.content} label={m.common_copy()} tooltip="bottom" />
				{#if targetOf(targets, `resources/${key}`)}
					<IconButton icon="tabler:pencil" label={m.common_edit()} href={editUrl(base, `resources/${key}`)} />
				{/if}
				<IconButton
					icon="tabler:arrow-up-right"
					label={m.resource_open_full_page()}
					href="{base}/resources/{encodePath(key)}"
				/>
			{/snippet}
			{#if selectedLoading}
				<Loading size="md" />
			{:else if selectedError}
				<StateMessage tone="oops" title={m.resource_load_failed()} message={selectedError} />
			{:else if selectedContent}
				<div class="-mx-6 overflow-auto bg-base-200 text-xs">
					<ResourceViewer content={selectedContent.content} extension={selectedContent.extension} />
				</div>
			{/if}
		</Inspector>
	{/if}
</section>

