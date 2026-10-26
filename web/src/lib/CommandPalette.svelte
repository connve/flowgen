<script lang="ts">
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import {
		apiUrl,
		encodePath,
		type ChangeSummary,
		type Connection,
		type FlowSummary,
		type ResourceSummary
	} from '$lib/api';
	import { agentConversations } from '$lib/agentConversations.svelte';
	import { authoringEnabled } from '$lib/changes';
	import { connectorByName } from '$lib/connectors';
	import { m } from '$lib/paraglide/messages.js';
	import { tick, untrack } from 'svelte';

	let { open = $bindable(false), agents }: { open: boolean; agents: boolean } = $props();

	interface Result {
		group: string;
		label: string;
		detail?: string;
		icon?: string;
		image?: string;
		href: string;
	}

	const PER_GROUP = 6;

	let query = $state('');
	let active = $state(0);
	let input = $state<HTMLInputElement | null>(null);
	let loading = $state(false);
	let authoring = $state(false);
	let flows = $state<FlowSummary[]>([]);
	let resources = $state<ResourceSummary[]>([]);
	let connections = $state<Connection[]>([]);
	let changes = $state<ChangeSummary[]>([]);

	async function list<T>(path: string, field?: string): Promise<T[]> {
		try {
			const res = await fetch(apiUrl(path));
			if (!res.ok) return [];
			const body = await res.json();
			return (field ? body[field] : body) ?? [];
		} catch {
			return [];
		}
	}

	async function load() {
		loading = flows.length === 0;
		const enabled = await authoringEnabled();
		[flows, resources, connections, changes] = await Promise.all([
			list<FlowSummary>('api/flows'),
			list<ResourceSummary>('api/resources'),
			list<Connection>('api/connections'),
			enabled ? list<ChangeSummary>('api/changes', 'changes') : Promise.resolve([])
		]);
		authoring = enabled;
		loading = false;
	}

	$effect(() => {
		if (!open) return;
		untrack(() => {
			query = '';
			active = 0;
			void load();
		});
		void tick().then(() => input?.focus());
	});

	let pages = $derived<Result[]>(
		[
			agents ? { label: m.nav_agents(), icon: 'tabler:prompt', href: '/agents' } : null,
			{ label: m.nav_flows(), icon: 'tabler:binary-tree', href: '/' },
			{ label: m.nav_resources(), icon: 'tabler:file-code', href: '/resources' },
			{ label: m.nav_connections(), icon: 'tabler:plug-connected', href: '/connections' },
			{ label: m.monitor_tab_logs(), icon: 'tabler:logs', href: '/monitor/logs' },
			{ label: m.monitor_tab_pods(), icon: 'tabler:box', href: '/monitor/pods' },
			authoring ? { label: m.changes_breadcrumb(), icon: 'tabler:git-pull-request', href: '/changes' } : null,
			{ label: m.nav_system(), icon: 'tabler:settings', href: '/system' }
		]
			.filter((page) => page !== null)
			.map((page) => ({ ...page, group: m.search_pages(), href: `${base}${page.href}` }))
	);

	let actions = $derived<Result[]>(
		[
			agents ? { label: m.agents_new_conversation(), icon: 'tabler:prompt', href: '/agents' } : null,
			authoring ? { label: m.editor_new_flow(), icon: 'tabler:binary-tree', href: '/edit?new=flow' } : null,
			authoring ? { label: m.editor_new_resource(), icon: 'tabler:file-code', href: '/edit?new=resource' } : null
		]
			.filter((action) => action !== null)
			.map((action) => ({ ...action, group: m.search_actions(), href: `${base}${action.href}` }))
	);

	// Groups in the order of the navigation tabs.
	let items = $derived<Result[]>([
		...agentConversations.list.map((conversation) => ({
			group: m.nav_agents(),
			label: conversation.title,
			detail: conversation.folder ?? undefined,
			icon: 'tabler:message-2',
			href: `${base}/agents/${encodeURIComponent(conversation.id)}`
		})),
		...flows.map((flow) => ({
			group: m.nav_flows(),
			label: flow.display_name || flow.name,
			detail: [flow.path, flow.description, ...flow.tags].filter(Boolean).join(' · '),
			icon: 'tabler:binary-tree',
			href: `${base}/flows/${encodePath(flow.path)}`
		})),
		...resources.map((resource) => ({
			group: m.nav_resources(),
			label: resource.key,
			icon: 'tabler:file-code',
			href: `${base}/resources/${encodePath(resource.key)}`
		})),
		...connections.map((connection) => {
			const connector = connectorByName(connection.connector);
			return {
				group: m.nav_connections(),
				label: connector.label,
				detail: [connection.account, connection.credentialsPath, ...connection.targets]
					.filter(Boolean)
					.join(' · '),
				image: connector.iconUrl,
				href: `${base}/?connection=${encodeURIComponent(connection.id)}`
			};
		}),
		...changes.map((change) => ({
			group: m.changes_breadcrumb(),
			label: change.title,
			detail: change.paths.join(' · '),
			icon: 'tabler:git-pull-request',
			href: `${base}/changes/${encodeURIComponent(change.id)}`
		})),
		...pages,
		...actions
	]);

	// Every word must appear in the label or detail; label prefixes rank first.
	function rank(result: Result, words: string[]): number | null {
		const label = result.label.toLowerCase();
		const haystack = `${label} ${result.detail?.toLowerCase() ?? ''}`;
		if (!words.every((word) => haystack.includes(word))) return null;
		if (label.startsWith(words[0])) return 0;
		return label.includes(words[0]) ? 1 : 2;
	}

	let results = $derived.by(() => {
		const words = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
		const found =
			words.length === 0
				? [...pages, ...actions]
				: items
						.map((result) => ({ result, score: rank(result, words) }))
						.filter((hit): hit is { result: Result; score: number } => hit.score !== null)
						.sort((a, b) => a.score - b.score)
						.map((hit) => hit.result);
		const shown = new Map<string, Result[]>(items.map((result) => [result.group, []]));
		for (const result of found) {
			const group = shown.get(result.group) ?? [];
			if (group.length < PER_GROUP) group.push(result);
			shown.set(result.group, group);
		}
		return [...shown.entries()].filter(([, group]) => group.length > 0);
	});

	let flat = $derived(results.flatMap(([, group]) => group));

	$effect(() => {
		if (active >= flat.length) active = Math.max(0, flat.length - 1);
	});

	function close() {
		open = false;
	}

	function choose(result: Result | undefined) {
		if (!result) return;
		close();
		goto(result.href);
	}

	function onkeydown(event: KeyboardEvent) {
		if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
			event.preventDefault();
			if (flat.length === 0) return;
			const step = event.key === 'ArrowDown' ? 1 : -1;
			active = (active + step + flat.length) % flat.length;
			void tick().then(() =>
				document.getElementById(`search-result-${active}`)?.scrollIntoView({ block: 'nearest' })
			);
		} else if (event.key === 'Enter') {
			event.preventDefault();
			choose(flat[active]);
		} else if (event.key === 'Escape') {
			event.preventDefault();
			event.stopPropagation();
			close();
		}
	}
</script>

{#if open}
	<div class="fixed inset-0 z-50 bg-black/50" aria-hidden="true" onclick={close}></div>
	<div
		class="fixed left-1/2 top-4 z-50 w-[calc(100%-2rem)] max-w-xl -translate-x-1/2 overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-xl md:top-24"
		role="dialog"
		aria-modal="true"
		aria-label={m.search_open()}
	>
		<div class="flex items-center gap-2 border-b border-base-200 px-4">
			<Icon icon="tabler:search" class="h-4 w-4 shrink-0 opacity-50" />
			<input
				bind:this={input}
				bind:value={query}
				{onkeydown}
				oninput={() => (active = 0)}
				type="text"
				class="h-12 w-full bg-transparent text-sm outline-none"
				placeholder={m.search_placeholder()}
				role="combobox"
				aria-expanded="true"
				aria-controls="search-results"
				aria-activedescendant={flat.length > 0 ? `search-result-${active}` : undefined}
				autocomplete="off"
				spellcheck="false"
			/>
			{#if loading}
				<span class="loading loading-spinner loading-xs text-primary"></span>
			{/if}
		</div>
		<ul id="search-results" class="max-h-96 overflow-y-auto p-1" role="listbox">
			{#each results as [group, groupResults] (group)}
				<li role="presentation" class="px-3 pb-1 pt-2 text-[10px] font-medium uppercase tracking-wide text-base-content/40">
					{group}
				</li>
				{#each groupResults as result (result.href + result.label)}
					{@const index = flat.indexOf(result)}
					<li role="presentation">
						<a
							id="search-result-{index}"
							href={result.href}
							role="option"
							aria-selected={index === active}
							tabindex="-1"
							class="flex items-center gap-2.5 rounded-md px-3 py-2 text-sm {index === active
								? 'bg-base-200 text-primary'
								: ''}"
							onmousemove={() => (active = index)}
							onclick={close}
						>
							{#if result.image}
								<img src={result.image} alt="" class="h-4 w-4 shrink-0" />
							{:else if result.icon}
								<Icon icon={result.icon} class="h-4 w-4 shrink-0 opacity-70" />
							{/if}
							<span class="truncate">{result.label}</span>
							{#if result.detail}
								<span class="ml-auto truncate pl-2 text-xs text-base-content/50">{result.detail}</span>
							{/if}
						</a>
					</li>
				{/each}
			{:else}
				<li class="px-3 py-6 text-center text-sm text-base-content/50">{m.search_no_results()}</li>
			{/each}
		</ul>
	</div>
{/if}
