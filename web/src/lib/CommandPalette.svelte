<script lang="ts">
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import {
		encodePath,
		listChanges,
		listConnections,
		listFlows,
		listResources,
		type ChangeSummary,
		type Connection,
		type FlowSummary,
		type ResourceSummary
	} from '$lib/api';
	import { agentSessions } from '$lib/agentSessions.svelte';
	import { authoringEnabled } from '$lib/changes';
	import { connectorByName } from '$lib/connectors';
	import { m } from '$lib/paraglide/messages.js';
	import { SearchPalette } from '@connve/ui';
	import { untrack } from 'svelte';

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
	let loading = $state(false);
	let authoring = $state(false);
	let flows = $state<FlowSummary[]>([]);
	let resources = $state<ResourceSummary[]>([]);
	let connections = $state<Connection[]>([]);
	let changes = $state<ChangeSummary[]>([]);

	function list<T>(call: Promise<{ data: T[] | undefined }>): Promise<T[]> {
		return call.then(
			({ data }) => data ?? [],
			() => []
		);
	}

	async function load() {
		loading = flows.length === 0;
		const enabled = await authoringEnabled();
		[flows, resources, connections, changes] = await Promise.all([
			list(listFlows()),
			list(listResources()),
			list(listConnections()),
			enabled ? list(listChanges().then(({ data }) => ({ data: data.changes }))) : Promise.resolve([])
		]);
		authoring = enabled;
		loading = false;
	}

	$effect(() => {
		if (open) void untrack(load);
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
			agents ? { label: m.agents_new_session(), icon: 'tabler:prompt', href: '/agents' } : null,
			authoring ? { label: m.editor_new_flow(), icon: 'tabler:binary-tree', href: '/edit?new=flow' } : null,
			authoring ? { label: m.editor_new_resource(), icon: 'tabler:file-code', href: '/edit?new=resource' } : null
		]
			.filter((action) => action !== null)
			.map((action) => ({ ...action, group: m.search_actions(), href: `${base}${action.href}` }))
	);

	// Groups in the order of the navigation tabs.
	let items = $derived<Result[]>([
		...agentSessions.list.map((session) => ({
			group: m.nav_agents(),
			label: session.title,
			detail: session.folder ?? undefined,
			icon: 'tabler:message-2',
			href: `${base}/agents/${encodeURIComponent(session.id)}`
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

</script>

{#snippet row(result: Result)}
	{#if result.image}
		<img src={result.image} alt="" class="h-4 w-4 shrink-0" />
	{:else if result.icon}
		<Icon icon={result.icon} class="h-4 w-4 shrink-0 opacity-70" />
	{/if}
	<span class="truncate">{result.label}</span>
	{#if result.detail}
		<span class="ml-auto truncate pl-2 text-xs text-base-content/50">{result.detail}</span>
	{/if}
{/snippet}

<SearchPalette
	bind:open
	bind:query
	groups={results}
	{row}
	{loading}
	placeholder={m.search_placeholder()}
	href={(result) => result.href}
	key={(result) => result.href + result.label}
	onchoose={(result) => goto(result.href)}
/>
