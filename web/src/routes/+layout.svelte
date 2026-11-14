<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import { env } from '$env/dynamic/public';
	import {
		AppShell,
		Badge,
		Loading,
		Logo,
		SearchTrigger,
		ThemeToggle,
		isSearchShortcut,
		theme,
		type NavItem
	} from '@connve/ui';
	import { getVersion, type UserContext } from '$lib/api';
	import { logoutUrl } from '$lib/urls';
	import { allMetrics } from '$lib/activityStore.svelte';
	import { agentSessions, followAgentSessions } from '$lib/agentSessions.svelte';
	import { auth } from '$lib/auth.svelte';
	import CommandPalette from '$lib/CommandPalette.svelte';
	import { setUiLabels } from '$lib/uiLabels';
	import { m } from '$lib/paraglide/messages.js';
	import Icon from '@iconify/svelte';
	import '../app.css';
	import type { LayoutProps } from './$types';

	let { children, data }: LayoutProps = $props();

	setUiLabels();

	const NAV = [
		{ href: '/agents', icon: 'tabler:prompt', label: m.nav_agents(), match: (p: string) => p.startsWith(base + '/agents') },
		{ href: '/', icon: 'tabler:binary-tree', label: m.nav_flows(), match: (p: string) => p === base + '/' || p === base || p.startsWith(base + '/flows') || p.startsWith(base + '/changes') },
		{ href: '/resources', icon: 'tabler:file-code', label: m.nav_resources(), match: (p: string) => p.startsWith(base + '/resources') },
		{ href: '/connections', icon: 'tabler:plug-connected', label: m.nav_connections(), match: (p: string) => p.startsWith(base + '/connections') },
		{ href: '/monitor/logs', icon: 'tabler:activity', label: m.nav_monitor(), match: (p: string) => p.startsWith(base + '/monitor') }
	];
	let currentPath = $derived(page.url.pathname);
	let nav = $derived<NavItem[]>(
		NAV.filter((item) => data.agents || item.href !== '/agents').map((item) => {
			const unread = item.href === '/agents' ? agentSessions.unread : 0;
			return {
				href: base + item.href,
				icon: item.icon,
				label: item.label,
				active: item.match(currentPath),
				badge: unread,
				badgeLabel: unread > 0 ? m.nav_agents_unread({ count: unread }) : undefined
			};
		})
	);

	let version = $state<string | null>(null);
	let latest = $state<string | null>(null);

	// Hides outer chrome when embedded (e.g. console.connve.dev); nav stays.
	const chromeless = env.PUBLIC_FLOWGEN_CHROME === 'embedded';

	let searching = $state(false);

	function onWindowKeydown(event: KeyboardEvent) {
		if (data.login || !isSearchShortcut(event)) return;
		event.preventDefault();
		searching = !searching;
	}

	// A boolean, so navigating (a new `data` each route) keeps the stream open.
	let followSessions = $derived(!!data.agents && !data.login);

	let lastEventMs = $derived.by(() => {
		if (data.login || !auth.checked) return 0;
		let latest = 0;
		for (const metrics of Object.values(allMetrics())) {
			latest = Math.max(latest, metrics.last_event_at_ms ?? 0);
		}
		return latest;
	});
	$effect(() => {
		if (!followSessions) return;
		return followAgentSessions();
	});

	onMount(() => theme.start());

	onMount(async () => {
		if (data.login) return;
		try {
			const { data } = await getVersion();
			version = data.version ?? null;
			latest = data.latest ?? null;
		} catch {
			// version is optional
		}
	});

	// Providers vary in which of these they issue, and `user_id` is whatever
	// claim `web.auth.user_id_claim` names — an opaque id for the default `sub`.
	function displayName(u: UserContext): string {
		const claim = (name: string): string | null => {
			const value = u.claims?.[name];
			return typeof value === 'string' && value.trim() !== '' ? value.trim() : null;
		};
		const parts = [claim('given_name'), claim('family_name')].filter(Boolean);
		const fullName = parts.length > 0 ? parts.join(' ') : null;
		return claim('name') ?? fullName ?? claim('email') ?? u.user_id;
	}
</script>

{#snippet brand()}
	<a href="{base}/" class="flex h-10 items-center text-primary" aria-label={m.nav_home()}>
		<Logo class="h-6 w-auto shrink-0" beat={lastEventMs} />
	</a>
{/snippet}

{#snippet railFooter({ compact }: { compact: boolean })}
	<div class="flex items-center gap-2">
		{#if version && !compact}
			<div class="tooltip tooltip-right" data-tip={m.nav_release_notes()}>
				<a
					href="https://github.com/connve/flowgen/releases/tag/v{version}"
					target="_blank"
					rel="noopener noreferrer"
					class="flex h-7 items-center transition-opacity hover:opacity-80"
				>
					<Badge>{version}</Badge>
				</a>
			</div>
			{#if latest}
				<div class="tooltip tooltip-right" data-tip={m.nav_update_available({ version: latest })}>
					<a
						href="https://github.com/connve/flowgen/releases/tag/v{latest}"
						target="_blank"
						rel="noopener noreferrer"
						class="flex h-7 items-center transition-opacity hover:opacity-80"
					>
						<Badge variant="success">{m.nav_update_badge()}</Badge>
					</a>
				</div>
			{/if}
		{/if}
		<div class="tooltip tooltip-right" data-tip={m.nav_system()}>
			<a
				href="{base}/system"
				class="flex h-8 w-8 items-center justify-center rounded-md transition-colors hover:bg-base-200 hover:text-base-content {currentPath.startsWith(
					base + '/system'
				)
					? 'bg-base-200 text-base-content'
					: 'text-base-content/50'}"
				aria-label={m.nav_system()}
			>
				<Icon icon="tabler:adjustments" class="h-5 w-5" />
			</a>
		</div>
	</div>
{/snippet}

<!-- Embedded, the host shows the theme and user; search stays here. -->
{#snippet actions()}
	<SearchTrigger onclick={() => (searching = true)} />
	{#if !chromeless}
		<div class="max-sm:hidden">
			<ThemeToggle />
		</div>

		{#if auth.user}
			<div class="tooltip tooltip-left" data-tip={m.user_sign_out({ name: displayName(auth.user) })}>
				<a
					href={logoutUrl()}
					data-sveltekit-reload
					class="flex h-8 w-8 items-center justify-center rounded-full bg-base-200 text-base-content/70 transition-colors hover:text-base-content"
					aria-label={m.user_sign_out({ name: displayName(auth.user) })}
				>
					<Icon icon="tabler:user-check" class="h-6 w-6" />
				</a>
			</div>
		{:else}
			<div
				class="tooltip tooltip-bottom flex h-8 w-8 items-center justify-center rounded-full bg-base-200 text-base-content/70"
				data-tip={m.user_signed_in()}
				aria-label={m.user_signed_in()}
			>
				<Icon icon="tabler:user" class="h-6 w-6" />
			</div>
		{/if}
	{/if}
{/snippet}

{#if data.login}
	{@render children()}
{:else if !auth.checked}
	<div class="flex min-h-screen bg-base-100">
		<Loading />
	</div>
{:else}
	<AppShell {nav} brand={chromeless ? undefined : brand} {railFooter} {actions}>
		{@render children()}
	</AppShell>
	<CommandPalette bind:open={searching} agents={!!data.agents} />
{/if}

<svelte:window onkeydown={onWindowKeydown} />
