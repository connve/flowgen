<script lang="ts">
	import { base } from '$app/paths';
	import { afterNavigate } from '$app/navigation';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import { env } from '$env/dynamic/public';
	import { apiUrl, type UserContext } from '$lib/api';
	import { allMetrics } from '$lib/activityStore.svelte';
	import { agentConversations, followAgentConversations } from '$lib/agentConversations.svelte';
	import { auth } from '$lib/auth.svelte';
	import Badge from '$lib/Badge.svelte';
	import CommandPalette from '$lib/CommandPalette.svelte';
	import Logo from '$lib/Logo.svelte';
	import { sidePaneSlot } from '$lib/sidePaneSlot.svelte';
	import { m } from '$lib/paraglide/messages.js';
	import Icon from '@iconify/svelte';
	import '../app.css';
	import type { LayoutProps } from './$types';

	type ThemePref = 'system' | 'light' | 'dark';

	let { children, data }: LayoutProps = $props();

	const NAV = [
		{ href: '/agents', icon: 'tabler:prompt', label: m.nav_agents(), match: (p: string) => p.startsWith(base + '/agents') },
		{ href: '/', icon: 'tabler:binary-tree', label: m.nav_flows(), match: (p: string) => p === base + '/' || p === base || p.startsWith(base + '/flows') || p.startsWith(base + '/changes') },
		{ href: '/resources', icon: 'tabler:file-code', label: m.nav_resources(), match: (p: string) => p.startsWith(base + '/resources') },
		{ href: '/connections', icon: 'tabler:plug-connected', label: m.nav_connections(), match: (p: string) => p.startsWith(base + '/connections') },
		{ href: '/monitor/logs', icon: 'tabler:activity', label: m.nav_monitor(), match: (p: string) => p.startsWith(base + '/monitor') }
	];
	let nav = $derived(data.agents ? NAV : NAV.filter((item) => item.href !== '/agents'));

	let collapsed = $state(true);
	// On mobile the sidebar is a drawer, opened from the top bar and always full width.
	let menuOpen = $state(false);
	let compact = $derived(collapsed && !menuOpen);
	afterNavigate(() => (menuOpen = false));
	let version = $state<string | null>(null);
	let currentPath = $derived(page.url.pathname);

	// User's explicit preference; `system` tracks the OS `prefers-color-scheme`.
	let themePref = $state<ThemePref>('system');
	// Live OS preference, kept in sync via the media-query listener below so
	// `system` mode reacts to the OS flipping (e.g. auto dark at night).
	let osDark = $state(false);
	// Effective dark state: the OS value when following the system, otherwise
	// the explicit choice.
	let dark = $derived(themePref === 'system' ? osDark : themePref === 'dark');

	// Hides outer chrome when embedded (e.g. console.connve.dev); nav stays.
	const chromeless = env.PUBLIC_FLOWGEN_CHROME === 'embedded';

	let searching = $state(false);
	const shortcut =
		typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform) ? '⌘K' : 'Ctrl K';

	function onWindowKeydown(event: KeyboardEvent) {
		if (data.login || event.key.toLowerCase() !== 'k' || !(event.metaKey || event.ctrlKey)) return;
		event.preventDefault();
		searching = !searching;
	}

	// A boolean, so navigating (a new `data` each route) keeps the stream open.
	let followConversations = $derived(!!data.agents && !data.login);

	let lastEventMs = $derived.by(() => {
		if (data.login || !auth.checked) return 0;
		let latest = 0;
		for (const metrics of Object.values(allMetrics())) {
			latest = Math.max(latest, metrics.last_event_at_ms ?? 0);
		}
		return latest;
	});
	$effect(() => {
		if (!followConversations) return;
		return followAgentConversations();
	});

	// Reapply the DOM theme attribute whenever the effective dark state changes.
	$effect(() => {
		document.documentElement.setAttribute('data-theme', dark ? 'mydark' : 'mytheme');
	});

	onMount(async () => {
		const navState = localStorage.getItem('flowgen-nav-collapsed');
		if (navState !== null) collapsed = navState === '1';

		const stored = localStorage.getItem('flowgen-theme');
		if (stored === 'dark' || stored === 'light' || stored === 'system') {
			themePref = stored;
		}

		const mq = window.matchMedia('(prefers-color-scheme: dark)');
		osDark = mq.matches;
		mq.addEventListener('change', (e) => {
			osDark = e.matches;
		});

		if (data.login) return;
		try {
			const res = await fetch(apiUrl('api/version'));
			if (res.ok) {
				const body = await res.json();
				version = body.version ?? null;
			}
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

	function setTheme(pref: ThemePref) {
		themePref = pref;
		localStorage.setItem('flowgen-theme', pref);
	}
</script>

{#if data.login}
	{@render children()}
{:else if !auth.checked}
	<div class="flex min-h-screen items-center justify-center bg-base-100">
		<span class="loading loading-spinner loading-lg text-primary"></span>
	</div>
{:else}
<div class="min-h-screen overflow-x-hidden bg-base-100 text-base-content">
	<div class="flex min-h-screen w-full min-w-0">
		{#if menuOpen}
			<div class="fixed inset-0 z-30 bg-black/30 md:hidden" aria-hidden="true" onclick={() => (menuOpen = false)}></div>
		{/if}
		<aside
			class="flex shrink-0 flex-col border-r border-base-200 bg-base-100 transition-[width] duration-200 ease-out {menuOpen
				? 'max-md:fixed max-md:inset-y-0 max-md:left-0 max-md:z-40 max-md:w-64 max-md:shadow-xl'
				: 'max-md:hidden'} {compact ? 'w-16' : 'w-56'}"
		>
			<div
				class="group relative flex h-16 shrink-0 items-center {compact
					? 'justify-center'
					: 'justify-between pl-6 pr-3'}"
			>
			{#if !chromeless}
					<a
						href="{base}/"
						class="flex h-10 items-center text-primary transition-opacity {compact
							? 'md:group-hover:opacity-0 md:group-has-[button:focus-visible]:opacity-0'
							: ''}"
						aria-label={m.nav_home()}
					>
						<Logo class="h-6 w-auto shrink-0" beat={lastEventMs} />
					</a>
			{/if}
				<div
					class="tooltip tooltip-right max-md:hidden {compact && !chromeless
						? 'absolute inset-0 m-auto h-8 w-8 opacity-0 transition-opacity group-hover:opacity-100 has-[button:focus-visible]:opacity-100'
						: ''}"
					data-tip={collapsed ? m.nav_expand() : m.nav_collapse()}
				>
					<button
						type="button"
						aria-label={collapsed ? m.nav_expand() : m.nav_collapse()}
						aria-expanded={!collapsed}
						class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/50 transition-colors hover:bg-base-200 hover:text-base-content"
						onclick={() => {
							collapsed = !collapsed;
							localStorage.setItem('flowgen-nav-collapsed', collapsed ? '1' : '0');
						}}
					>
						<Icon
							icon={collapsed ? 'tabler:layout-sidebar-left-expand' : 'tabler:layout-sidebar-left-collapse'}
							class="h-5 w-5"
						/>
					</button>
				</div>
			</div>

			<nav
				class="flex-1 space-y-px pb-2 max-md:pt-2 {compact ? 'flex flex-col items-center' : 'px-3'}"
			>
				{#each nav as item (item.href)}
					{@const active = item.match(currentPath)}
					{@const unread = item.href === '/agents' ? agentConversations.unread : 0}
					{@const label = unread > 0 ? m.nav_agents_unread({ count: unread }) : item.label}
					<a
						href="{base}{item.href}"
						class="relative flex h-9 items-center rounded-md text-sm transition-colors {active
							? 'bg-base-200 font-semibold text-base-content'
							: 'font-medium text-base-content/70 hover:bg-base-200/60 hover:text-base-content'} {compact
							? 'tooltip tooltip-right w-9 justify-center'
							: 'gap-3 px-3'}"
						data-tip={compact ? label : undefined}
						aria-label={label}
						aria-current={active ? 'page' : undefined}
					>
						<Icon icon={item.icon} class="h-5 w-5 shrink-0" />
						{#if !compact}
							<span>{item.label}</span>
						{/if}
						{#if unread > 0}
							{#if compact}
								<span class="badge badge-primary badge-xs absolute -right-1 -top-1 min-w-4 px-1">{unread}</span>
							{:else}
								<span class="badge badge-primary badge-sm ml-auto">{unread}</span>
							{/if}
						{/if}
					</a>
				{/each}
			</nav>

			<div class="flex h-12 shrink-0 items-center {compact ? 'justify-center' : 'px-6'}">
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
			</div>
		</aside>

		{#if sidePaneSlot.pane}
			<div class="relative flex h-screen shrink-0">
				{@render sidePaneSlot.pane()}
			</div>
		{/if}

		<div class="flex min-w-0 flex-1 flex-col bg-base-100">
			<!-- Embedded, the host shows the theme and user; search stays here. -->
			<header class="flex h-16 items-center justify-between gap-3 px-4 md:px-6">
				<div class="flex items-center">
					<button
						type="button"
						class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content md:hidden"
						aria-label={m.nav_menu()}
						onclick={() => (menuOpen = true)}
					>
						<Icon icon="tabler:menu-2" class="h-5 w-5" />
					</button>
					{#if sidePaneSlot.expand}
						<button
							type="button"
							class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content md:hidden"
							aria-label={sidePaneSlot.expand.label}
							onclick={sidePaneSlot.expand.onclick}
						>
							<Icon icon="tabler:layout-sidebar-left-expand" class="h-5 w-5" />
						</button>
					{/if}
				</div>
				<div class="flex items-center justify-end gap-3">
					<div class="tooltip tooltip-bottom" data-tip="{m.search_open()} {shortcut}">
						<button
							type="button"
							class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content"
							aria-label={m.search_open()}
							aria-keyshortcuts="Meta+K Control+K"
							onclick={() => (searching = true)}
						>
							<Icon icon="tabler:search" class="h-5 w-5" />
						</button>
					</div>
					{#if !chromeless}
						<div
							class="flex items-center gap-0.5 rounded-full border border-base-300 bg-base-200/50 p-0.5 max-sm:hidden"
							role="group"
							aria-label={m.theme_label()}
						>
							{#each [{ pref: 'system' as ThemePref, icon: 'tabler:device-desktop', label: m.theme_system() }, { pref: 'light' as ThemePref, icon: 'tabler:sun', label: m.theme_light() }, { pref: 'dark' as ThemePref, icon: 'tabler:moon', label: m.theme_dark() }] as opt (opt.pref)}
								<div class="tooltip tooltip-bottom" data-tip={opt.label}>
									<button
										type="button"
										class="flex h-7 w-7 items-center justify-center rounded-full transition-colors {themePref ===
										opt.pref
											? 'bg-base-100 text-primary shadow-sm'
											: 'text-base-content/60 hover:text-base-content'}"
										aria-label={m.theme_option({ theme: opt.label })}
										aria-pressed={themePref === opt.pref}
										onclick={() => setTheme(opt.pref)}
									>
										<Icon icon={opt.icon} class="h-4 w-4" />
									</button>
								</div>
							{/each}
						</div>

						{#if auth.user}
							<div class="tooltip tooltip-left" data-tip={m.user_sign_out({ name: displayName(auth.user) })}>
								<a
									href={apiUrl('auth/logout')}
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
				</div>
			</header>

			<main class="min-w-0 flex-1 overflow-hidden bg-base-100">
				{@render children()}
			</main>
		</div>
	</div>
</div>
<CommandPalette bind:open={searching} agents={!!data.agents} />
{/if}

<svelte:window onkeydown={onWindowKeydown} />
