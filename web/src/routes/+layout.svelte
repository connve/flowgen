<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import { env } from '$env/dynamic/public';
	import { apiUrl } from '$lib/api';
	import Badge from '$lib/Badge.svelte';
	import Icon from '@iconify/svelte';
	import '../app.css';

	type ThemePref = 'system' | 'light' | 'dark';

	let { children } = $props();
	let collapsed = $state(true);
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

	function setTheme(pref: ThemePref) {
		themePref = pref;
		localStorage.setItem('flowgen-theme', pref);
	}
</script>

<div class="min-h-screen overflow-x-hidden bg-base-100 text-base-content">
	<div class="flex min-h-screen w-full min-w-0">
		<aside
			class="flex shrink-0 flex-col border-r border-base-300 bg-base-100 transition-[width] duration-200 ease-out {collapsed
				? 'w-16'
				: 'w-56'}"
		>
			{#if !chromeless}
				<div
					class="flex h-16 shrink-0 items-center border-b border-base-300 {collapsed
						? 'justify-center'
						: 'px-6'}"
				>
					<a
						href="{base}/"
						class="flex h-10 items-center text-primary {collapsed ? 'w-10 justify-center' : ''}"
						aria-label="CONNVE home"
					>
						<svg
							class="h-6 w-auto shrink-0"
							viewBox="0 0 86 55"
							fill="currentColor"
							xmlns="http://www.w3.org/2000/svg"
							aria-hidden="true"
						>
							<path d="M57.3371 40.7701H85.1608C85.4689 40.7701 85.7176 41.0196 85.7176 41.3264C85.7176 41.635 85.468 41.8835 85.1608 41.8835H57.5684L50.8189 48.6314C50.7156 48.7352 50.5739 48.7943 50.4257 48.7943H26.6342C26.3526 51.8582 23.7705 54.2669 20.6344 54.2669C17.3108 54.2669 14.6081 51.562 14.6081 48.2371C14.6081 44.9132 17.3108 42.2093 20.6344 42.2093C23.7705 42.2093 26.3526 44.616 26.6342 47.6799H50.1953L56.9439 40.933C57.0482 40.8283 57.1899 40.7701 57.3371 40.7701Z"/>
							<path d="M41.2724 33.9604C41.2724 37.2843 38.567 39.9882 35.2434 39.9882C32.1064 39.9882 29.5253 37.5805 29.2418 34.5165H0.556839C0.249627 34.5165 0 34.268 0 33.9604C0 33.6518 0.249627 33.4032 0.556839 33.4032H29.2427C29.5253 30.3393 32.1073 27.9316 35.2434 27.9316C38.5679 27.9316 41.2724 30.6354 41.2724 33.9604Z"/>
							<path d="M57.7558 31.4031H85.1608C85.4689 31.4031 85.7176 31.6526 85.7176 31.9603C85.7176 32.2679 85.468 32.5175 85.1608 32.5175H57.5254C57.3764 32.5175 57.2356 32.4584 57.1304 32.3546L45.3266 20.5522H26.6342C26.3526 23.6162 23.7705 26.0239 20.6344 26.0239C17.3108 26.0239 14.6081 23.3199 14.6081 19.9951C14.6081 16.6693 17.3108 13.9643 20.6344 13.9643C23.7705 13.9643 26.3526 16.373 26.6342 19.4379H45.557C45.7042 19.4379 45.8468 19.496 45.9501 19.6008L57.7558 31.4031Z"/>
							<path d="M67.6543 15.9758C67.3718 19.0398 64.7906 21.4484 61.6536 21.4484C58.33 21.4484 55.6264 18.7427 55.6246 15.4187C55.6246 12.0938 58.33 9.38898 61.6536 9.38898C64.7906 9.38898 67.3718 11.7976 67.6543 14.8616H85.1608C85.4689 14.8616 85.7176 15.1111 85.7176 15.4187C85.7176 15.7263 85.468 15.9758 85.1608 15.9758H67.6543Z"/>
							<path d="M38.6109 6.58503H0.556839C0.249627 6.58503 0 6.33551 0 6.02784C0 5.72027 0.249627 5.47066 0.556839 5.47066H38.6109C38.8943 2.40768 41.4754 0 44.6116 0C47.9351 0 50.6388 2.70392 50.6388 6.02784C50.6388 9.35277 47.9351 12.0567 44.6116 12.0567C41.4754 12.0567 38.8925 9.64892 38.6109 6.58503Z"/>
						</svg>
					</a>
				</div>
			{/if}

			<nav
				class="flex-1 space-y-0.5 py-2 {collapsed ? 'flex flex-col items-center' : 'px-3'}"
			>
				{#each [{ href: '/agents', icon: 'tabler:robot', label: 'Agents', match: (p: string) => p.startsWith(base + '/agents') }, { href: '/', icon: 'tabler:binary-tree', label: 'Flows', match: (p: string) => p === base + '/' || p === base || p.startsWith(base + '/flows') }, { href: '/resources', icon: 'tabler:file-code', label: 'Resources', match: (p: string) => p.startsWith(base + '/resources') }, { href: '/logs', icon: 'tabler:terminal-2', label: 'Logs', match: (p: string) => p.startsWith(base + '/logs') }] as item (item.href)}
					{@const active = item.match(currentPath)}
					<a
						href="{base}{item.href}"
						class="relative flex h-10 items-center rounded-md text-sm font-medium transition-colors hover:bg-base-200 {active
							? 'bg-base-200 text-primary'
							: 'text-base-content'} {collapsed ? 'tooltip tooltip-right w-10 justify-center' : 'gap-3 px-3'}"
						data-tip={collapsed ? item.label : undefined}
						aria-label={item.label}
						aria-current={active ? 'page' : undefined}
					>
						{#if active}
							<span class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"></span>
						{/if}
						<Icon icon={item.icon} class="h-5 w-5 shrink-0" />
						{#if !collapsed}
							<span>{item.label}</span>
						{/if}
					</a>
				{/each}
			</nav>

			<div
				class="flex h-12 shrink-0 items-center border-t border-base-300 {collapsed
					? 'justify-center'
					: 'justify-between pl-6 pr-3'}"
			>
				{#if !collapsed}
					<div class="flex items-center gap-2">
						{#if version}
							<div class="tooltip tooltip-right" data-tip="Release notes">
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
						<div class="tooltip tooltip-right" data-tip="System">
							<a
								href="{base}/system"
								class="flex h-7 w-7 items-center justify-center rounded-md text-base-content/50 transition-colors hover:bg-base-200 hover:text-base-content {currentPath.startsWith(
									base + '/system'
								)
									? 'bg-base-200 text-primary'
									: ''}"
								aria-label="System"
							>
								<Icon icon="tabler:adjustments" class="h-5 w-5" />
							</a>
						</div>
					</div>
				{/if}
				<button
					type="button"
					aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
					data-tip={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
					class="tooltip {collapsed
						? 'tooltip-right'
						: 'tooltip-top'} flex h-10 w-10 items-center justify-center rounded-md text-base-content/70 transition-colors hover:bg-base-200 hover:text-base-content"
					onclick={() => {
						collapsed = !collapsed;
						localStorage.setItem('flowgen-nav-collapsed', collapsed ? '1' : '0');
					}}
				>
					<Icon
						icon={collapsed ? 'tabler:chevron-right' : 'tabler:chevron-left'}
						class="h-5 w-5"
					/>
				</button>
			</div>
		</aside>

		<div class="flex flex-1 flex-col bg-base-100">
			{#if !chromeless}
				<header class="flex h-16 items-center justify-end gap-3 border-b border-base-300 px-6">
					<div
						class="flex items-center gap-0.5 rounded-full border border-base-300 bg-base-200/50 p-0.5"
						role="group"
						aria-label="Color theme"
					>
						{#each [{ pref: 'system' as ThemePref, icon: 'tabler:device-desktop', label: 'System' }, { pref: 'light' as ThemePref, icon: 'tabler:sun', label: 'Light' }, { pref: 'dark' as ThemePref, icon: 'tabler:moon', label: 'Dark' }] as opt (opt.pref)}
							<div class="tooltip tooltip-bottom" data-tip={opt.label}>
								<button
									type="button"
									class="flex h-7 w-7 items-center justify-center rounded-full transition-colors {themePref ===
									opt.pref
										? 'bg-base-100 text-primary shadow-sm'
										: 'text-base-content/60 hover:text-base-content'}"
									aria-label="{opt.label} theme"
									aria-pressed={themePref === opt.pref}
									onclick={() => setTheme(opt.pref)}
								>
									<Icon icon={opt.icon} class="h-4 w-4" />
								</button>
							</div>
						{/each}
					</div>

					<div
						class="tooltip tooltip-bottom flex h-8 w-8 items-center justify-center rounded-full bg-base-200 text-base-content/70"
						data-tip="Signed in"
						aria-label="User"
					>
						<Icon icon="tabler:user" class="h-6 w-6" />
					</div>
				</header>
			{/if}

			<main class="min-w-0 flex-1 overflow-hidden bg-base-100">
				{@render children()}
			</main>
		</div>
	</div>
</div>
