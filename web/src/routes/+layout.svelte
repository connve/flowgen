<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import { env } from '$env/dynamic/public';
	import { apiUrl } from '$lib/api';
	import Badge from '$lib/Badge.svelte';
	import Icon from '@iconify/svelte';
	import '../app.css';

	let { children } = $props();
	let collapsed = $state(false);
	let dark = $state(false);
	let version = $state<string | null>(null);
	let currentPath = $derived(page.url.pathname);

	// Hides outer chrome when embedded (e.g. console.connve.dev); nav stays.
	const chromeless = env.PUBLIC_FLOWGEN_CHROME === 'embedded';

	onMount(async () => {
		const stored = localStorage.getItem('flowgen-theme');
		if (stored === 'dark' || stored === 'light') {
			dark = stored === 'dark';
		} else {
			// Fall back to the OS setting when the user hasn't chosen yet,
			// then track further OS changes as long as they haven't overridden.
			const mq = window.matchMedia('(prefers-color-scheme: dark)');
			dark = mq.matches;
			mq.addEventListener('change', (e) => {
				if (!localStorage.getItem('flowgen-theme')) {
					dark = e.matches;
					applyTheme();
				}
			});
		}
		applyTheme();
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

	function toggleTheme() {
		dark = !dark;
		localStorage.setItem('flowgen-theme', dark ? 'dark' : 'light');
		applyTheme();
	}

	function applyTheme() {
		if (typeof document !== 'undefined') {
			document.documentElement.setAttribute('data-theme', dark ? 'mydark' : 'mytheme');
		}
	}
</script>

<div class="min-h-screen bg-base-100 text-base-content">
	<div class="flex min-h-screen">
		<aside
			class="flex flex-col border-r border-base-200 bg-base-100 transition-[width] duration-200 ease-out {collapsed
				? 'w-14'
				: 'w-56'}"
		>
			{#if !chromeless}
				<div
					class="flex h-16 shrink-0 items-center border-b border-base-200 {collapsed
						? 'justify-center'
						: 'px-3'}"
				>
					<a
						href="{base}/"
						class="flex h-10 w-10 items-center justify-center"
						aria-label="CONNVE home"
					>
						<img src="{base}/favicon.png" alt="CONNVE" class="h-7 w-7 shrink-0" />
					</a>
				</div>
			{/if}

			<nav
				class="flex-1 space-y-0.5 py-2 {collapsed ? 'flex flex-col items-center' : 'px-3'}"
			>
				{#each [{ href: '/', icon: 'tabler:sitemap', label: 'Flows', match: (p: string) => p === base + '/' || p === base }, { href: '/resources', icon: 'tabler:file-code', label: 'Resources', match: (p: string) => p.startsWith(base + '/resources') }] as item (item.href)}
					{@const active = item.match(currentPath)}
					<a
						href="{base}{item.href}"
						class="relative flex h-10 items-center rounded-md text-sm font-medium transition-colors hover:bg-base-200 {active
							? 'bg-base-200 text-primary'
							: 'text-base-content'} {collapsed ? 'w-10 justify-center' : 'gap-3 px-3'}"
						title={item.label}
						aria-label={item.label}
						aria-current={active ? 'page' : undefined}
					>
						{#if active && !collapsed}
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
				class="flex h-12 shrink-0 items-center border-t border-base-200 {collapsed
					? 'justify-center'
					: 'justify-between px-3'}"
			>
				{#if !collapsed && version}
					<Badge>{version}</Badge>
				{/if}
				<button
					type="button"
					aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
					class="flex h-10 w-10 items-center justify-center rounded-md text-base-content/70 transition-colors hover:bg-base-200 hover:text-base-content"
					onclick={() => (collapsed = !collapsed)}
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
				<header class="flex h-16 items-center justify-end gap-3 border-b border-base-200 px-6">
					<button
						type="button"
						class="btn btn-ghost btn-sm btn-circle"
						aria-label={dark ? 'Switch to light mode' : 'Switch to dark mode'}
						onclick={toggleTheme}
					>
						{#if dark}
							<Icon icon="tabler:sun" class="h-6 w-6" />
						{:else}
							<Icon icon="tabler:moon" class="h-6 w-6" />
						{/if}
					</button>

					<div
						class="flex h-8 w-8 items-center justify-center rounded-full bg-base-200 text-base-content/70"
						title="Signed in"
						aria-label="User"
					>
						<Icon icon="tabler:user" class="h-6 w-6" />
					</div>
				</header>
			{/if}

			<main class="flex-1 bg-base-100">
				{@render children()}
			</main>
		</div>
	</div>
</div>
