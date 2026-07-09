<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import { apiUrl } from '$lib/api';
	import Badge from '$lib/Badge.svelte';
	import Icon from '@iconify/svelte';
	import '../app.css';

	let { children } = $props();
	let collapsed = $state(false);
	let dark = $state(false);
	let version = $state<string | null>(null);
	let currentPath = $derived(page.url.pathname);

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
				? 'w-16'
				: 'w-56'}"
		>
			<div class="flex h-16 items-center border-b border-base-200 p-2">
				<a
					href="{base}/"
					class="flex items-center gap-3 rounded-md px-3 py-2"
					aria-label="CONNVE home"
				>
					{#if collapsed}
						<img src="{base}/favicon.png" alt="CONNVE" class="h-6 w-6 shrink-0" />
					{:else}
						<img src="{base}/connve.png" alt="CONNVE" class="h-5 w-auto" />
					{/if}
				</a>
			</div>

			<nav class="flex-1 space-y-1 p-2">
				<a
					href="{base}/"
					class="flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium hover:bg-base-200 {currentPath ===
						base + '/' || currentPath === base
						? 'bg-base-200 text-primary'
						: 'text-base-content'}"
					title="Flows"
				>
					<Icon icon="tabler:sitemap" class="h-6 w-6 shrink-0" />
					{#if !collapsed}
						<span>Flows</span>
					{/if}
				</a>
				<a
					href="{base}/resources"
					class="flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium hover:bg-base-200 {currentPath.startsWith(
						base + '/resources'
					)
						? 'bg-base-200 text-primary'
						: 'text-base-content'}"
					title="Resources"
				>
					<Icon icon="tabler:file-code" class="h-6 w-6 shrink-0" />
					{#if !collapsed}
						<span>Resources</span>
					{/if}
				</a>
			</nav>

			<div
				class="flex items-center border-t border-base-200 px-3 py-2 {collapsed
					? 'justify-center'
					: 'justify-between'}"
			>
				{#if !collapsed && version}
					<Badge>v{version}</Badge>
				{/if}
				<button
					type="button"
					aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
					class="btn btn-ghost btn-xs btn-circle"
					onclick={() => (collapsed = !collapsed)}
				>
					{#if collapsed}
						<Icon icon="tabler:chevron-right" class="h-6 w-6" />
					{:else}
						<Icon icon="tabler:chevron-left" class="h-6 w-6" />
					{/if}
				</button>
			</div>
		</aside>

		<div class="flex flex-1 flex-col bg-base-100">
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

			<main class="flex-1 bg-base-100">
				{@render children()}
			</main>
		</div>
	</div>
</div>
