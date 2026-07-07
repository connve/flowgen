<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import Workflow from 'lucide-svelte/icons/workflow';
	import FileCode from 'lucide-svelte/icons/file-code';
	import Sun from 'lucide-svelte/icons/sun';
	import Moon from 'lucide-svelte/icons/moon';
	import ChevronLeft from 'lucide-svelte/icons/chevron-left';
	import ChevronRight from 'lucide-svelte/icons/chevron-right';
	import '../app.css';

	let { children } = $props();
	let collapsed = $state(false);
	let dark = $state(false);
	let version = $state<string | null>(null);
	let currentPath = $derived(page.url.pathname);

	onMount(async () => {
		const stored = localStorage.getItem('flowgen-theme');
		dark = stored === 'dark';
		applyTheme();
		try {
			const res = await fetch('api/version');
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
			<div class="flex h-16 items-center gap-2 border-b border-base-200 px-3">
				<a
					href="{base}/"
					class="flex flex-1 items-center gap-2 overflow-hidden"
					aria-label="CONNVE home"
				>
					{#if collapsed}
						<img src="{base}/favicon.png" alt="CONNVE" class="h-8 w-8 shrink-0" />
					{:else}
						<img src="{base}/connve.png" alt="CONNVE" class="h-6 w-auto" />
					{/if}
				</a>
				<button
					type="button"
					aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
					class="btn btn-ghost btn-xs btn-circle shrink-0"
					onclick={() => (collapsed = !collapsed)}
				>
					{#if collapsed}
						<ChevronRight class="h-4 w-4" />
					{:else}
						<ChevronLeft class="h-4 w-4" />
					{/if}
				</button>
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
					<Workflow class="h-5 w-5 shrink-0" />
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
					<FileCode class="h-5 w-5 shrink-0" />
					{#if !collapsed}
						<span>Resources</span>
					{/if}
				</a>
			</nav>

			{#if !collapsed && version}
				<div class="border-t border-base-200 px-4 py-3 text-xs opacity-60">
					flowgen v{version}
				</div>
			{/if}
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
						<Sun class="h-5 w-5" />
					{:else}
						<Moon class="h-5 w-5" />
					{/if}
				</button>

				<div
					class="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10 text-xs font-semibold text-primary"
					title="Signed in (OIDC sync coming later)"
				>
					JD
				</div>
			</header>

			<main class="flex-1 bg-base-100">
				{@render children()}
			</main>
		</div>
	</div>
</div>
