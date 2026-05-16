<script lang="ts">
	import '../app.css';
	import 'prismjs/themes/prism-tomorrow.css';
	import { base } from '$app/paths';
	import Sidebar from '$lib/components/Sidebar.svelte';
	import { onMount } from 'svelte';

	let { children } = $props();

	onMount(() => {
		document.querySelectorAll('pre').forEach((pre) => {
			if (pre.querySelector('.copy-btn')) return;

			const wrapper = document.createElement('div');
			wrapper.style.position = 'relative';
			pre.parentNode?.insertBefore(wrapper, pre);
			wrapper.appendChild(pre);

			const btn = document.createElement('button');
			btn.className = 'copy-btn';
			btn.textContent = 'Copy';
			btn.addEventListener('click', () => {
				const code = pre.querySelector('code');
				if (code) {
					navigator.clipboard.writeText(code.textContent || '');
					btn.textContent = 'Copied';
					setTimeout(() => (btn.textContent = 'Copy'), 2000);
				}
			});
			wrapper.appendChild(btn);
		});
	});
</script>

<svelte:head>
	<title>Flowgen Documentation</title>
	<meta name="description" content="Flowgen — open-source data activation engine written in Rust" />
</svelte:head>

<div class="drawer lg:drawer-open">
	<input id="sidebar-drawer" type="checkbox" class="drawer-toggle" />

	<div class="drawer-content flex flex-col">
		<header class="sticky top-0 z-40 border-b border-base-300 bg-base-100 lg:hidden">
			<nav class="flex items-center justify-between px-4 py-4">
				<a href="{base}/" class="flex items-center gap-2">
					<span class="text-xl font-bold text-primary">flowgen</span>
					<span class="badge badge-outline text-xs px-2 py-0.5">docs</span>
				</a>
				<label for="sidebar-drawer" class="inline-flex cursor-pointer items-center justify-center rounded-md p-1">
					<svg class="h-6 w-6" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
						<path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 12h16M4 18h16" />
					</svg>
				</label>
			</nav>
		</header>

		<main class="flex-1 min-w-0">
			<div class="max-w-3xl mx-auto px-6 py-10">
				<article class="prose">
					{@render children()}
				</article>
			</div>
		</main>
	</div>

	<div class="drawer-side z-50">
		<label for="sidebar-drawer" aria-label="close sidebar" class="drawer-overlay"></label>
		<Sidebar />
	</div>
</div>

<footer class="border-t border-base-200">
	<div class="container mx-auto px-4 py-10 sm:px-6 lg:px-8">
		<div class="flex flex-col items-start justify-between gap-6 md:flex-row md:items-center">
			<div class="flex items-center gap-6">
				<img class="h-8 w-auto" src="{base}/favicon.png" alt="CONNVE" />
				<div class="flex flex-col gap-1 text-xs opacity-60">
					<div class="flex items-center gap-2">
						<span>ul. Złota 75A lok. 7, 00-819 Warszawa</span>
						<span class="rounded border border-current px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider">HQ</span>
					</div>
					<div>NIP 5090052574 · REGON 381354056</div>
				</div>
			</div>
			<div class="flex items-center gap-3">
				<a href="mailto:hello@connve.com" aria-label="Email Connve" class="social-circle">
					<svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
						<path stroke-linecap="round" stroke-linejoin="round" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
					</svg>
				</a>
				<a href="https://www.linkedin.com/company/connve/" target="_blank" rel="noopener" aria-label="Connve on LinkedIn" class="social-circle">
					<svg class="h-5 w-5" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
						<path d="M19 0h-14c-2.761 0-5 2.239-5 5v14c0 2.761 2.239 5 5 5h14c2.762 0 5-2.239 5-5v-14c0-2.761-2.238-5-5-5zm-11 19h-3v-11h3v11zm-1.5-12.268c-.966 0-1.75-.79-1.75-1.764s.784-1.764 1.75-1.764 1.75.79 1.75 1.764-.783 1.764-1.75 1.764zm13.5 12.268h-3v-5.604c0-3.368-4-3.113-4 0v5.604h-3v-11h3v1.765c1.396-2.586 7-2.777 7 2.476v6.759z" />
					</svg>
				</a>
			</div>
		</div>
		<div class="mt-6 border-t border-base-200 pt-4 text-xs opacity-60">
			Copyright &copy; {new Date().getFullYear()} CONNVE. All rights reserved.
		</div>
	</div>
</footer>

<style>
	.social-circle {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 2.75rem;
		height: 2.75rem;
		border-radius: 9999px;
		background-color: #ffffff;
		color: #0e271b;
		border: 1px solid rgb(0 0 0 / 0.08);
		transition:
			color 0.15s ease,
			border-color 0.15s ease,
			transform 0.15s ease;
	}
	.social-circle:hover {
		color: #006b55;
		border-color: #006b55;
		transform: translateY(-1px);
	}
</style>
