<script lang="ts">
	import '../app.css';
	import 'prismjs/themes/prism-tomorrow.css';
	import { base } from '$app/paths';
	import Sidebar from '$lib/components/Sidebar.svelte';
	import { initPosthog, capture } from '$lib/posthog';
	import { onMount } from 'svelte';

	let { children } = $props();

	let nlEmail = $state('');
	let nlSubmitted = $state(false);
	let nlError = $state<string | null>(null);

	const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

	function handleNewsletter(event: SubmitEvent) {
		event.preventDefault();
		const email = nlEmail.trim();
		if (!email || !EMAIL_RE.test(email)) {
			nlError = 'Please enter a valid email address.';
			return;
		}
		nlError = null;
		capture('newsletter_signup', { email });
		nlSubmitted = true;
	}

	onMount(() => {
		initPosthog();

		requestAnimationFrame(() => {
			document.querySelectorAll('.prose table').forEach((table) => {
				if (table.parentElement?.classList.contains('table-wrapper')) return;
				const wrapper = document.createElement('div');
				wrapper.className = 'table-wrapper';
				table.parentNode?.insertBefore(wrapper, table);
				wrapper.appendChild(table);
			});

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

<div class="newsletter-strip bg-neutral text-base-100">
	<div class="container mx-auto px-4 py-10 sm:px-6 lg:px-8">
		<div class="newsletter-section">
			<div class="newsletter-text">
				<h3 class="text-base font-semibold">Get the edge.</h3>
				<p class="mt-1 text-sm opacity-60">News, releases, and insights — no noise.</p>
			</div>
			{#if nlSubmitted}
				<p class="text-sm font-medium text-accent">You're in. Watch your inbox.</p>
			{:else}
				<form onsubmit={handleNewsletter} class="newsletter-form">
					<input
						type="email"
						bind:value={nlEmail}
						placeholder="you@company.com"
						required
						class="newsletter-input"
					/>
					<button type="submit" class="newsletter-btn">Subscribe</button>
				</form>
			{/if}
			{#if nlError}
				<p class="mt-1 text-xs text-error">{nlError}</p>
			{/if}
		</div>
	</div>
</div>

<footer class="border-t border-base-200">
	<div class="container mx-auto px-4 py-10 sm:px-6 lg:px-8">
		<div class="flex flex-col items-start justify-between gap-6 md:flex-row md:items-center">
			<div class="flex items-center gap-6">
				<a href="https://connve.com"><img class="h-8 w-auto" src="https://connve.com/favicon.png" alt="CONNVE" /></a>
				<div class="flex flex-col gap-1 text-xs opacity-60">
					<div class="flex items-center gap-2">
						<span>ul. Złota 75A lok. 7, 00-819 Warszawa</span>
						<span class="rounded border border-current px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider">HQ</span>
					</div>
					<div>NIP 5090052574 · REGON 381354056</div>
				</div>
			</div>
			<div class="flex items-center gap-3">
				<a href="mailto:hello@connve.com" aria-label="Email CONNVE" class="social-circle">
					<svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
						<path stroke-linecap="round" stroke-linejoin="round" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
					</svg>
				</a>
				<a href="https://www.linkedin.com/company/connve/" target="_blank" rel="noopener" aria-label="CONNVE on LinkedIn" class="social-circle">
					<svg class="h-5 w-5" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
						<path d="M19 0h-14c-2.761 0-5 2.239-5 5v14c0 2.761 2.239 5 5 5h14c2.762 0 5-2.239 5-5v-14c0-2.761-2.238-5-5-5zm-11 19h-3v-11h3v11zm-1.5-12.268c-.966 0-1.75-.79-1.75-1.764s.784-1.764 1.75-1.764 1.75.79 1.75 1.764-.783 1.764-1.75 1.764zm13.5 12.268h-3v-5.604c0-3.368-4-3.113-4 0v5.604h-3v-11h3v1.765c1.396-2.586 7-2.777 7 2.476v6.759z" />
					</svg>
				</a>
				<a href="https://github.com/connve" target="_blank" rel="noopener" aria-label="CONNVE on GitHub" class="social-circle">
					<svg class="h-5 w-5" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
						<path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z" />
					</svg>
				</a>
				<a href="https://helm.connve.com/" target="_blank" rel="noopener" aria-label="CONNVE Helm repository" class="social-circle">
					<img class="h-5 w-5" src="https://connve.com/helm.svg" alt="Helm" aria-hidden="true" />
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

	.newsletter-section {
		display: flex;
		flex-direction: column;
		gap: 0.75rem;
	}

	@media (min-width: 768px) {
		.newsletter-section {
			flex-direction: row;
			align-items: center;
			justify-content: space-between;
		}
	}

	.newsletter-form {
		display: flex;
		gap: 0.5rem;
		width: 100%;
	}

	@media (min-width: 768px) {
		.newsletter-form {
			width: auto;
		}
	}

	.newsletter-input {
		flex: 1;
		min-width: 0;
		padding: 0.5rem 0.75rem;
		font-size: 0.875rem;
		border: 1px solid rgb(255 255 255 / 0.2);
		border-radius: 0.5rem;
		background: rgb(255 255 255 / 0.1);
		color: inherit;
		outline: none;
		transition: border-color 0.15s ease;
	}

	.newsletter-input:focus {
		border-color: #00e168;
	}

	.newsletter-input::placeholder {
		color: rgb(255 255 255 / 0.4);
	}

	.newsletter-btn {
		padding: 0.5rem 1.25rem;
		font-size: 0.875rem;
		font-weight: 500;
		color: #0e271b;
		background-color: #00e168;
		border: none;
		border-radius: 0.5rem;
		cursor: pointer;
		white-space: nowrap;
		transition:
			background-color 0.15s ease,
			transform 0.15s ease;
	}

	.newsletter-btn:hover {
		background-color: #00c85a;
		transform: translateY(-1px);
	}
</style>
