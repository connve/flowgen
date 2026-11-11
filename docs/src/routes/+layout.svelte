<script lang="ts">
	import '../app.css';
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { afterNavigate } from '$app/navigation';
	import { mount, onMount, tick, unmount } from 'svelte';
	import { fly } from 'svelte/transition';
	import Icon from '@iconify/svelte';
	import { Badge, CopyButton, Logo, ThemeToggle, theme } from '@connve/ui';
	import DocsNav from '$lib/components/DocsNav.svelte';
	import PrevNext from '$lib/components/PrevNext.svelte';
	import SearchPalette from '$lib/components/SearchPalette.svelte';
	import Toc from '$lib/components/Toc.svelte';
	import { close, github, menu, search } from '$lib/icons';
	import { initPosthog, identify, capture } from '$lib/posthog';

	let { data, children } = $props();

	const jsonLd = $derived(
		JSON.stringify({
			'@context': 'https://schema.org',
			'@type': 'TechArticle',
			headline: data.seo.title,
			description: data.seo.description,
			url: data.seo.canonical,
			inLanguage: 'en',
			image: 'https://connve.com/og.png',
			author: {
				'@type': 'Organization',
				name: 'CONNVE',
				url: 'https://connve.com'
			},
			publisher: {
				'@type': 'Organization',
				name: 'CONNVE',
				url: 'https://connve.com',
				logo: {
					'@type': 'ImageObject',
					url: 'https://connve.com/favicon.png'
				}
			},
			isPartOf: {
				'@type': 'WebSite',
				name: 'Flowgen documentation',
				url: 'https://connve.com/docs/flowgen/'
			}
		})
	);

	const GITHUB = 'https://github.com/connve/flowgen';
	const BUTTON =
		'flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content';

	let route = $derived(page.url.pathname.slice(base.length).replace(/\/$/, '') || '/');

	let searching = $state(false);
	let shortcut = $state('Ctrl K');
	let drawerOpen = $state(false);
	let drawer = $state<HTMLElement | null>(null);
	let menuButton = $state<HTMLButtonElement | null>(null);
	let copyButtons: ReturnType<typeof mount>[] = [];

	function onWindowKeydown(event: KeyboardEvent) {
		if (event.key.toLowerCase() !== 'k' || !(event.metaKey || event.ctrlKey)) return;
		event.preventDefault();
		drawerOpen = false;
		searching = !searching;
	}

	async function openDrawer() {
		drawerOpen = true;
		await tick();
		(
			drawer?.querySelector<HTMLElement>('[aria-current="page"]') ??
			drawer?.querySelector<HTMLElement>('nav a[href]')
		)?.focus({ preventScroll: true });
	}

	function closeDrawer() {
		if (!drawerOpen) return;
		drawerOpen = false;
		menuButton?.focus();
	}

	function onDrawerKeydown(event: KeyboardEvent) {
		if (!drawer) return;
		if (event.key === 'Escape') {
			closeDrawer();
			return;
		}
		if (event.key !== 'Tab') return;
		const focusable = [...drawer.querySelectorAll<HTMLElement>('a[href], button:not([disabled])')];
		const first = focusable[0];
		const last = focusable[focusable.length - 1];
		if (event.shiftKey && document.activeElement === first) {
			event.preventDefault();
			last?.focus();
		} else if (!event.shiftKey && document.activeElement === last) {
			event.preventDefault();
			first?.focus();
		}
	}

	$effect(() => {
		document.documentElement.style.overflow = drawerOpen || searching ? 'hidden' : '';
	});

	afterNavigate(() => {
		drawerOpen = false;
		for (const button of copyButtons) unmount(button);
		copyButtons = [];
		document.querySelectorAll('article.prose pre').forEach((pre) => {
			pre.querySelectorAll(':scope > .code-copy, :scope > .copy-btn').forEach((old) => old.remove());
			const text = (pre.querySelector('code') ?? pre).textContent?.trimEnd() ?? '';
			const host = document.createElement('div');
			host.className = 'code-copy';
			pre.append(host);
			copyButtons.push(mount(CopyButton, { target: host, props: { text, label: 'Copy code' } }));
		});
	});

	let nlEmail = $state('');
	let nlConsent = $state(false);
	let nlSubmitted = $state(false);
	let nlError = $state<string | null>(null);

	const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

	function handleEmailInput() {
		nlConsent = nlEmail.trim().length > 0;
	}

	function handleNewsletter(event: SubmitEvent) {
		event.preventDefault();
		const email = nlEmail.trim();
		if (!email || !EMAIL_RE.test(email)) {
			nlError = 'Please enter a valid email address.';
			return;
		}
		if (!nlConsent) {
			nlError = 'Please agree to receive marketing communication.';
			return;
		}
		nlError = null;
		identify(email);
		capture('newsletter_signup', { email, marketing_consent: true });
		nlSubmitted = true;
	}

	onMount(() => {
		if (/Mac|iPhone|iPad/.test(navigator.platform)) shortcut = '⌘K';
		initPosthog();
		return theme.start();
	});
</script>

<svelte:head>
	<title>{data.seo.title}</title>
	<meta name="description" content={data.seo.description} />
	<link rel="canonical" href={data.seo.canonical} />
	<meta property="og:type" content="article" />
	<meta property="og:url" content={data.seo.canonical} />
	<meta property="og:title" content={data.seo.title} />
	<meta property="og:description" content={data.seo.description} />
	<meta property="og:site_name" content="CONNVE" />
	<meta property="og:image" content="https://connve.com/og.png" />
	<meta property="og:image:width" content="1200" />
	<meta property="og:image:height" content="630" />
	<meta name="twitter:card" content="summary_large_image" />
	<meta name="twitter:title" content={data.seo.title} />
	<meta name="twitter:description" content={data.seo.description} />
	<meta name="twitter:image" content="https://connve.com/og.png" />
	{@html `<script type="application/ld+json">${jsonLd}</script>`}
</svelte:head>

<svelte:window onkeydown={onWindowKeydown} onpopstate={() => (drawerOpen = false)} />

{#snippet brand()}
	<a href="{base}/" class="flex h-10 items-center gap-2.5 text-primary" aria-label="Flowgen docs home">
		<Logo class="h-6 w-auto shrink-0" />
		<span class="text-[15px] font-semibold tracking-tight text-base-content">Docs</span>
	</a>
{/snippet}

<div class="min-h-screen bg-base-100 text-base-content">
	<header class="sticky top-0 z-40 border-b border-base-200 bg-base-100/90 backdrop-blur">
		<div class="mx-auto flex h-16 max-w-[90rem] items-center gap-2 px-4 md:px-6">
			<button
				bind:this={menuButton}
				type="button"
				class="{BUTTON} -ml-1 md:hidden"
				aria-label="Open navigation"
				aria-expanded={drawerOpen}
				onclick={openDrawer}
			>
				<Icon icon={menu} class="h-5 w-5" />
			</button>
			{@render brand()}

			<div class="ml-auto flex items-center gap-2">
				<button
					type="button"
					class="flex h-8 w-64 items-center gap-2 rounded-md border border-base-300 bg-base-200/50 px-2.5 text-sm text-base-content/60 transition-colors hover:border-base-content/30 hover:text-base-content max-md:hidden"
					aria-label="Search the docs"
					aria-keyshortcuts="Meta+K Control+K"
					onclick={() => (searching = true)}
				>
					<Icon icon={search} class="h-4 w-4 shrink-0" />
					<span>Search docs…</span>
					<kbd
						class="ml-auto rounded border border-base-300 bg-base-100 px-1.5 py-0.5 font-sans text-[10px] leading-none font-medium"
						>{shortcut}</kbd
					>
				</button>
				<div class="tooltip tooltip-bottom md:hidden" data-tip="Search {shortcut}">
					<button
						type="button"
						class={BUTTON}
						aria-label="Search the docs"
						onclick={() => (searching = true)}
					>
						<Icon icon={search} class="h-5 w-5" />
					</button>
				</div>
				<div class="max-sm:hidden">
					<ThemeToggle />
				</div>
				<div class="tooltip tooltip-left" data-tip="GitHub">
					<a
						href={GITHUB}
						target="_blank"
						rel="noopener"
						class={BUTTON}
						aria-label="Flowgen on GitHub"
					>
						<Icon icon={github} class="h-5 w-5" />
					</a>
				</div>
			</div>
		</div>
	</header>

	{#if drawerOpen}
		<div
			class="fixed inset-0 z-50 bg-black/30 md:hidden"
			aria-hidden="true"
			onclick={closeDrawer}
		></div>
		<div
			bind:this={drawer}
			role="dialog"
			aria-modal="true"
			aria-label="Documentation"
			tabindex="-1"
			class="fixed inset-y-0 left-0 z-50 flex w-72 max-w-[85vw] flex-col bg-base-100 shadow-xl md:hidden"
			onkeydown={onDrawerKeydown}
			transition:fly={{ x: -288, duration: 200, opacity: 1 }}
		>
			<div class="flex h-16 shrink-0 items-center justify-between px-4">
				{@render brand()}
				<button type="button" class={BUTTON} aria-label="Close navigation" onclick={closeDrawer}>
					<Icon icon={close} class="h-5 w-5" />
				</button>
			</div>
			<div data-nav-scroll class="min-h-0 flex-1 overflow-y-auto px-3 pt-1 pb-6">
				<DocsNav {route} onnavigate={closeDrawer} />
			</div>
			<div class="flex shrink-0 items-center justify-between border-t border-base-200 px-4 py-3">
				<span class="text-xs text-base-content/60">Theme</span>
				<ThemeToggle />
			</div>
		</div>
	{/if}

	<div class="mx-auto flex max-w-[90rem]">
		<aside
			data-nav-scroll
			class="sticky top-16 hidden h-[calc(100dvh-4rem)] w-64 shrink-0 overflow-y-auto border-r border-base-200 px-3 pt-6 pb-10 md:block"
		>
			<DocsNav {route} />
		</aside>

		<main class="min-w-0 flex-1 px-4 pt-8 pb-16 md:px-10 lg:px-12">
			{#if page.error}
				{@render children()}
			{:else}
				<div class="mx-auto max-w-3xl">
					<article class="prose">
						{@render children()}
					</article>
					<PrevNext prev={data.nav.prev} next={data.nav.next} />
				</div>
			{/if}
		</main>

		{#if !page.error}
			<aside
				class="sticky top-16 hidden h-[calc(100dvh-4rem)] w-60 shrink-0 overflow-y-auto pt-10 pr-6 pb-10 xl:block"
			>
				<Toc />
			</aside>
		{/if}
	</div>

	<section class="border-t border-base-200 bg-base-200/50">
		<div
			class="mx-auto flex max-w-[90rem] flex-col gap-4 px-4 py-10 md:px-6 lg:flex-row lg:items-start lg:justify-between"
		>
			<div>
				<h3 class="text-base font-semibold">Get the edge.</h3>
				<p class="mt-1 text-sm text-base-content/60">News, releases, and insights — no noise.</p>
			</div>
			{#if nlSubmitted}
				<p class="text-sm font-medium text-primary">You're in. Watch your inbox.</p>
			{:else}
				<form onsubmit={handleNewsletter} class="flex w-full max-w-xl flex-col gap-2 lg:w-auto">
					<div class="flex items-start gap-2">
						<input
							type="email"
							bind:value={nlEmail}
							oninput={handleEmailInput}
							placeholder="you@company.com"
							aria-label="Email address"
							required
							class="input input-sm min-w-0 flex-1 border border-base-300 bg-base-100 outline-none focus:border-primary lg:w-80"
						/>
						<button type="submit" class="btn btn-sm btn-primary">Subscribe</button>
					</div>
					<label class="flex cursor-pointer items-start gap-2 text-xs lg:max-w-md">
						<input
							type="checkbox"
							bind:checked={nlConsent}
							class="checkbox mt-0.5 checkbox-xs checkbox-primary"
						/>
						<span class="leading-relaxed text-base-content/60"
							>I consent to receive marketing communication from CONNVE, including emails, ads, and
							personalised content. You can withdraw consent anytime.</span
						>
					</label>
					{#if nlError}
						<p class="text-xs text-error">{nlError}</p>
					{/if}
				</form>
			{/if}
		</div>
	</section>

	<footer class="border-t border-base-200">
		<div class="mx-auto max-w-[90rem] px-4 py-10 md:px-6">
			<div class="flex flex-col items-start justify-between gap-6 md:flex-row md:items-center">
				<div class="flex items-center gap-6">
					<a href="https://connve.com"
						><img class="h-8 w-auto" src="https://connve.com/favicon.png" alt="CONNVE" /></a
					>
					<div class="flex flex-col gap-1 text-xs text-base-content/60">
						<div class="flex items-center gap-2">
							<span>ul. Złota 75A lok. 7, 00-819 Warszawa</span>
							<Badge>HQ</Badge>
						</div>
						<div>NIP 5090052574 · REGON 381354056</div>
					</div>
				</div>
				<div class="flex items-center gap-3">
					<a href="mailto:hello@connve.com" aria-label="Email CONNVE" class="social-circle">
						<svg
							class="h-5 w-5"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
							aria-hidden="true"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"
							/>
						</svg>
					</a>
					<a
						href="https://www.linkedin.com/company/connve/"
						target="_blank"
						rel="noopener"
						aria-label="CONNVE on LinkedIn"
						class="social-circle"
					>
						<svg class="h-5 w-5" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
							<path
								d="M19 0h-14c-2.761 0-5 2.239-5 5v14c0 2.761 2.239 5 5 5h14c2.762 0 5-2.239 5-5v-14c0-2.761-2.238-5-5-5zm-11 19h-3v-11h3v11zm-1.5-12.268c-.966 0-1.75-.79-1.75-1.764s.784-1.764 1.75-1.764 1.75.79 1.75 1.764-.783 1.764-1.75 1.764zm13.5 12.268h-3v-5.604c0-3.368-4-3.113-4 0v5.604h-3v-11h3v1.765c1.396-2.586 7-2.777 7 2.476v6.759z"
							/>
						</svg>
					</a>
					<a
						href="https://github.com/connve"
						target="_blank"
						rel="noopener"
						aria-label="CONNVE on GitHub"
						class="social-circle"
					>
						<svg class="h-5 w-5" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
							<path
								d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"
							/>
						</svg>
					</a>
					<a
						href="https://helm.connve.com/"
						target="_blank"
						rel="noopener"
						aria-label="CONNVE Helm repository"
						class="social-circle"
					>
						<img class="h-5 w-5" src="https://connve.com/helm.svg" alt="Helm" aria-hidden="true" />
					</a>
				</div>
			</div>
			<div class="mt-6 border-t border-base-200 pt-4 text-xs text-base-content/60">
				Copyright &copy; {new Date().getFullYear()} CONNVE. All rights reserved.
			</div>
		</div>
	</footer>
</div>

<SearchPalette bind:open={searching} />

<style>
	.social-circle {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 2.75rem;
		height: 2.75rem;
		border-radius: 9999px;
		background-color: var(--color-base-100);
		color: var(--color-base-content);
		border: 1px solid var(--color-base-300);
		transition:
			color 0.15s ease,
			border-color 0.15s ease,
			transform 0.15s ease;
	}
	.social-circle:hover {
		color: var(--color-primary);
		border-color: var(--color-primary);
		transform: translateY(-1px);
	}
</style>
