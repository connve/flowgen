<script lang="ts">
	import '../app.css';
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { afterNavigate } from '$app/navigation';
	import { mount, onMount, tick, unmount } from 'svelte';
	import { fly } from 'svelte/transition';
	import Icon from '@iconify/svelte';
	import {
		CopyButton,
		Logo,
		SearchTrigger,
		SiteFooter,
		ThemeToggle,
		icons,
		isSearchShortcut,
		theme
	} from '@connve/ui';
	import DocsNav from '$lib/components/DocsNav.svelte';
	import PrevNext from '$lib/components/PrevNext.svelte';
	import SearchPalette from '$lib/components/SearchPalette.svelte';
	import Toc from '$lib/components/Toc.svelte';
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

	const BUTTON =
		'flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content';

	let route = $derived(page.url.pathname.slice(base.length).replace(/\/$/, '') || '/');

	let searching = $state(false);
	let drawerOpen = $state(false);
	let drawer = $state<HTMLElement | null>(null);
	let menuButton = $state<HTMLButtonElement | null>(null);
	let copyButtons: ReturnType<typeof mount>[] = [];

	function onWindowKeydown(event: KeyboardEvent) {
		if (!isSearchShortcut(event)) return;
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
		document.documentElement.style.overflow = drawerOpen ? 'hidden' : '';
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
				<Icon icon={icons.menu2} class="h-5 w-5" />
			</button>
			{@render brand()}

			<div class="ml-auto flex items-center gap-2">
				<SearchTrigger placeholder="Search docs…" onclick={() => (searching = true)} />
				<div class="max-sm:hidden">
					<ThemeToggle />
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
					<Icon icon={icons.x} class="h-5 w-5" />
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

	<SiteFooter />
</div>

<SearchPalette bind:open={searching} />
