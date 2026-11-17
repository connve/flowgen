<script lang="ts">
	import '../app.css';
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { afterNavigate } from '$app/navigation';
	import { onMount } from 'svelte';
	import {
		NewsletterSignup,
		SearchTrigger,
		SiteFooter,
		SiteHeader,
		SiteMenu,
		type HeaderLink,
		ThemeToggle,
		codeCopy,
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

	const SITE_LINKS: HeaderLink[] = [
		{ href: 'https://connve.com/journal', label: 'Journal' },
		{ href: 'https://connve.com/contact', label: 'Contact' }
	];

	let route = $derived(page.url.pathname.slice(base.length).replace(/\/$/, '') || '/');

	let searching = $state(false);
	let menuOpen = $state(false);
	let menuButton = $state<HTMLButtonElement | null>(null);

	function onWindowKeydown(event: KeyboardEvent) {
		if (!isSearchShortcut(event)) return;
		event.preventDefault();
		menuOpen = false;
		searching = !searching;
	}

	afterNavigate(() => {
		menuOpen = false;
	});

	function subscribe(email: string) {
		identify(email);
		capture('newsletter_signup', { email, marketing_consent: true });
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

<svelte:window onkeydown={onWindowKeydown} />

<div class="min-h-screen bg-base-100 text-base-content">
	<SiteHeader
		home="{base}/"
		homeLabel="Flowgen docs home"
		label="Docs"
		links={SITE_LINKS}
		onmenu={() => (menuOpen = true)}
		{menuOpen}
		bind:menuButton
	>
		{#snippet actions()}
			<SearchTrigger placeholder="Search docs…" onclick={() => (searching = true)} />
			<div class="max-sm:hidden">
				<ThemeToggle />
			</div>
		{/snippet}
	</SiteHeader>

	<SiteMenu bind:open={menuOpen} links={SITE_LINKS} returnFocus={menuButton} label="Documentation">
		<DocsNav {route} />
		{#snippet footer()}
			<ThemeToggle />
		{/snippet}
	</SiteMenu>

	<div class="mx-auto mb-10 flex max-w-[90rem]">
		<aside
			class="sticky top-16 hidden h-[calc(100dvh-4rem)] w-64 shrink-0 overflow-y-auto px-3 pt-6 pb-10 md:block"
		>
			<DocsNav {route} />
		</aside>

		<main class="min-w-0 flex-1 px-4 pt-8 md:px-10 lg:px-12">
			{#if page.error}
				{@render children()}
			{:else}
				<div class="mx-auto max-w-3xl">
					<article class="prose" {@attach codeCopy('Copy code')}>
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

	<NewsletterSignup onsubscribe={subscribe} />

	<SiteFooter />
</div>

<SearchPalette bind:open={searching} />
