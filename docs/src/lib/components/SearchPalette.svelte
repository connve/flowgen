<script lang="ts">
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import { SearchPalette, icons } from '@connve/ui';
	import { navigation } from '$lib/nav';
	import type { SearchEntry } from '$lib/search';

	let { open = $bindable(false) }: { open: boolean } = $props();

	const LIMIT = 20;
	const PER_PAGE = 3;
	const SNIPPET = 160;

	let query = $state('');
	let entries = $state<SearchEntry[]>([]);
	let loading = $state(false);
	let failed = $state(false);
	let request: Promise<void> | undefined;

	function load() {
		request ??= fetch(`${base}/search.json`)
			.then((response) => (response.ok ? response.json() : Promise.reject(response.status)))
			.then((index: SearchEntry[]) => {
				entries = index;
			})
			.catch(() => {
				failed = true;
				request = undefined;
			})
			.finally(() => (loading = false));
		loading = entries.length === 0;
	}

	$effect(() => {
		if (!open) return;
		failed = false;
		load();
	});

	interface Hit {
		entry: SearchEntry;
		score: number;
		snippet: string;
	}

	const suggestions: SearchEntry[] = navigation[0].items.map((item) => ({
		page: item.title,
		section: navigation[0].title,
		href: item.href,
		text: ''
	}));

	// Every word must appear in the page title, heading or text; title and heading matches rank first.
	function rank(entry: SearchEntry, words: string[], phrase: string): number | null {
		const page = entry.page.toLowerCase();
		const heading = entry.heading?.toLowerCase() ?? '';
		const text = entry.text.toLowerCase();
		let score = 0;
		for (const word of words) {
			if (heading.includes(word)) score += 4;
			else if (page.includes(word)) score += entry.heading ? 2 : 5;
			else if (text.includes(word)) score += 1;
			else return null;
		}
		if (heading.includes(phrase) || (!entry.heading && page.includes(phrase))) score += 4;
		if (text.includes(phrase)) score += 1;
		return score;
	}

	function snippet(text: string, words: string[]): string {
		const lower = text.toLowerCase();
		const at = Math.min(...words.map((word) => lower.indexOf(word)).filter((index) => index >= 0));
		if (!Number.isFinite(at) || at < SNIPPET / 3) {
			return text.length > SNIPPET ? `${text.slice(0, SNIPPET).replace(/\s\S*$/, '')}…` : text;
		}
		const start = text.lastIndexOf(' ', at - SNIPPET / 4) + 1;
		const end = start + SNIPPET;
		return `…${text.slice(start, end).replace(/\s\S*$/, '')}${end < text.length ? '…' : ''}`;
	}

	let words = $derived(query.trim().toLowerCase().split(/\s+/).filter(Boolean));

	let hits = $derived.by<Hit[]>(() => {
		if (words.length === 0) {
			return suggestions.map((entry) => ({ entry, score: 0, snippet: '' }));
		}
		const phrase = words.join(' ');
		const ranked = entries
			.map((entry) => ({ entry, score: rank(entry, words, phrase) }))
			.filter((hit): hit is { entry: SearchEntry; score: number } => hit.score !== null)
			.sort((a, b) => b.score - a.score);
		const perPage = new Map<string, number>();
		const kept: Hit[] = [];
		for (const hit of ranked) {
			const count = perPage.get(hit.entry.page + hit.entry.section) ?? 0;
			if (count >= PER_PAGE) continue;
			perPage.set(hit.entry.page + hit.entry.section, count + 1);
			kept.push({ ...hit, snippet: snippet(hit.entry.text, words) });
			if (kept.length === LIMIT) break;
		}
		return kept;
	});

	let groups = $derived.by<[string, Hit[]][]>(() => {
		const bySection = new Map<string, Hit[]>();
		for (const hit of hits) {
			const section = words.length === 0 ? 'Start here' : hit.entry.section;
			bySection.set(section, [...(bySection.get(section) ?? []), hit]);
		}
		return [...bySection.entries()];
	});

	function escape(word: string): string {
		return word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
	}

	function marked(text: string): { text: string; match: boolean }[] {
		if (words.length === 0) return [{ text, match: false }];
		const pattern = new RegExp(`(${words.map(escape).join('|')})`, 'gi');
		return text
			.split(pattern)
			.filter(Boolean)
			.map((part) => ({ text: part, match: words.includes(part.toLowerCase()) }));
	}
</script>

{#snippet highlighted(text: string)}
	{#each marked(text) as part, index (index)}
		{#if part.match}<mark class="rounded-sm bg-primary/15 text-base-content">{part.text}</mark
			>{:else}{part.text}{/if}
	{/each}
{/snippet}

{#snippet row(hit: Hit)}
	<Icon icon={hit.entry.heading ? icons.hash : icons.fileText} class="h-4 w-4 shrink-0 self-start mt-0.5 opacity-70" />
	<span class="min-w-0 flex-1">
		<span class="block truncate">
			{@render highlighted(hit.entry.page)}
			{#if hit.entry.heading}
				<span class="mr-1 text-base-content/50">›</span>{@render highlighted(hit.entry.heading)}
			{/if}
		</span>
		{#if hit.snippet}
			<span class="mt-0.5 line-clamp-2 text-xs leading-relaxed text-base-content/60">
				{@render highlighted(hit.snippet)}
			</span>
		{/if}
	</span>
{/snippet}

<SearchPalette
	bind:open
	bind:query
	{groups}
	{row}
	{loading}
	placeholder="Search the docs…"
	empty={failed ? 'Search is unavailable right now' : undefined}
	href={(hit) => `${base}${hit.entry.href}`}
	key={(hit) => hit.entry.href}
	onchoose={(hit) => goto(`${base}${hit.entry.href}`)}
/>
