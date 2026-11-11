<script lang="ts">
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import { tick, untrack } from 'svelte';
	import { navigation } from '$lib/nav';
	import type { SearchEntry } from '$lib/search';
	import { fileText, hash, search } from '$lib/icons';

	let { open = $bindable(false) }: { open: boolean } = $props();

	const LIMIT = 20;
	const PER_PAGE = 3;
	const SNIPPET = 160;

	let query = $state('');
	let active = $state(0);
	let input = $state<HTMLInputElement | null>(null);
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
		untrack(() => {
			query = '';
			active = 0;
			failed = false;
			load();
		});
		void tick().then(() => input?.focus());
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

	let groups = $derived.by(() => {
		const bySection = new Map<string, Hit[]>();
		for (const hit of hits) {
			bySection.set(hit.entry.section, [...(bySection.get(hit.entry.section) ?? []), hit]);
		}
		return [...bySection.entries()];
	});

	let flat = $derived(groups.flatMap(([, group]) => group));

	$effect(() => {
		if (active >= flat.length) active = Math.max(0, flat.length - 1);
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

	function close() {
		open = false;
	}

	function choose(hit: Hit | undefined) {
		if (!hit) return;
		close();
		goto(`${base}${hit.entry.href}`);
	}

	function onkeydown(event: KeyboardEvent) {
		if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
			event.preventDefault();
			if (flat.length === 0) return;
			const step = event.key === 'ArrowDown' ? 1 : -1;
			active = (active + step + flat.length) % flat.length;
			void tick().then(() =>
				document.getElementById(`search-result-${active}`)?.scrollIntoView({ block: 'nearest' })
			);
		} else if (event.key === 'Enter') {
			event.preventDefault();
			choose(flat[active]);
		} else if (event.key === 'Escape') {
			event.preventDefault();
			event.stopPropagation();
			close();
		}
	}
</script>

{#snippet highlighted(text: string)}
	{#each marked(text) as part, index (index)}
		{#if part.match}<mark class="rounded-sm bg-primary/15 text-base-content">{part.text}</mark
			>{:else}{part.text}{/if}
	{/each}
{/snippet}

{#if open}
	<div class="fixed inset-0 z-50 bg-black/30" aria-hidden="true" onclick={close}></div>
	<div
		class="fixed top-4 left-1/2 z-50 w-[calc(100%-2rem)] max-w-xl -translate-x-1/2 overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-xl md:top-24"
		role="dialog"
		aria-modal="true"
		aria-label="Search the docs"
	>
		<div class="flex items-center gap-2 border-b border-base-200 px-4">
			<Icon icon={search} class="h-4 w-4 shrink-0 opacity-50" />
			<input
				bind:this={input}
				bind:value={query}
				{onkeydown}
				oninput={() => (active = 0)}
				type="text"
				class="h-12 w-full bg-transparent text-sm outline-none"
				placeholder="Search the docs…"
				role="combobox"
				aria-expanded="true"
				aria-controls="search-results"
				aria-activedescendant={flat.length > 0 ? `search-result-${active}` : undefined}
				autocomplete="off"
				spellcheck="false"
			/>
			{#if loading}
				<span class="loading loading-xs loading-spinner text-primary"></span>
			{/if}
		</div>
		<ul id="search-results" class="max-h-[min(28rem,70dvh)] overflow-y-auto p-1" role="listbox">
			{#each groups as [section, group] (section)}
				<li role="presentation" class="px-3 pt-2 pb-1 text-xs font-medium text-base-content/50">
					{words.length === 0 ? 'Start here' : section}
				</li>
				{#each group as hit (hit.entry.href)}
					{@const index = flat.indexOf(hit)}
					<li role="presentation">
						<a
							id="search-result-{index}"
							href="{base}{hit.entry.href}"
							role="option"
							aria-selected={index === active}
							tabindex="-1"
							class="flex items-start gap-2.5 rounded-md px-3 py-2 text-sm {index === active
								? 'bg-base-200'
								: ''}"
							onmousemove={() => (active = index)}
							onclick={close}
						>
							<Icon
								icon={hit.entry.heading ? hash : fileText}
								class="mt-0.5 h-4 w-4 shrink-0 opacity-70"
							/>
							<span class="min-w-0 flex-1">
								<span class="block truncate">
									{@render highlighted(hit.entry.page)}
									{#if hit.entry.heading}
										<span class="mr-1 text-base-content/50">›</span>{@render highlighted(
											hit.entry.heading
										)}
									{/if}
								</span>
								{#if hit.snippet}
									<span class="mt-0.5 line-clamp-2 text-xs leading-relaxed text-base-content/60">
										{@render highlighted(hit.snippet)}
									</span>
								{/if}
							</span>
						</a>
					</li>
				{/each}
			{:else}
				<li class="px-3 py-6 text-center text-sm text-base-content/50">
					{failed ? 'Search is unavailable right now' : loading ? 'Loading…' : 'Nothing found'}
				</li>
			{/each}
		</ul>
	</div>
{/if}
