<script lang="ts">
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import { tick, untrack } from 'svelte';
	import { slide } from 'svelte/transition';
	import { NavList, type NavGroup, type NavLink } from '@connve/ui';
	import { navigation, type NavItem, type NavSection } from '$lib/nav';
	import { icons } from '@connve/ui';

	interface Props {
		/** The current route without the base path. */
		route: string;
		onnavigate?: () => void;
	}

	let { route, onnavigate }: Props = $props();

	const GUIDES = new Set(['Getting Started', 'Concepts']);
	const guides = navigation.filter((section) => GUIDES.has(section.title));
	const tasks = navigation.filter((section) => !GUIDES.has(section.title));

	const HEADING =
		'mb-1 flex items-center gap-1.5 px-2 text-xs font-semibold tracking-wider text-base-content/60 uppercase';
	const ROW = 'block truncate rounded-md px-2 py-1.5 text-sm transition-colors hover:bg-base-200';
	const ROW_ACTIVE = 'bg-base-200 font-medium text-base-content';
	const ROW_IDLE = 'text-base-content/70 hover:text-base-content';

	function link(item: NavItem): NavLink {
		return { href: `${base}${item.href}`, label: item.title, active: item.href === route };
	}

	function holds(section: NavSection): boolean {
		return [...section.items, ...(section.subsections ?? []).flatMap((sub) => sub.items)].some(
			(item) => item.href === route
		);
	}

	let groups = $derived<NavGroup[]>(
		guides.map((section) => ({ title: section.title, links: section.items.map(link) }))
	);

	let current = $derived(tasks.find(holds)?.title);
	let toggled = $state<Record<string, boolean>>({});

	$effect(() => {
		const title = current;
		if (title) untrack(() => (toggled[title] = true));
	});

	function isOpen(title: string): boolean {
		return toggled[title] ?? title === current;
	}

	let root = $state<HTMLElement | null>(null);

	function scrollerOf(element: HTMLElement | null): HTMLElement | null {
		for (let parent = element?.parentElement; parent; parent = parent.parentElement) {
			if (getComputedStyle(parent).overflowY === 'auto') return parent;
		}
		return null;
	}

	// Scrolls the list's container so the current page shows.
	function reveal() {
		const scroller = scrollerOf(root);
		const link = root?.querySelector<HTMLElement>('[aria-current="page"]');
		if (!scroller || !link) return;
		const top = link.getBoundingClientRect().top - scroller.getBoundingClientRect().top;
		if (top < 0 || top > scroller.clientHeight - link.offsetHeight) {
			scroller.scrollTop += top - scroller.clientHeight / 3;
		}
	}

	$effect(() => {
		void route;
		void tick().then(reveal);
	});

	function slug(title: string): string {
		return `nav-${title.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
	}
</script>

{#snippet links(items: NavItem[])}
	<ul class="space-y-0.5">
		{#each items.map(link) as item (item.href)}
			<li>
				<a
					href={item.href}
					aria-current={item.active ? 'page' : undefined}
					class={[ROW, item.active ? ROW_ACTIVE : ROW_IDLE]}
					onclick={onnavigate}
				>
					{item.label}
				</a>
			</li>
		{/each}
	</ul>
{/snippet}

<nav bind:this={root} aria-label="Documentation" class="space-y-5">
	<NavList {groups} {onnavigate} />

	<section>
		<h2 class={HEADING}>Tasks</h2>
		<ul class="space-y-0.5">
			{#each tasks as section (section.title)}
				{@const open = isOpen(section.title)}
				<li>
					<button
						type="button"
						class={[
							'flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-sm transition-colors hover:bg-base-200',
							section.title === current ? 'font-medium text-base-content' : ROW_IDLE
						]}
						aria-expanded={open}
						aria-controls={slug(section.title)}
						onclick={() => (toggled[section.title] = !open)}
					>
						{#if section.icon}
							<img src="{base}{section.icon}" alt="" class="h-4 w-4 shrink-0" />
						{/if}
						<span class="truncate">{section.title}</span>
						<Icon
							icon={icons.chevronRight}
							class="ml-auto h-4 w-4 shrink-0 opacity-50 transition-transform motion-reduce:transition-none {open
								? 'rotate-90'
								: ''}"
						/>
					</button>
					{#if open}
						<div
							id={slug(section.title)}
							class="mt-0.5 mb-1 ml-4 border-l border-base-200 pl-2"
							transition:slide={{ duration: 150 }}
						>
							{@render links(section.items)}
							{#each section.subsections ?? [] as sub (sub.title)}
								<h3
									class="mt-2 mb-1 px-2 text-[10px] font-medium tracking-wider text-base-content/60 uppercase"
								>
									{sub.title}
								</h3>
								{@render links(sub.items)}
							{/each}
						</div>
					{/if}
				</li>
			{/each}
		</ul>
	</section>
</nav>
