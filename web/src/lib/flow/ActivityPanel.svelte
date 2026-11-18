<script lang="ts">
	import Icon from '@iconify/svelte';
	import { IconButton, PaneResizer, StatusDot } from '@connve/ui';
	import { activityHeight } from '$lib/flow/activityHeight';
	import {
		activityLevels,
		getFlowActivityLimit,
		DEFAULT_ACTIVITY_LEVELS,
		resetActivityLevels,
		setFlowActivityLimit,
	} from '$lib/activityStore.svelte';
	import {
		clickedLevels,
		levelChip,
		levelChipClass,
		levelLabel,
		levelVariant
	} from '$lib/logRecord';
	import type { ActivityLevel } from '$lib/logRecord';
	import { LOGS_LIMIT_MAX, clampLogsLimit } from '$lib/logsLimit';
	import { formatRelative } from '$lib/time';
	import { VirtualRows } from '$lib/virtualRows.svelte';
	import { LOG_LINE, LOG_MESSAGE, quietLine, withCause } from '$lib/logLine';
	import { m } from '$lib/paraglide/messages.js';
	import type { Activity } from '$lib/flow/ActivityDetails.svelte';

	interface Props {
		activities: Activity[];
		expanded: boolean;
		onToggle: () => void;
		onRowClick: (taskName: string) => void;
		/** The row shown in the flow's inspector. */
		selected?: Activity | null;
		/** Takes the whole flow view, with the graph and config hidden. */
		maximized?: boolean;
		/** Fills the height it is given instead of its own resizable height. */
		fill?: boolean;
	}

	let {
		activities,
		expanded,
		onToggle,
		onRowClick,
		selected = $bindable(null),
		maximized = $bindable(false),
		fill = false
	}: Props = $props();

	let scroller = $state<HTMLElement | null>(null);
	let flowActivityLimit = $derived(getFlowActivityLimit());
	let countWidth = $derived(`${String(flowActivityLimit).length}ch`);

	// Free-text runs against task + message + task_type, and a task lock pins
	// to one node.
	const levelFilter = activityLevels;
	let search = $state('');
	let searchOpen = $state(false);
	let taskFilter = $state<string | null>(null);

	// Debounce the search input so typing at 10-events-per-frame doesn't
	// re-derive the whole filtered list on every keystroke.
	let searchDebounced = $state('');
	let searchTimer: ReturnType<typeof setTimeout> | null = null;
	$effect(() => {
		const term = search;
		if (searchTimer !== null) clearTimeout(searchTimer);
		searchTimer = setTimeout(() => (searchDebounced = term.trim().toLowerCase()), 150);
	});

	let counts = $derived.by(() => {
		const c: Record<ActivityLevel, number> = {
			info: 0,
			warn: 0,
			error: 0,
			debug: 0,
			trace: 0,
		};
		for (const a of activities) c[a.level] += 1;
		return c;
	});

	let latest = $derived(activities.at(-1));

	let anyFilterActive = $derived(
		!levelFilter.info ||
			!levelFilter.warn ||
			!levelFilter.error ||
			levelFilter.debug ||
			levelFilter.trace ||
			searchDebounced.length > 0 ||
			taskFilter !== null,
	);

	// Latest first — read direction "what happened just now" without scrolling.
	// Filters applied here so the virtualization math sees only the visible set.
	let ordered = $derived.by(() => {
		const out: Activity[] = [];
		const term = searchDebounced;
		for (let i = activities.length - 1; i >= 0; i--) {
			const a = activities[i];
			if (!levelFilter[a.level]) continue;
			if (taskFilter !== null && a.task !== taskFilter) continue;
			if (term.length > 0) {
				const hay =
					(a.task ?? '') + ' ' + (a.task_type ?? '') + ' ' + a.message;
				if (!hay.toLowerCase().includes(term)) continue;
			}
			out.push(a);
		}
		return out;
	});

	const rows = new VirtualRows(() => ordered, 28);

	function onScroll(e: Event) {
		rows.scrollTop = (e.currentTarget as HTMLElement).scrollTop;
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key !== 'Escape' || event.defaultPrevented || !maximized || selected) return;
		const target = event.target instanceof HTMLElement ? event.target : null;
		if (target?.isContentEditable || target?.closest('input, textarea, select, [role="dialog"]')) return;
		event.preventDefault();
		maximized = false;
	}

	// A click picks a level or drops it again; with none picked the defaults show.
	function clickLevel(level: ActivityLevel, e: MouseEvent) {
		e.stopPropagation();
		Object.assign(levelFilter, clickedLevels(levelFilter, DEFAULT_ACTIVITY_LEVELS, level));
	}

	function clearFilters() {
		resetActivityLevels();
		search = '';
		searchDebounced = '';
		taskFilter = null;
	}

	function applyLimit(next: number) {
		setFlowActivityLimit(clampLogsLimit(next));
	}

	// Search-and-scroll used by DAG node clicks; falls back to top when the
	// requested task hasn't emitted anything yet. Also locks the task filter
	// so subsequent rows narrow to just the clicked node.
	export function scrollToLatestFor(taskName: string) {
		taskFilter = taskName;
		if (!scroller) return;
		queueMicrotask(() => scroller?.scrollTo({ top: 0, behavior: 'smooth' }));
	}
</script>

<svelte:window onkeydown={onKeydown} />

<div
	class="@container relative flex flex-col bg-base-100 {fill && expanded
		? 'min-h-0 flex-1'
		: 'shrink-0'} {maximized ? '' : 'border-t border-base-200'}"
>
	{#if expanded && !fill}
		<PaneResizer pane={activityHeight} />
	{/if}
	<div class="flex h-12 items-center gap-2 pl-2 pr-3">
		<button
			type="button"
			class="flex min-w-0 flex-1 items-center gap-2 rounded-md px-2 py-1.5 text-left transition-colors outline-none hover:bg-base-200/60 focus-visible:bg-base-200/60"
			onclick={onToggle}
			aria-expanded={expanded}
		>
			<Icon
				icon={expanded ? 'tabler:chevron-down' : 'tabler:chevron-up'}
				class="h-4 w-4 shrink-0 opacity-60"
			/>
			<span class="flex items-baseline gap-1.5 text-[0.9375rem] font-semibold">
				<span>{m.activity_heading()}</span>
				<span class="text-xs font-normal opacity-50">· {m.logs_count({ count: ordered.length })}</span>
			</span>
			{#if !expanded}
				<span class="ml-2 flex items-center gap-3 text-xs">
					{#if counts.warn > 0}
						<StatusDot variant="warning" label={String(counts.warn)} />
					{/if}
					{#if counts.error > 0}
						<StatusDot variant="error" label={String(counts.error)} />
					{/if}
				</span>
				<span class="ml-auto truncate text-xs text-base-content/50">
					{#if latest}
						{formatRelative(latest.ts_ms)} · {latest.task ?? m.activity_flow_task()}
					{:else}
						{m.activity_empty_collapsed()}
					{/if}
				</span>
			{/if}
		</button>
		{#if expanded}
			{#if searchOpen || search}
				<label
					class="input input-sm flex w-40 items-center @lg:w-64 gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary"
				>
					<Icon icon="tabler:search" class="h-4 w-4 shrink-0 opacity-50" />
					<input
						type="text"
						placeholder={m.activity_search_placeholder()}
						aria-label={m.activity_search_placeholder()}
						bind:value={search}
						{@attach (node: HTMLInputElement) => node.focus()}
						onblur={() => (searchOpen = false)}
						onkeydown={(e) => {
							if (e.key !== 'Escape') return;
							e.preventDefault();
							search = '';
							searchOpen = false;
						}}
					/>
					{#if search}
						<button
							type="button"
							class="opacity-50 hover:opacity-100"
							aria-label={m.common_clear_search()}
							onclick={() => (search = '')}
						>
							<Icon icon="tabler:x" class="h-4 w-4" />
						</button>
					{/if}
				</label>
			{:else}
				<IconButton
					icon="tabler:search"
					label={m.activity_search_placeholder()}
					tooltip="left"
					onclick={() => (searchOpen = true)}
				/>
			{/if}
			<div class="dropdown dropdown-end {fill ? '' : 'dropdown-top'}">
				<button
					type="button"
					tabindex="0"
					class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content"
					aria-label={m.logs_more_actions()}
				>
					<Icon icon="tabler:dots" class="h-5 w-5" />
				</button>
				<div
					tabindex="-1"
					class="dropdown-content z-10 my-1 w-60 rounded-md border border-base-200 bg-base-100 p-1 text-sm shadow-lg"
				>
					<label class="flex h-9 items-center gap-2 rounded-md px-3">
						<span class="flex-1">{m.logs_limit_label()}</span>
						<input
							type="number"
							min="1"
							max={LOGS_LIMIT_MAX}
							step="1000"
							value={flowActivityLimit}
							onchange={(e) => applyLimit(e.currentTarget.valueAsNumber)}
							class="no-spinner w-16 rounded border border-base-300 bg-base-100 px-1.5 py-0.5 text-right tabular-nums outline-none focus:border-primary"
						/>
					</label>
				</div>
			</div>
			<IconButton
				icon={maximized ? 'tabler:arrows-minimize' : 'tabler:arrows-maximize'}
				label={maximized ? m.activity_restore() : m.activity_maximize()}
				tooltip="left"
				onclick={() => (maximized = !maximized)}
			/>
		{/if}
	</div>

	{#if expanded}
		<div class="flex flex-wrap items-center gap-2 px-4 pb-3">
			<span class="flex flex-wrap items-center gap-1">
				{#each ['info', 'warn', 'error', 'debug', 'trace'] as const as level (level)}
					{@const chip = levelChip(levelFilter, DEFAULT_ACTIVITY_LEVELS, level)}
					<button
						type="button"
						class="chip {levelChipClass(level, chip)}"
						aria-pressed={chip === 'picked'}
						onclick={(e) => clickLevel(level, e)}
					>
						<span>{levelLabel(level)}</span>
						{#if levelFilter[level]}
							<span
								class="inline-block text-left tabular-nums opacity-60"
								style="min-width: {countWidth}">{counts[level]}</span
							>
						{/if}
					</button>
				{/each}
			</span>

			{#if taskFilter}
				<span
					class="chip chip-neutral"
				>
					<span class="opacity-60">{m.activity_task_filter_prefix()}</span>
					<span class="font-mono">{taskFilter}</span>
					<button
						type="button"
						class="ml-0.5 opacity-60 hover:opacity-100"
						aria-label={m.activity_task_filter_clear()}
						onclick={() => (taskFilter = null)}
					>
						<Icon icon="tabler:x" class="h-3.5 w-3.5" />
					</button>
				</span>
			{/if}

			{#if anyFilterActive}
				<button
					type="button"
					class="btn btn-ghost btn-xs font-normal text-base-content/60"
					onclick={clearFilters}
				>
					{m.activity_filters_clear()}
				</button>
			{/if}
		</div>

		{#if ordered.length === 0}
			<div class="px-4 py-6 text-center text-xs opacity-50">
				{#if anyFilterActive}
					{m.activity_no_match()}
				{:else}
					{m.activity_empty()}
				{/if}
			</div>
		{:else}
			<div
				bind:this={scroller}
				class="overflow-x-hidden overflow-y-auto bg-base-100 pb-2 {fill ? 'min-h-0 flex-1' : ''}"
				style:height={fill ? undefined : `min(${activityHeight.size}px, 60vh)`}
				onscroll={onScroll}
				bind:clientHeight={rows.viewportHeight}
			>
				<div style:height="{rows.topPad}px"></div>
				{#each rows.visible as event (event)}
					<button
						type="button"
						{@attach rows.measure(event)}
						class="{LOG_LINE} w-full cursor-pointer px-4 text-left {selected === event
							? 'bg-base-200'
							: 'hover:bg-base-200/60'}"
						onclick={() => {
							selected = event;
							if (event.task) onRowClick(event.task);
						}}
					>
						<span class="flex h-4 items-center">
							<StatusDot variant={levelVariant(event.level)} label="" />
							<span class="sr-only">{levelLabel(event.level)}</span>
						</span>
						<span class="whitespace-nowrap text-base-content/50">{formatRelative(event.ts_ms)}</span>
						<span class="truncate font-mono text-base-content/70">{event.task ?? m.activity_flow_task()}</span>
						{#if event.message}
							<span class={LOG_MESSAGE}>{withCause(event.message, event.extra)}</span>
						{:else}
							<span class="{LOG_MESSAGE} text-base-content/50">
								{quietLine(event.task_type, event.duration_ms)}
							</span>
						{/if}
					</button>
				{/each}
				<div style:height="{rows.bottomPad}px"></div>
			</div>
		{/if}
	{/if}
</div>

