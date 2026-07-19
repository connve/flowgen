<script lang="ts">
	import Icon from '@iconify/svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import { formatAbsolute, formatRelative } from '$lib/time';

	interface Activity {
		flow: string;
		task: string | null;
		task_type: string | null;
		level: 'info' | 'warning' | 'error';
		ts_ms: number;
		message: string;
		duration_ms?: number;
		event_id?: string;
	}

	function formatDuration(ms: number | undefined): string {
		if (ms === undefined) return '—';
		if (ms < 1000) return `${ms}ms`;
		if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
		return `${Math.floor(ms / 60_000)}m`;
	}

	interface Props {
		activities: Activity[];
		expanded: boolean;
		onToggle: () => void;
		onRowClick: (taskName: string) => void;
	}

	let { activities, expanded, onToggle, onRowClick }: Props = $props();

	// Fixed row height keeps virtualization math trivial; content is single-line
	// truncated so no row varies. Details for wrapped/full view live in the drawer.
	const ROW_HEIGHT = 32;
	const OVERSCAN = 6;

	let scroller = $state<HTMLElement | null>(null);
	let scrollTop = $state(0);
	let viewportHeight = $state(256);
	let selected = $state<Activity | null>(null);

	// Filter state — level chips act as toggles (all-on by default), free-text
	// runs against task + message + task_type, and a task lock pins to one node.
	let levelFilter = $state<Record<Activity['level'], boolean>>({
		info: true,
		warning: true,
		error: true,
	});
	let search = $state('');
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
		const c = { info: 0, warning: 0, error: 0 };
		for (const a of activities) c[a.level] += 1;
		return c;
	});

	let latest = $derived(activities.at(-1));

	let anyFilterActive = $derived(
		!levelFilter.info ||
			!levelFilter.warning ||
			!levelFilter.error ||
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

	let startIdx = $derived(Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN));
	let endIdx = $derived(
		Math.min(ordered.length, Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + OVERSCAN),
	);
	let visible = $derived(ordered.slice(startIdx, endIdx));
	let topPad = $derived(startIdx * ROW_HEIGHT);
	let bottomPad = $derived(Math.max(0, (ordered.length - endIdx) * ROW_HEIGHT));

	function onScroll(e: Event) {
		const el = e.currentTarget as HTMLElement;
		scrollTop = el.scrollTop;
	}

	function toggleLevel(level: Activity['level'], e: MouseEvent) {
		e.stopPropagation();
		levelFilter = { ...levelFilter, [level]: !levelFilter[level] };
	}

	function clearFilters() {
		levelFilter = { info: true, warning: true, error: true };
		search = '';
		searchDebounced = '';
		taskFilter = null;
	}

	// Serialize the selected activity to a JSON payload. Copies the full
	// record — timestamp, level, flow/task/processor, message, event id,
	// and every extra attribute — so ops can paste one blob into
	// tickets, chat, or grep without stitching fields together by hand.
	function buildClipboard(a: Activity): string {
		const payload: Record<string, unknown> = {
			timestamp: new Date(a.ts_ms).toISOString(),
			level: a.level,
			flow: a.flow,
			task: a.task,
			processor: a.task_type,
			message: a.message,
		};
		if (a.duration_ms !== undefined) payload.duration_ms = a.duration_ms;
		if (a.event_id) payload.event_id = a.event_id;
		if (a.extra && a.extra.length > 0) {
			payload.attributes = Object.fromEntries(a.extra);
		}
		return JSON.stringify(payload, null, 2);
	}

	// Search-and-scroll used by DAG node clicks; falls back to top when the
	// requested task hasn't emitted anything yet. Also locks the task filter
	// so subsequent rows narrow to just the clicked node.
	export function scrollToLatestFor(taskName: string) {
		taskFilter = taskName;
		if (!scroller) return;
		queueMicrotask(() => scroller?.scrollTo({ top: 0, behavior: 'smooth' }));
	}

	// Esc closes the drawer first, then bubbles on the next press. Capture
	// phase + stopPropagation so parent modal's Esc handler doesn't fire on
	// the same keypress and close both at once.
	function onWindowKeydown(e: KeyboardEvent) {
		if (e.key === 'Escape' && selected) {
			selected = null;
			e.stopPropagation();
		}
	}
</script>

<svelte:window onkeydowncapture={onWindowKeydown} />

<div class="flex shrink-0 flex-col border-t border-base-200 bg-base-100">
	<button
		type="button"
		class="flex h-10 items-center gap-4 border-b border-base-200 bg-base-200/50 px-4 text-left transition-colors hover:bg-base-200 {expanded
			? ''
			: 'border-b-transparent'}"
		onclick={onToggle}
		aria-expanded={expanded}
	>
		<span class="flex items-center gap-2">
			<Icon
				icon={expanded ? 'tabler:chevron-down' : 'tabler:chevron-up'}
				class="h-4 w-4 opacity-60"
			/>
			<span class="text-xs font-semibold uppercase tracking-wide opacity-70">Activity</span>
		</span>
		{#if !expanded}
			<span class="h-4 w-px bg-base-300"></span>
			<span class="flex items-center gap-3 text-xs">
				<span class="flex items-center gap-1.5">
					<span class="inline-flex h-3.5 w-3.5 items-center justify-center rounded-full bg-primary text-white">
						<Icon icon="tabler:check" class="h-2.5 w-2.5" />
					</span>
					<span class="tabular-nums">{counts.info}</span>
				</span>
				<span class="flex items-center gap-1.5">
					<span class="inline-flex h-3.5 w-3.5 items-center justify-center rounded-full bg-warning text-white">
						<Icon icon="tabler:exclamation-mark" class="h-2.5 w-2.5" />
					</span>
					<span class="tabular-nums">{counts.warning}</span>
				</span>
				<span class="flex items-center gap-1.5">
					<span class="inline-flex h-3.5 w-3.5 items-center justify-center rounded-full bg-error text-white">
						<Icon icon="tabler:x" class="h-2.5 w-2.5" />
					</span>
					<span class="tabular-nums">{counts.error}</span>
				</span>
			</span>
		{/if}
		{#if latest}
			<span class="ml-auto flex items-center gap-2 text-xs opacity-60">
				<span>{formatRelative(latest.ts_ms)}</span>
				<span class="opacity-40">•</span>
				<span class="font-mono">{latest.task_type ?? '—'}</span>
				<span class="opacity-40">•</span>
				<span class="font-mono">{latest.task ?? 'flow'}</span>
			</span>
		{:else}
			<span class="ml-auto text-xs opacity-40">No events yet</span>
		{/if}
	</button>

	{#if expanded}
		<!-- Filter row: level toggles, search, task-lock chip. Sits between the
		     header and the column head so filters visually own the list below. -->
		<div class="flex h-10 items-center gap-3 border-b border-base-200 bg-base-100 px-4">
			<span class="flex items-center gap-1">
				<button
					type="button"
					class="flex h-6 items-center gap-1.5 rounded-full border px-2 text-xs transition-colors {levelFilter.info
						? 'border-primary/50 bg-primary/10'
						: 'border-base-300 opacity-40 hover:opacity-70'}"
					aria-pressed={levelFilter.info}
					onclick={(e) => toggleLevel('info', e)}
				>
					<span class="inline-flex h-3 w-3 items-center justify-center rounded-full bg-primary text-white">
						<Icon icon="tabler:check" class="h-2.5 w-2.5" />
					</span>
					<span class="tabular-nums">{counts.info}</span>
				</button>
				<button
					type="button"
					class="flex h-6 items-center gap-1.5 rounded-full border px-2 text-xs transition-colors {levelFilter.warning
						? 'border-warning/50 bg-warning/10'
						: 'border-base-300 opacity-40 hover:opacity-70'}"
					aria-pressed={levelFilter.warning}
					onclick={(e) => toggleLevel('warning', e)}
				>
					<span class="inline-flex h-3 w-3 items-center justify-center rounded-full bg-warning text-white">
						<Icon icon="tabler:exclamation-mark" class="h-2.5 w-2.5" />
					</span>
					<span class="tabular-nums">{counts.warning}</span>
				</button>
				<button
					type="button"
					class="flex h-6 items-center gap-1.5 rounded-full border px-2 text-xs transition-colors {levelFilter.error
						? 'border-error/50 bg-error/10'
						: 'border-base-300 opacity-40 hover:opacity-70'}"
					aria-pressed={levelFilter.error}
					onclick={(e) => toggleLevel('error', e)}
				>
					<span class="inline-flex h-3 w-3 items-center justify-center rounded-full bg-error text-white">
						<Icon icon="tabler:x" class="h-2.5 w-2.5" />
					</span>
					<span class="tabular-nums">{counts.error}</span>
				</button>
			</span>

			<label
				class="input input-xs flex flex-1 items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary"
			>
				<Icon icon="tabler:search" class="h-4 w-4 opacity-50" />
				<input type="text" placeholder="Search task, processor, message..." bind:value={search} />
				{#if search}
					<button
						type="button"
						class="opacity-50 hover:opacity-100"
						aria-label="Clear search"
						onclick={() => (search = '')}
					>
						<Icon icon="tabler:x" class="h-4 w-4" />
					</button>
				{/if}
			</label>

			{#if taskFilter}
				<span
					class="flex h-6 items-center gap-1 rounded-full border border-primary/50 bg-primary/10 px-2 text-xs"
				>
					<span class="opacity-60">task:</span>
					<span class="font-mono">{taskFilter}</span>
					<button
						type="button"
						class="ml-0.5 opacity-60 hover:opacity-100"
						aria-label="Clear task filter"
						onclick={() => (taskFilter = null)}
					>
						<Icon icon="tabler:x" class="h-3.5 w-3.5" />
					</button>
				</span>
			{/if}

			{#if anyFilterActive}
				<button
					type="button"
					class="text-xs opacity-60 hover:opacity-100"
					onclick={clearFilters}
				>
					Clear
				</button>
			{/if}
		</div>

		{#if ordered.length === 0}
			<div class="px-4 py-6 text-center text-xs opacity-50">
				{#if anyFilterActive}
					No events match the current filters
				{:else}
					No activity recorded
				{/if}
			</div>
		{:else}
			<!-- Column headers rendered outside the virtualized list so they stay
			     visible without an intra-list sticky header (which fights virtualization). -->
			<div
				class="grid h-8 items-center border-b border-base-200 bg-base-100 px-2 text-xs uppercase tracking-wide opacity-60"
				style="grid-template-columns: 5.5rem 5rem 11rem 9rem 9rem 5rem 9rem 1fr; gap: 0.75rem"
			>
				<span>Status</span>
				<span>When</span>
				<span>Timestamp</span>
				<span>Processor</span>
				<span>Task</span>
				<span>Duration</span>
				<span>Event</span>
				<span>Message</span>
			</div>
			<div
				bind:this={scroller}
				class="max-h-64 overflow-auto bg-base-100"
				onscroll={onScroll}
				bind:clientHeight={viewportHeight}
			>
				<!-- Top spacer holds the un-rendered rows above the viewport -->
				<div style="height: {topPad}px"></div>
				{#each visible as event, i (event.ts_ms + '-' + (event.task ?? '_') + '-' + (startIdx + i))}
					<button
						type="button"
						class="grid w-full cursor-pointer items-center px-2 text-left text-xs hover:bg-base-200"
						style="grid-template-columns: 5.5rem 5rem 11rem 9rem 9rem 5rem 9rem 1fr; gap: 0.75rem; height: {ROW_HEIGHT}px"
						onclick={() => {
							selected = event;
							if (event.task) onRowClick(event.task);
						}}
					>
						<span class="flex items-center gap-1.5">
							<span
								class="inline-flex h-3 w-3 items-center justify-center rounded-full text-white"
								class:bg-primary={event.level === 'info'}
								class:bg-warning={event.level === 'warning'}
								class:bg-error={event.level === 'error'}
							>
								{#if event.level === 'info'}
									<Icon icon="tabler:check" class="h-2.5 w-2.5" />
								{:else if event.level === 'warning'}
									<Icon icon="tabler:exclamation-mark" class="h-2.5 w-2.5" />
								{:else}
									<Icon icon="tabler:x" class="h-2.5 w-2.5" />
								{/if}
							</span>
							<span class="uppercase opacity-70">{event.level}</span>
						</span>
						<span class="whitespace-nowrap opacity-60" title={formatAbsolute(event.ts_ms)}>
							{formatRelative(event.ts_ms)}
						</span>
						<span class="whitespace-nowrap font-mono opacity-60">{formatAbsolute(event.ts_ms)}</span>
						<span class="truncate font-mono opacity-70">{event.task_type ?? '—'}</span>
						<span class="truncate font-mono opacity-80">{event.task ?? '_flow'}</span>
						<span class="whitespace-nowrap font-mono tabular-nums opacity-60">
							{formatDuration(event.duration_ms)}
						</span>
						<span class="truncate font-mono opacity-60" title={event.event_id ?? ''}>
							{event.event_id ?? '—'}
						</span>
						<span class="truncate font-mono opacity-70" title={event.message}>{event.message}</span>
					</button>
				{/each}
				<div style="height: {bottomPad}px"></div>
			</div>
		{/if}
	{/if}
</div>

{#if selected}
	<div
		class="fixed inset-y-0 right-0 z-40 flex w-full max-w-xl flex-col border-l border-base-200 bg-base-100 shadow-xl"
		role="dialog"
		aria-modal="true"
		aria-label="Activity detail"
	>
		<div class="flex items-center justify-between border-b border-base-200 px-4 py-2">
			<div class="flex items-center gap-2 text-xs">
				<span
					class="inline-flex h-3 w-3 items-center justify-center rounded-full text-white"
					class:bg-primary={selected.level === 'info'}
					class:bg-warning={selected.level === 'warning'}
					class:bg-error={selected.level === 'error'}
				>
					{#if selected.level === 'info'}
						<Icon icon="tabler:check" class="h-2.5 w-2.5" />
					{:else if selected.level === 'warning'}
						<Icon icon="tabler:exclamation-mark" class="h-2.5 w-2.5" />
					{:else}
						<Icon icon="tabler:x" class="h-2.5 w-2.5" />
					{/if}
				</span>
				<span class="uppercase opacity-70">{selected.level}</span>
				<span class="opacity-40">•</span>
				<span class="font-mono opacity-70">{selected.task_type ?? '—'}</span>
				<span class="opacity-40">•</span>
				<span class="font-mono opacity-80">{selected.task ?? '_flow'}</span>
			</div>
			<div class="flex items-center gap-1">
				<CopyButton text={buildClipboard(selected)} label="Copy log" />
				<div class="tooltip tooltip-left" data-tip="Close">
					<button
						type="button"
						class="btn btn-ghost btn-sm btn-circle"
						aria-label="Close"
						onclick={() => (selected = null)}
					>
						<Icon icon="tabler:x" class="h-6 w-6" />
					</button>
				</div>
			</div>
		</div>
		<dl class="grid grid-cols-[6rem_1fr] gap-x-4 gap-y-1 px-4 py-2 text-xs">
			<dt class="opacity-50">Timestamp</dt>
			<dd class="font-mono">{formatAbsolute(selected.ts_ms)}</dd>
			<dt class="opacity-50">When</dt>
			<dd>{formatRelative(selected.ts_ms)}</dd>
			<dt class="opacity-50">Duration</dt>
			<dd class="font-mono tabular-nums">{formatDuration(selected.duration_ms)}</dd>
			<dt class="opacity-50">Event</dt>
			<dd class="font-mono">{selected.event_id ?? '—'}</dd>
		</dl>
		<div class="border-t border-base-200 px-4 py-2">
			<div class="pb-1 text-xs opacity-50">Message</div>
			<pre class="max-h-64 overflow-auto whitespace-pre-wrap break-words font-mono text-xs {selected.message
					? 'opacity-80'
					: 'opacity-40'}">{selected.message || '—'}</pre>
		</div>
		{#if selected.extra && selected.extra.length > 0}
			<div class="border-t border-base-200 px-4 py-2">
				<div class="pb-1 text-xs opacity-50">Attributes</div>
				<dl class="grid grid-cols-[10rem_1fr] gap-x-4 gap-y-1 text-xs">
					{#each selected.extra as [key, value] (key)}
						<dt class="truncate font-mono opacity-70" title={key}>{key}</dt>
						<dd class="whitespace-pre-wrap break-words font-mono opacity-80">{value}</dd>
					{/each}
				</dl>
			</div>
		{/if}
	</div>
{/if}
