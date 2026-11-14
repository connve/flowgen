<script lang="ts">
	import { onDestroy, onMount, tick } from 'svelte';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import {
		CopyButton,
		IconButton,
		Inspector,
		InspectorSection,
		StateMessage,
		StatusDot
	} from '@connve/ui';
	import { encodePath, errorMessage, listLogs, type LogRecord } from '$lib/api';
	import { logsStream } from '$lib/urls';
	import { formatRelative } from '$lib/time';
	import { formatDuration } from '$lib/toolRuns.svelte';
	import { rafBatch } from '$lib/rafBatch';
	import { LOG_LINE, LOG_MESSAGE, quietLine, withCause } from '$lib/logLine';
	import { VirtualRows } from '$lib/virtualRows.svelte';
	import {
		clickedLevels,
		extractFieldSummary,
		extractSpanSummary,
		hasPickedLevels,
		levelChip,
		levelChipClass,
		levelLabel,
		levelVariant,
		nonHoistedSpans,
		timestampMs,
	} from '$lib/logRecord';
	import { LOGS_LIMIT_MAX, clampLogsLimit } from '$lib/logsLimit';
	import {
		getLogsLimit,
		LOGS_LEVELS_DEFAULT,
		logsLevels,
		setLogsLimit,
	} from '$lib/logsPageState.svelte';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';

	let logsLimit = $derived(getLogsLimit());

	type Level = LogRecord['level'];

	const LEVELS: Level[] = ['info', 'warn', 'error', 'debug', 'trace'];

	// Records get a monotonic ID at insert time so `#each` can key on a
	// stable value (indices shift when we trim the buffer, which would
	// force Svelte to destroy and recreate every row on every append).
	interface LogRow {
		id: number;
		record: LogRecord;
	}

	let nextId = 0;

	// Records arrive out of chronological order: the snapshot flattens
	// per-flow ring buffers in hash-map order, and the live tail interleaves
	// retries from many flows. Sort newest-first (id breaks ties and orders
	// records with no timestamp by arrival) so the freshest record is always
	// at the top; the `limit` trim then evicts the oldest from the tail.
	function rowOrder(a: LogRow, b: LogRow): number {
		const ta = timestampMs(a.record);
		const tb = timestampMs(b.record);
		if (ta !== null && tb !== null && ta !== tb) return tb - ta;
		return b.id - a.id;
	}


	let rows = $state<LogRow[]>([]);
	const levelFilter = logsLevels;
	let search = $state('');
	let searchOpen = $state(false);
	let live = $state(true);
	let atTop = $state(true);
	let selected = $state<LogRecord | null>(null);
	let scrollPane: HTMLDivElement | null = $state(null);
	let source: EventSource | null = null;

	// Line limit per level, defaulting to the Grafana/Loki-standard 1000. The
	// operator can change it in the toolbar; it caps both each level's
	// snapshot fetch and the in-browser buffer the live tail trims to.
	// Clamped to the backend ceiling (`MAX_QUERY_LIMIT`, 10000). Lives in
	// `logsPageState` (module-level), not local `$state`, so it survives
	// switching to another tab and back — only a full reload resets it.

	let levelCounts = $derived.by(() => {
		const c: Record<Level, number> = { info: 0, warn: 0, error: 0, debug: 0, trace: 0 };
		for (const row of rows) c[row.record.level] += 1;
		return c;
	});

	function selectedLevels(): Level[] {
		return (Object.keys(levelFilter) as Level[]).filter((level) => levelFilter[level]);
	}

	function trimPerLevel(list: LogRow[], limit: number): LogRow[] {
		const kept: Record<Level, number> = { info: 0, warn: 0, error: 0, debug: 0, trace: 0 };
		return list.filter((row) => kept[row.record.level]++ < limit);
	}

	let filtered = $derived(
		rows.filter((row) => {
			const r = row.record;
			if (search.trim().length === 0) return true;
			const needle = search.toLowerCase();
			if (r.body.toLowerCase().includes(needle)) return true;
			if (r.target.toLowerCase().includes(needle)) return true;
			for (const s of r.spans) {
				if (s.name.toLowerCase().includes(needle)) return true;
				for (const f of s.fields) if (f.value.toLowerCase().includes(needle)) return true;
			}
			for (const f of r.fields) if (f.value.toLowerCase().includes(needle)) return true;
			return false;
		}),
	);

	let filtering = $derived(search.trim().length > 0);
	let loadError = $state<string | null>(null);

	// Virtualized: live streaming would otherwise re-render every row on each append.
	const list = new VirtualRows(() => filtered, 28, 8);

	// A click picks a level or drops it again; with none picked the defaults show.
	function clickLevel(level: Level) {
		showLevels(clickedLevels(levelFilter, LOGS_LEVELS_DEFAULT, level));
	}

	function showLevels(next: Record<Level, boolean>) {
		const added = LEVELS.filter((key) => next[key] && !levelFilter[key]);
		const removed = LEVELS.filter((key) => !next[key] && levelFilter[key]);
		for (const key of LEVELS) levelFilter[key] = next[key];
		rows = rows.filter((row) => !removed.includes(row.record.level));
		if (selected && removed.includes(selected.level)) selected = null;
		void loadLevels(added, logsLimit);
		connectStream();
	}

	onMount(() => {
		void loadLevels(selectedLevels(), logsLimit);
		connectStream();
	});

	onDestroy(() => {
		source?.close();
	});

	const levelRequests: Record<Level, number> = { info: 0, warn: 0, error: 0, debug: 0, trace: 0 };

	async function loadLevels(levels: Level[], limit: number) {
		await Promise.all(levels.map((level) => loadLevel(level, limit)));
		await tick();
		scrollToTop();
	}

	async function loadLevel(level: Level, limit: number) {
		const request = ++levelRequests[level];
		try {
			const { data: body } = await listLogs({ query: { limit, levels: [level] } });
			loadError = null;
			if (request !== levelRequests[level] || !levelFilter[level]) return;
			const fresh = body.map((record) => ({ id: nextId++, record }));
			const newest = Math.max(-Infinity, ...body.map((record) => timestampMs(record) ?? -Infinity));
			const kept = rows.filter(
				(row) =>
					row.record.level !== level || (timestampMs(row.record) ?? -Infinity) > newest,
			);
			rows = trimPerLevel([...kept, ...fresh].sort(rowOrder), limit);
		} catch (err) {
			loadError = errorMessage(err, m.logs_load_failed_title());
		}
	}

	// Reloads the snapshot at a new line limit, clamped to the backend
	// ceiling. Trims the in-browser buffer immediately so lowering the
	// limit takes effect without waiting for the next tail append.
	function applyLimit(next: number) {
		const clamped = clampLogsLimit(next);
		if (clamped === logsLimit) return;
		setLogsLimit(clamped);
		// Use `clamped`, not the `logsLimit` derived binding: Svelte doesn't
		// re-run $derived synchronously within this same function call, so
		// reading it here would still see the pre-update value.
		rows = trimPerLevel(rows, clamped);
		void loadLevels(selectedLevels(), clamped);
	}

	// Coalesce SSE frames into one state mutation per animation frame. A
	// restart burst pushes thousands of records/second; appending + scrolling
	// per frame instead of per record keeps the event loop responsive.
	const flushLogs = rafBatch<LogRecord>((batch) => {
		for (const record of batch) {
			if (levelFilter[record.level]) rows.push({ id: nextId++, record });
		}
		rows.sort(rowOrder);
		rows = trimPerLevel(rows, logsLimit);
		if (live && atTop) {
			void tick().then(scrollToTop);
		}
	});

	function connectStream() {
		source?.close();
		source = null;
		const levels = selectedLevels();
		if (levels.length === 0) return;
		source = new EventSource(logsStream({ levels }));
		source.addEventListener('log', (ev) => {
			try {
				flushLogs(JSON.parse(ev.data) as LogRecord);
			} catch {
				// drop malformed frames
			}
		});
	}

	function scrollToTop() {
		if (!scrollPane) return;
		scrollPane.scrollTop = 0;
		list.scrollTop = 0;
	}

	function onScroll() {
		if (!scrollPane) return;
		list.scrollTop = scrollPane.scrollTop;
		atTop = scrollPane.scrollTop < 40;
	}

	function resume() {
		live = true;
		atTop = true;
		scrollToTop();
	}

	function clearView() {
		rows = [];
		selected = null;
	}

	function formatAbsolute(ts: string | null | undefined): string {
		if (!ts) return '—';
		try {
			const d = new Date(ts);
			return d.toISOString().replace('T', ' ').replace('Z', ' UTC');
		} catch {
			return ts;
		}
	}

	function buildClipboard(record: LogRecord): string {
		const lines: string[] = [];
		lines.push(`[${record.level.toUpperCase()}] ${formatAbsolute(record.timestamp)}`);
		lines.push(`target: ${record.target}`);
		lines.push(`message: ${record.body}`);
		if (record.spans.length > 0) {
			lines.push('spans:');
			for (const s of record.spans) {
				const fields = s.fields.map((f) => `${f.key}=${f.value}`).join(' ');
				lines.push(`  ${s.name}${fields ? ` ${fields}` : ''}`);
			}
		}
		if (record.fields.length > 0) {
			lines.push('fields:');
			for (const f of record.fields) {
				lines.push(`  ${f.key}=${f.value}`);
			}
		}
		return lines.join('\n');
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape' && selected) selected = null;
	}

	let selectedSummary = $derived(selected ? extractSpanSummary(selected) : null);
	let selectedFieldSummary = $derived(selected ? extractFieldSummary(selected) : null);
	let selectedTsMs = $derived(selected ? timestampMs(selected) : null);
	let selectedDurationMs = $derived(selectedSummary?.duration_ms ?? null);
	let selectedEventId = $derived(selectedFieldSummary?.event_id ?? null);
	let selectedExtraFields = $derived(selectedFieldSummary?.extra ?? []);
	let selectedExtraSpans = $derived(selected ? nonHoistedSpans(selected) : []);

</script>

<svelte:head>
	<title>{pageTitle(m.monitor_tab_logs(), m.nav_monitor())}</title>
</svelte:head>

<div class="flex min-h-0 min-w-0 flex-1">
<section class="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
	<div class="shrink-0 space-y-3 bg-base-100 px-4 pb-4 pt-6 md:px-6">
		<div class="flex min-h-8 items-center justify-between gap-3">
			<div class="flex items-baseline gap-1.5 text-[0.9375rem] font-semibold">
				<span>{m.logs_all()}</span>
				<span class="text-xs font-normal opacity-50">· {m.logs_count({ count: filtered.length })}</span>
			</div>
			<div class="flex items-center gap-1">
				{#if searchOpen || search}
					<label
						class="input input-sm flex w-64 items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary"
					>
						<Icon icon="tabler:search" class="h-4 w-4 shrink-0 opacity-50" />
						<input
							type="text"
							placeholder={m.logs_search_placeholder()}
							aria-label={m.logs_search_placeholder()}
							bind:value={search}
							{@attach (node: HTMLInputElement) => node.focus()}
							onblur={() => (searchOpen = false)}
							onkeydown={(e) => {
								if (e.key !== 'Escape') return;
								e.stopPropagation();
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
						label={m.logs_search_placeholder()}
						onclick={() => (searchOpen = true)}
					/>
				{/if}
				{#if !atTop && live}
					<IconButton icon="tabler:arrow-up" label={m.logs_jump_to_latest()} onclick={resume} />
				{/if}
				<div class="tooltip tooltip-bottom" data-tip={live ? m.logs_tail_pause() : m.logs_tail_resume()}>
					<button
						type="button"
						class="flex h-8 items-center gap-2 rounded-md px-2.5 text-sm transition-colors hover:bg-base-200 {live
							? 'text-base-content'
							: 'text-base-content/50'}"
						aria-pressed={live}
						onclick={() => (live = !live)}
					>
						<span
							class="h-2 w-2 rounded-full {live ? 'animate-pulse bg-primary' : 'bg-base-content/30'}"
							aria-hidden="true"
						></span>
						{live ? m.logs_live() : m.logs_paused()}
					</button>
				</div>
				<div class="dropdown dropdown-end">
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
						class="dropdown-content z-10 mt-1 w-60 space-y-1 rounded-md border border-base-200 bg-base-100 p-1 text-sm shadow-lg"
					>
						<label class="flex h-9 items-center gap-2 rounded-md px-3">
							<span class="flex-1">{m.logs_limit_label()}</span>
							<input
								type="number"
								min="1"
								max={LOGS_LIMIT_MAX}
								step="1000"
								value={logsLimit}
								onchange={(e) => applyLimit(e.currentTarget.valueAsNumber)}
								class="no-spinner w-16 rounded border border-base-300 bg-base-100 px-1.5 py-0.5 text-right tabular-nums outline-none focus:border-primary"
							/>
						</label>
						<button
							type="button"
							class="flex h-9 w-full items-center rounded-md px-3 text-left transition-colors hover:bg-base-200"
							onclick={clearView}
						>
							{m.logs_clear_view()}
						</button>
					</div>
				</div>
			</div>
		</div>
		<div class="flex flex-wrap items-center gap-2">
			<div class="flex flex-wrap items-center gap-1">
				{#each LEVELS as key (key)}
					{@const chip = levelChip(levelFilter, LOGS_LEVELS_DEFAULT, key)}
					<button
						type="button"
						class="chip {levelChipClass(key, chip)}"
						aria-pressed={chip === 'picked'}
						onclick={() => clickLevel(key)}
					>
						<span>{levelLabel(key)}</span>
						{#if levelFilter[key]}
							<span
								class="inline-block text-left tabular-nums opacity-60"
								style="min-width: {String(logsLimit).length}ch">{levelCounts[key]}</span
							>
						{/if}
					</button>
				{/each}
				{#if hasPickedLevels(levelFilter, LOGS_LEVELS_DEFAULT)}
					<button
						type="button"
						class="btn btn-ghost btn-xs font-normal text-base-content/60"
						onclick={() => showLevels({ ...LOGS_LEVELS_DEFAULT })}
					>
						{m.flows_tag_filters_clear()}
					</button>
				{/if}
			</div>
		</div>
	</div>

	<div class="flex min-h-0 flex-1 flex-col px-4 pb-6 md:px-6">
		{#if filtered.length === 0 && loadError}
			<StateMessage tone="oops" title={m.logs_load_failed_title()} message={loadError} />
		{:else if filtered.length === 0 && filtering}
			<StateMessage tone="notice" title={m.logs_no_match_title()} message={m.logs_no_match_message()} />
		{:else if filtered.length === 0}
			<StateMessage tone="notice" title={m.logs_empty_title()} message={m.logs_empty_message()} />
		{:else}
			<div
				class="@container flex min-h-0 flex-1 flex-col overflow-hidden bg-base-100"
			>
				<div
					bind:this={scrollPane}
					bind:clientHeight={list.viewportHeight}
					onscroll={onScroll}
					class="min-h-0 flex-1 overflow-y-auto overflow-x-hidden"
				>
					<div style:height="{list.topPad}px"></div>
					{#each list.visible as row (row.id)}
						{@const record = row.record}
						{@const tsMs = timestampMs(record)}
						{@const summary = extractSpanSummary(record)}
						<button
							type="button"
							{@attach list.measure(row)}
							class="{LOG_LINE} w-full px-3 text-left focus:outline-none focus-visible:bg-base-200 {selected ===
							record
								? 'bg-base-200'
								: 'hover:bg-base-200/60'}"
							onclick={() => (selected = record)}
						>
							<span class="flex h-4 items-center">
								<StatusDot variant={levelVariant(record.level)} label="" />
								<span class="sr-only">{levelLabel(record.level)}</span>
							</span>
							<span class="whitespace-nowrap text-base-content/50">
								{tsMs !== null ? formatRelative(tsMs) : '—'}
							</span>
							<span class="truncate font-mono text-base-content/70">
								{summary.task ?? record.target}
							</span>
							{#if record.body}
								<span class={LOG_MESSAGE}>
									{withCause(record.body, extractFieldSummary(record).extra)}
								</span>
							{:else}
								<span class="{LOG_MESSAGE} text-base-content/50">
									{quietLine(summary.task_type, summary.duration_ms)}
								</span>
							{/if}
						</button>
					{/each}
					<div style:height="{list.bottomPad}px"></div>
				</div>
			</div>
		{/if}
	</div>
</section>

{#if selected}
	{@const record = selected}
	<Inspector
		title={selectedSummary?.task ?? selected.target}
		label={m.logs_detail_label()}
		onclose={() => (selected = null)}
	>
		{#snippet actions()}
			<CopyButton text={buildClipboard(record)} label={m.logs_copy_record()} tooltip="bottom" />
		{/snippet}
		{#snippet subtitle()}
			<StatusDot variant={levelVariant(record.level)} label={levelLabel(record.level)} />
		{/snippet}
		<div class="space-y-3">
			<dl class="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1">
				<dt class="text-base-content/50">{m.logs_column_timestamp()}</dt>
				<dd class="tabular-nums">{formatAbsolute(selected.timestamp)}</dd>
				{#if selectedTsMs !== null}
					<dt class="text-base-content/50">{m.logs_column_when()}</dt>
					<dd>{formatRelative(selectedTsMs)}</dd>
				{/if}
				{#if selectedSummary?.task_type}
					<dt class="text-base-content/50">{m.activity_field_processor()}</dt>
					<dd class="[overflow-wrap:anywhere]">{selectedSummary.task_type}</dd>
				{/if}
				{#if selectedDurationMs !== null}
					<dt class="text-base-content/50">{m.activity_field_duration()}</dt>
					<dd class="tabular-nums">{formatDuration(selectedDurationMs)}</dd>
				{/if}
				{#if selectedEventId}
					<dt class="text-base-content/50">{m.logs_detail_event()}</dt>
					<dd class="[overflow-wrap:anywhere]">{selectedEventId}</dd>
				{/if}
				{#if selectedSummary?.flow}
					<dt class="text-base-content/50">{m.logs_detail_flow()}</dt>
					<dd class="min-w-0 break-words">
						<a href="{base}/flows/{encodePath(selectedSummary.flow)}" class="text-primary hover:underline">
							{selectedSummary.flow}
						</a>
					</dd>
				{/if}
				<dt class="text-base-content/50">{m.logs_column_target()}</dt>
				<dd class="[overflow-wrap:anywhere]">{selected.target}</dd>
			</dl>
		</div>

		<InspectorSection title={m.logs_column_message()}>
			<p class="whitespace-pre-wrap break-words {selected.body ? '' : 'text-base-content/40'}">
				{selected.body || '—'}
			</p>
		</InspectorSection>

		{#if selectedExtraSpans.length > 0}
			<InspectorSection title={m.logs_detail_spans()}>
				<ul class="space-y-3">
					{#each selectedExtraSpans as span (span.name)}
						<li>
							<div class="font-medium">{span.name}</div>
							<dl class="mt-1 grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1">
								{#each span.fields as f (f.key)}
									<dt class="text-base-content/50">{f.key}</dt>
									<dd class="whitespace-pre-wrap [overflow-wrap:anywhere]">{f.value}</dd>
								{/each}
							</dl>
						</li>
					{/each}
				</ul>
			</InspectorSection>
		{/if}

		{#if selectedExtraFields.length > 0}
			<InspectorSection title={m.logs_detail_attributes()}>
				<dl class="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1">
					{#each selectedExtraFields as [key, value] (key)}
						<dt class="text-base-content/50">{key}</dt>
						<dd class="max-h-64 overflow-auto whitespace-pre-wrap [overflow-wrap:anywhere]">{value}</dd>
					{/each}
				</dl>
			</InspectorSection>
		{/if}
	</Inspector>
{/if}
</div>

<svelte:window on:keydown={onKeydown} />
