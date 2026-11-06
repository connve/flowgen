<script lang="ts">
	import { onDestroy, onMount, tick } from 'svelte';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import StatusDot from '$lib/StatusDot.svelte';
	import Inspector from '$lib/Inspector.svelte';
	import InspectorSection from '$lib/InspectorSection.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { apiUrl, encodePath, errorMessage, failure, type LogRecord } from '$lib/api';
	import { formatRelative } from '$lib/time';
	import { formatDuration } from '$lib/toolRuns.svelte';
	import { rafBatch } from '$lib/rafBatch';
	import {
		extractFieldSummary,
		extractSpanSummary,
		levelChipClass,
		levelLabel,
		levelVariant,
		nonHoistedSpans,
		timestampMs,
	} from '$lib/logRecord';
	import { LOGS_LIMIT_MAX, clampLogsLimit } from '$lib/logsLimit';
	import { getLogsLimit, logsLevels, setLogsLimit } from '$lib/logsPageState.svelte';
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

	// Virtualized list: render only the rows in view plus a small overscan
	// buffer, with top/bottom spacers standing in for the rest. Without this,
	// live streaming re-renders all 1000 rows on every append and locks up.
	const ROW_HEIGHT = 28;
	/** Timestamp and target fold away when the list is narrow, e.g. beside the inspector. */
	const COLUMNS =
		'grid-cols-[5rem_5rem_minmax(0,1fr)] @3xl:grid-cols-[5rem_5rem_7rem_18rem_minmax(0,1fr)]';
	const OVERSCAN = 8;
	let scrollTop = $state(0);
	let viewportHeight = $state(600);

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

	let startIdx = $derived(Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN));
	let endIdx = $derived(
		Math.min(filtered.length, Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + OVERSCAN),
	);
	let visible = $derived(filtered.slice(startIdx, endIdx));
	let topPad = $derived(startIdx * ROW_HEIGHT);
	let bottomPad = $derived(Math.max(0, (filtered.length - endIdx) * ROW_HEIGHT));

	function toggleLevel(level: Level) {
		levelFilter[level] = !levelFilter[level];
		if (levelFilter[level]) {
			void loadLevels([level], logsLimit);
		} else {
			rows = rows.filter((row) => row.record.level !== level);
			if (selected?.level === level) selected = null;
		}
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
			const res = await fetch(apiUrl(`api/logs?limit=${limit}&levels=${level}`));
			if (!res.ok) throw await failure(res);
			const body = (await res.json()) as LogRecord[];
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
		const query = levels.map((level) => `levels=${level}`).join('&');
		source = new EventSource(apiUrl(`api/logs/stream?${query}`));
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
		scrollTop = 0;
	}

	function onScroll() {
		if (!scrollPane) return;
		scrollTop = scrollPane.scrollTop;
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

	function formatTs(ts: string | null | undefined): string {
		if (!ts) return '';
		try {
			const d = new Date(ts);
			return d.toISOString().split('T')[1].slice(0, 12);
		} catch {
			return ts;
		}
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
	<div class="shrink-0 space-y-3 bg-base-100 px-6 pb-4 pt-6">
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
					<div class="tooltip tooltip-bottom" data-tip={m.logs_search_placeholder()}>
						<button
							type="button"
							class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content"
							aria-label={m.logs_search_placeholder()}
							onclick={() => (searchOpen = true)}
						>
							<Icon icon="tabler:search" class="h-5 w-5" />
						</button>
					</div>
				{/if}
				{#if !atTop && live}
					<div class="tooltip tooltip-bottom" data-tip={m.logs_jump_to_latest()}>
						<button
							type="button"
							class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content"
							aria-label={m.logs_jump_to_latest()}
							onclick={resume}
						>
							<Icon icon="tabler:arrow-up" class="h-5 w-5" />
						</button>
					</div>
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
							class="h-2 w-2 rounded-full {live ? 'animate-pulse bg-success' : 'bg-base-content/30'}"
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
			<div class="flex items-center gap-1">
				{#each LEVELS as key (key)}
					<button
						type="button"
						class="chip {levelChipClass(key, levelFilter[key])}"
						aria-pressed={levelFilter[key]}
						onclick={() => toggleLevel(key)}
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
			</div>

		</div>
	</div>

	<div class="flex min-h-0 flex-1 flex-col px-6 pb-6">
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
				<!-- Column headers rendered outside the virtualized scroll area so they
				     stay visible without a sticky header fighting the spacer math. -->
				<div
					class="grid shrink-0 items-center gap-3 border-b border-base-200 px-3 py-2 text-xs font-medium text-base-content/50 {COLUMNS}"
				>
					<span>{m.logs_column_level()}</span>
					<span>{m.logs_column_when()}</span>
					<span class="@max-3xl:hidden">{m.logs_column_timestamp()}</span>
					<span class="@max-3xl:hidden">{m.logs_column_target()}</span>
					<span>{m.logs_column_message()}</span>
				</div>
				<div
					bind:this={scrollPane}
					bind:clientHeight={viewportHeight}
					onscroll={onScroll}
					class="min-h-0 flex-1 overflow-y-auto overflow-x-hidden text-xs"
				>
					<div style="height: {topPad}px"></div>
					{#each visible as row (row.id)}
						{@const record = row.record}
						{@const isSelected = selected === record}
						{@const tsMs = timestampMs(record)}
						<button
							type="button"
							class="grid w-full items-center gap-3 border-b border-base-200 px-3 text-left transition-colors hover:bg-base-200 focus:outline-none focus-visible:bg-base-200 {COLUMNS} {isSelected
								? 'bg-base-200'
								: ''}"
							style:height="{ROW_HEIGHT}px"
							onclick={() => (selected = record)}
						>
							<span><StatusDot variant={levelVariant(record.level)} label={levelLabel(record.level)} /></span>
							<span class="whitespace-nowrap opacity-70">
								{tsMs !== null ? formatRelative(tsMs) : '—'}
							</span>
							<span class="whitespace-nowrap font-mono opacity-70 @max-3xl:hidden">
								{formatTs(record.timestamp) || '—'}
							</span>
							<span class="truncate font-mono opacity-70 @max-3xl:hidden">
								{record.target}
							</span>
							<span class="truncate font-mono">{record.body}</span>
						</button>
					{/each}
					<div style="height: {bottomPad}px"></div>
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
			{#if selectedSummary?.task_type}
				<span class="truncate">· {selectedSummary.task_type}</span>
			{/if}
			{#if selectedDurationMs !== null}
				<span class="tabular-nums">· {formatDuration(selectedDurationMs)}</span>
			{/if}
		{/snippet}
		<div class="space-y-3">
			<dl class="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1">
				<dt class="text-base-content/50">{m.logs_column_timestamp()}</dt>
				<dd class="tabular-nums">{formatAbsolute(selected.timestamp)}</dd>
				{#if selectedTsMs !== null}
					<dt class="text-base-content/50">{m.logs_column_when()}</dt>
					<dd>{formatRelative(selectedTsMs)}</dd>
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
