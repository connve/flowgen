<script lang="ts">
	import { onDestroy, onMount, tick } from 'svelte';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import { apiUrl, type LogRecord } from '$lib/api';
	import { formatRelative } from '$lib/time';
	import { rafBatch } from '$lib/rafBatch';
	import {
		extractFieldSummary,
		extractSpanSummary,
		nonHoistedSpans,
		timestampMs,
	} from '$lib/logRecord';

	type Level = LogRecord['level'];

	// Records get a monotonic ID at insert time so `#each` can key on a
	// stable value (indices shift when we trim the buffer, which would
	// force Svelte to destroy and recreate every row on every append).
	interface LogRow {
		id: number;
		record: LogRecord;
	}

	let nextId = 0;

	function encodePath(path: string): string {
		return path.split('/').map(encodeURIComponent).join('/');
	}

	let rows = $state<LogRow[]>([]);
	// Default filters: warn + error only. Info/debug/trace hidden until the
	// operator explicitly widens the filter — matches how the Flows page
	// leans on error/warning counters over ambient info noise.
	let levelFilter = $state<Record<Level, boolean>>({
		info: false,
		warn: true,
		error: true,
		debug: false,
		trace: false,
	});
	let search = $state('');
	let live = $state(true);
	let atBottom = $state(true);
	let selected = $state<LogRecord | null>(null);
	let scrollPane: HTMLDivElement | null = $state(null);
	let source: EventSource | null = null;

	// Line limit, defaulting to the Grafana/Loki-standard 1000. The operator
	// can change it in the toolbar; it caps both the initial snapshot fetch
	// and the in-browser buffer the live tail trims to. Clamped to the
	// backend ceiling (`LOGS_SNAPSHOT_MAX_LIMIT`, 10000).
	const LIMIT_MAX = 10000;
	let limit = $state(1000);

	// Virtualized list: render only the rows in view plus a small overscan
	// buffer, with top/bottom spacers standing in for the rest. Without this,
	// live streaming re-renders all 1000 rows on every append and locks up.
	const ROW_HEIGHT = 28;
	const OVERSCAN = 8;
	let scrollTop = $state(0);
	let viewportHeight = $state(600);

	let levelCounts = $derived.by(() => {
		const c: Record<Level, number> = { info: 0, warn: 0, error: 0, debug: 0, trace: 0 };
		for (const row of rows) c[row.record.level] += 1;
		return c;
	});

	let filtered = $derived(
		rows.filter((row) => {
			const r = row.record;
			if (!levelFilter[r.level]) return false;
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

	let startIdx = $derived(Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN));
	let endIdx = $derived(
		Math.min(filtered.length, Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + OVERSCAN),
	);
	let visible = $derived(filtered.slice(startIdx, endIdx));
	let topPad = $derived(startIdx * ROW_HEIGHT);
	let bottomPad = $derived(Math.max(0, (filtered.length - endIdx) * ROW_HEIGHT));

	function toggleLevel(level: Level) {
		levelFilter = { ...levelFilter, [level]: !levelFilter[level] };
	}

	onMount(() => {
		void loadHistory();
		connectStream();
	});

	onDestroy(() => {
		source?.close();
	});

	async function loadHistory() {
		try {
			const res = await fetch(apiUrl(`api/logs?limit=${limit}`));
			if (!res.ok) return;
			const body = (await res.json()) as LogRecord[];
			rows = body.map((record) => ({ id: nextId++, record }));
			await tick();
			scrollToBottom();
		} catch {
			// history fetch is best-effort; the SSE stream backfills.
		}
	}

	// Reloads the snapshot at a new line limit, clamped to the backend
	// ceiling. Trims the in-browser buffer immediately so lowering the
	// limit takes effect without waiting for the next tail append.
	function applyLimit(next: number) {
		const clamped = Math.min(Math.max(1, Math.floor(next)), LIMIT_MAX);
		if (clamped === limit) return;
		limit = clamped;
		if (rows.length > limit) rows.splice(0, rows.length - limit);
		void loadHistory();
	}

	// Coalesce SSE frames into one state mutation per animation frame. A
	// restart burst pushes thousands of records/second; appending + scrolling
	// per frame instead of per record keeps the event loop responsive.
	const flushLogs = rafBatch<LogRecord>((batch) => {
		for (const record of batch) {
			rows.push({ id: nextId++, record });
		}
		if (rows.length > limit) {
			rows.splice(0, rows.length - limit);
		}
		if (live && atBottom) {
			void tick().then(scrollToBottom);
		}
	});

	function connectStream() {
		source?.close();
		source = new EventSource(apiUrl('api/logs/stream'));
		source.addEventListener('log', (ev) => {
			try {
				flushLogs(JSON.parse(ev.data) as LogRecord);
			} catch {
				// drop malformed frames
			}
		});
	}

	function scrollToBottom() {
		if (!scrollPane) return;
		scrollPane.scrollTop = scrollPane.scrollHeight;
		scrollTop = scrollPane.scrollTop;
	}

	function onScroll() {
		if (!scrollPane) return;
		scrollTop = scrollPane.scrollTop;
		const gap = scrollPane.scrollHeight - scrollPane.scrollTop - scrollPane.clientHeight;
		atBottom = gap < 40;
	}

	function resume() {
		live = true;
		atBottom = true;
		scrollToBottom();
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

	function formatDuration(ms: number | null): string {
		if (ms === null) return '—';
		if (ms < 1000) return `${ms}ms`;
		if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
		return `${(ms / 60_000).toFixed(1)}m`;
	}

	function levelClasses(level: Level, active: boolean): string {
		if (!active) return 'chip chip-inactive';
		switch (level) {
			case 'error':
				return 'chip chip-error';
			case 'warn':
				return 'chip chip-warn';
			case 'info':
				return 'chip chip-info';
			default:
				return 'chip chip-neutral';
		}
	}

	function levelBadgeColor(level: Level): string {
		switch (level) {
			case 'error':
				return 'text-error';
			case 'warn':
				return 'text-warning';
			case 'debug':
			case 'trace':
				return 'text-base-content/50';
			default:
				return 'text-primary';
		}
	}

	function levelLabel(level: Level): string {
		switch (level) {
			case 'warn':
				return 'Warn';
			case 'error':
				return 'Error';
			case 'debug':
				return 'Debug';
			case 'trace':
				return 'Trace';
			default:
				return 'Info';
		}
	}
</script>

<section class="flex h-[calc(100vh-4rem)] min-w-0 flex-col overflow-hidden">
	<div class="shrink-0 border-b border-base-200 bg-base-100 px-6 pb-4 pt-6">
		<div class="flex flex-wrap items-center gap-2">
			<div class="flex items-center gap-1.5 text-sm">
				<span>All logs</span>
				<span class="text-xs opacity-50">· {filtered.length} of {rows.length}</span>
			</div>

			<div class="flex items-center gap-1">
				{#each [
					{ key: 'info' as Level, label: 'Info' },
					{ key: 'warn' as Level, label: 'Warn' },
					{ key: 'error' as Level, label: 'Error' },
					{ key: 'debug' as Level, label: 'Debug' },
					{ key: 'trace' as Level, label: 'Trace' },
				] as { key, label } (key)}
					<button
						type="button"
						class={levelClasses(key, levelFilter[key])}
						aria-pressed={levelFilter[key]}
						onclick={() => toggleLevel(key)}
					>
						<span>{label}</span>
						<span class="tabular-nums opacity-60">{levelCounts[key]}</span>
					</button>
				{/each}
			</div>

			<label
				class="input input-sm flex items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary"
			>
				<span class="text-base-content/50">Limit</span>
				<input
					type="number"
					min="1"
					max={LIMIT_MAX}
					step="1000"
					value={limit}
					onchange={(e) => applyLimit(e.currentTarget.valueAsNumber)}
					class="no-spinner w-16 tabular-nums"
					aria-label="Line limit"
				/>
			</label>

			{#if !atBottom && live}
				<div class="tooltip tooltip-bottom" data-tip="Jump to latest">
					<button type="button" class="btn btn-sm btn-ghost gap-1" onclick={resume}>
						<Icon icon="tabler:arrow-down" class="h-4 w-4" />
						Resume
					</button>
				</div>
			{/if}
			<div class="tooltip tooltip-bottom" data-tip={live ? 'Pause live tail' : 'Resume live tail'}>
				<button type="button" class="btn btn-sm btn-ghost gap-1" onclick={() => (live = !live)}>
					<Icon icon={live ? 'tabler:player-pause' : 'tabler:player-play'} class="h-4 w-4" />
					{live ? 'Live' : 'Paused'}
				</button>
			</div>
			<button type="button" class="btn btn-sm btn-ghost gap-1" onclick={clearView}>
				<Icon icon="tabler:x" class="h-4 w-4" />
				Clear
			</button>

			<div class="flex-1"></div>

			<label
				class="input input-sm flex items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary"
			>
				<svg
					class="h-4 w-4 opacity-50"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<circle cx="11" cy="11" r="8" />
					<path d="M21 21l-4.35-4.35" />
				</svg>
				<input type="text" placeholder="Search logs..." bind:value={search} />
				{#if search}
					<button
						type="button"
						class="opacity-50 hover:opacity-100"
						aria-label="Clear search"
						onclick={() => (search = '')}
					>
						<Icon icon="tabler:x" class="h-6 w-6" />
					</button>
				{/if}
			</label>
		</div>
	</div>

	{#if filtered.length === 0}
		<div class="flex flex-1 items-center justify-center text-sm text-base-content/50">
			No log records match the current filters.
		</div>
	{:else}
		<!-- Column headers rendered outside the virtualized scroll area so they
		     stay visible without a sticky header fighting the spacer math. -->
		<div
			class="grid shrink-0 items-center border-b border-base-300 bg-base-100 px-6 py-1.5 text-xs uppercase tracking-wide text-base-content/60"
			style="grid-template-columns: 6rem 5rem 7rem 18rem minmax(0, 1fr); gap: 0.75rem"
		>
			<span>Status</span>
			<span>When</span>
			<span>Timestamp</span>
			<span>Target</span>
			<span>Message</span>
		</div>
		<div
			bind:this={scrollPane}
			bind:clientHeight={viewportHeight}
			onscroll={onScroll}
			class="min-h-0 flex-1 overflow-y-auto overflow-x-hidden bg-base-100 text-xs"
		>
			<div style="height: {topPad}px"></div>
			{#each visible as row (row.id)}
				{@const record = row.record}
				{@const isSelected = selected === record}
				{@const tsMs = timestampMs(record)}
				<button
					type="button"
					class="grid w-full items-center px-6 text-left transition-colors hover:bg-base-200 focus:outline-none focus-visible:bg-base-200 {isSelected
						? 'bg-base-200'
						: ''}"
					style="grid-template-columns: 6rem 5rem 7rem 18rem minmax(0, 1fr); gap: 0.75rem; height: {ROW_HEIGHT}px"
					onclick={() => (selected = record)}
				>
					<span class="flex items-center gap-1.5">
						<span
							class="inline-flex h-3 w-3 items-center justify-center rounded-full text-white"
							class:bg-primary={record.level === 'info'}
							class:bg-warning={record.level === 'warn'}
							class:bg-error={record.level === 'error'}
							class:bg-base-300={record.level === 'debug' || record.level === 'trace'}
						>
							{#if record.level === 'info'}
								<Icon icon="tabler:check" class="h-2.5 w-2.5" />
							{:else if record.level === 'warn'}
								<Icon icon="tabler:exclamation-mark" class="h-2.5 w-2.5" />
							{:else if record.level === 'error'}
								<Icon icon="tabler:x" class="h-2.5 w-2.5" />
							{/if}
						</span>
						<span class="uppercase opacity-70">{levelLabel(record.level)}</span>
					</span>
					<span class="whitespace-nowrap opacity-60" title={formatAbsolute(record.timestamp)}>
						{tsMs !== null ? formatRelative(tsMs) : '—'}
					</span>
					<span class="whitespace-nowrap font-mono opacity-60">
						{formatTs(record.timestamp) || '—'}
					</span>
					<span class="truncate font-mono text-base-content/70">
						{record.target}
					</span>
					<span class="truncate font-mono" title={record.body}>{record.body}</span>
				</button>
			{/each}
			<div style="height: {bottomPad}px"></div>
		</div>
	{/if}
</section>

<svelte:window on:keydown={onKeydown} />

{#if selected}
	<div
		class="fixed inset-y-0 right-0 z-40 flex w-full max-w-2xl flex-col border-l border-base-300 bg-base-100 shadow-2xl ring-1 ring-base-content/10"
		role="dialog"
		aria-modal="true"
		aria-label="Log record detail"
	>
		<div class="flex items-center justify-between border-b border-base-300 px-4 py-2">
			<div class="flex items-center gap-2 text-xs">
				<span
					class="inline-flex h-3 w-3 items-center justify-center rounded-full text-white"
					class:bg-primary={selected.level === 'info'}
					class:bg-warning={selected.level === 'warn'}
					class:bg-error={selected.level === 'error'}
					class:bg-base-300={selected.level === 'debug' || selected.level === 'trace'}
				>
					{#if selected.level === 'info'}
						<Icon icon="tabler:check" class="h-2.5 w-2.5" />
					{:else if selected.level === 'warn'}
						<Icon icon="tabler:exclamation-mark" class="h-2.5 w-2.5" />
					{:else if selected.level === 'error'}
						<Icon icon="tabler:x" class="h-2.5 w-2.5" />
					{/if}
				</span>
				<span class="uppercase opacity-70">{levelLabel(selected.level)}</span>
				{#if selectedSummary?.task_type}
					<span class="opacity-40">•</span>
					<span class="font-mono opacity-70">{selectedSummary.task_type}</span>
				{/if}
				{#if selectedSummary?.task}
					<span class="opacity-40">•</span>
					<span class="font-mono opacity-80">{selectedSummary.task}</span>
				{:else if !selectedSummary?.task_type}
					<span class="opacity-40">•</span>
					<span class="font-mono opacity-80">{selected.target}</span>
				{/if}
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

		<div class="flex-1 overflow-y-auto">
			<dl class="grid grid-cols-[6rem_1fr] gap-x-4 gap-y-1 px-4 py-2 text-xs">
				<dt class="opacity-50">Timestamp</dt>
				<dd class="font-mono">{formatAbsolute(selected.timestamp)}</dd>
				{#if selectedTsMs !== null}
					<dt class="opacity-50">When</dt>
					<dd>{formatRelative(selectedTsMs)}</dd>
				{/if}
				{#if selectedDurationMs !== null}
					<dt class="opacity-50">Duration</dt>
					<dd class="font-mono tabular-nums">{formatDuration(selectedDurationMs)}</dd>
				{/if}
				{#if selectedEventId}
					<dt class="opacity-50">Event</dt>
					<dd class="font-mono">{selectedEventId}</dd>
				{/if}
				{#if selectedSummary?.flow}
					<dt class="opacity-50">Flow</dt>
					<dd class="min-w-0">
						<a
							href="{base}/flows/{encodePath(selectedSummary.flow)}"
							class="font-mono text-primary hover:underline"
							title="Open flow"
						>
							{selectedSummary.flow}
						</a>
					</dd>
				{/if}
				<dt class="opacity-50">Target</dt>
				<dd class="font-mono">{selected.target}</dd>
			</dl>

			<div class="border-t border-base-300 px-4 py-2">
				<div class="pb-1 text-xs opacity-50">Message</div>
				<pre class="max-h-64 overflow-auto whitespace-pre-wrap break-words font-mono text-xs {selected.body
						? 'opacity-80'
						: 'opacity-40'}">{selected.body || '—'}</pre>
			</div>

			{#if selectedExtraSpans.length > 0}
				<div class="border-t border-base-300 px-4 py-2">
					<div class="pb-1 text-xs opacity-50">Spans</div>
					<ul class="space-y-1 text-xs">
						{#each selectedExtraSpans as span (span.name)}
							<li>
								<div class="font-mono font-semibold">{span.name}</div>
								<dl class="ml-4 mt-0.5 grid grid-cols-[10rem_minmax(0,1fr)] gap-x-4 gap-y-0.5">
									{#each span.fields as f (f.key)}
										<dt class="truncate font-mono opacity-70" title={f.key}>{f.key}</dt>
										<dd class="min-w-0 font-mono">
											<pre class="whitespace-pre-wrap break-words">{f.value}</pre>
										</dd>
									{/each}
								</dl>
							</li>
						{/each}
					</ul>
				</div>
			{/if}

			{#if selectedExtraFields.length > 0}
				<div class="border-t border-base-300 px-4 py-2">
					<div class="pb-1 text-xs opacity-50">Attributes</div>
					<dl class="grid grid-cols-[10rem_minmax(0,1fr)] gap-x-4 gap-y-1 text-xs">
						{#each selectedExtraFields as [key, value] (key)}
							<dt class="truncate font-mono opacity-70" title={key}>{key}</dt>
							<dd class="min-w-0">
								<pre class="max-h-64 overflow-auto whitespace-pre-wrap break-all font-mono opacity-80">{value}</pre>
							</dd>
						{/each}
					</dl>
				</div>
			{/if}
		</div>
	</div>
{/if}
