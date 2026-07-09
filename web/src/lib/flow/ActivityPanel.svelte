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
	}

	interface Props {
		activities: Activity[];
		expanded: boolean;
		onToggle: () => void;
		onRowClick: (taskName: string) => void;
	}

	let { activities, expanded, onToggle, onRowClick }: Props = $props();

	let scroller = $state<HTMLElement | null>(null);
	let expandedRows = $state<Set<string>>(new Set());

	// Latest first — read direction "what happened just now" without scrolling.
	let ordered = $derived([...activities].reverse());

	function rowKey(event: Activity, index: number): string {
		return event.ts_ms + '-' + (event.task ?? '_flow') + '-' + index;
	}

	function toggleRow(key: string, e: MouseEvent) {
		e.stopPropagation();
		const next = new Set(expandedRows);
		if (next.has(key)) next.delete(key);
		else next.add(key);
		expandedRows = next;
	}

	let counts = $derived.by(() => {
		const c = { info: 0, warning: 0, error: 0 };
		for (const a of activities) c[a.level] += 1;
		return c;
	});

	let latest = $derived(activities.at(-1));

	export function scrollToLatestFor(taskName: string) {
		if (!scroller) return;
		const row = scroller.querySelector<HTMLElement>(
			`[data-row-task="${CSS.escape(taskName)}"]`
		);
		if (!row) return;
		row.scrollIntoView({ behavior: 'smooth', block: 'center' });
		row.classList.remove('row-highlight');
		void row.offsetWidth;
		row.classList.add('row-highlight');
	}
</script>

<div class="flex shrink-0 flex-col border-t border-base-200 bg-base-100">
	<button
		type="button"
		class="flex items-center gap-3 px-4 py-2 text-left hover:bg-base-200"
		onclick={onToggle}
		aria-expanded={expanded}
	>
		<Icon
			icon={expanded ? 'tabler:chevron-down' : 'tabler:chevron-up'}
			class="h-4 w-4 opacity-60"
		/>
		<span class="text-xs font-medium opacity-70">Activity</span>
		<span class="flex items-center gap-3 text-xs">
			<span class="flex items-center gap-1">
				<span class="level-pill inline-flex h-3 w-3 items-center justify-center rounded-full bg-primary text-white">
					<Icon icon="tabler:check" class="h-2.5 w-2.5" />
				</span>
				{counts.info}
			</span>
			<span class="flex items-center gap-1">
				<span class="level-pill inline-flex h-3 w-3 items-center justify-center rounded-full bg-warning text-white">
					<Icon icon="tabler:exclamation-mark" class="h-2.5 w-2.5" />
				</span>
				{counts.warning}
			</span>
			<span class="flex items-center gap-1">
				<span class="level-pill inline-flex h-3 w-3 items-center justify-center rounded-full bg-error text-white">
					<Icon icon="tabler:x" class="h-2.5 w-2.5" />
				</span>
				{counts.error}
			</span>
		</span>
		{#if latest}
			<span class="ml-auto text-xs opacity-60">
				{formatRelative(latest.ts_ms)} • {latest.task_type ?? '—'} • {latest.task ?? 'flow'}
			</span>
		{:else}
			<span class="ml-auto text-xs opacity-40">No events yet</span>
		{/if}
	</button>

	{#if expanded}
		<div bind:this={scroller} class="max-h-64 overflow-auto border-t border-base-200 bg-base-100">
			{#if ordered.length === 0}
				<div class="px-4 py-6 text-center text-xs opacity-50">No activity recorded</div>
			{:else}
				<table class="table table-xs">
					<thead class="sticky top-0 z-10 bg-base-100 text-xs uppercase tracking-wide opacity-60">
						<tr>
							<th class="w-8"></th>
							<th class="w-24">Status</th>
							<th class="w-24">When</th>
							<th class="w-44">Timestamp</th>
							<th class="w-40">Processor</th>
							<th class="w-40">Task</th>
							<th>Message</th>
						</tr>
					</thead>
					<tbody>
						{#each ordered as event, i (rowKey(event, i))}
							{@const key = rowKey(event, i)}
							{@const isOpen = expandedRows.has(key)}
							<tr
								data-row-task={event.task ?? '_flow'}
								class="cursor-pointer hover:bg-base-300"
								onclick={() => event.task && onRowClick(event.task)}
							>
								<td class="w-8">
									<button
										type="button"
										class="btn btn-ghost btn-xs btn-circle"
										aria-label={isOpen ? 'Collapse message' : 'Expand message'}
										aria-expanded={isOpen}
										onclick={(e) => toggleRow(key, e)}
									>
										<Icon
											icon={isOpen ? 'tabler:chevron-down' : 'tabler:chevron-right'}
											class="h-4 w-4 opacity-60"
										/>
									</button>
								</td>
								<td class="w-24">
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
								</td>
								<td class="w-24 whitespace-nowrap opacity-60" title={formatAbsolute(event.ts_ms)}>
									{formatRelative(event.ts_ms)}
								</td>
								<td class="w-44 whitespace-nowrap font-mono text-xs opacity-60">
									{formatAbsolute(event.ts_ms)}
								</td>
								<td class="w-40 font-mono text-xs opacity-70">{event.task_type ?? '—'}</td>
								<td class="w-40 font-mono opacity-80">{event.task ?? '_flow'}</td>
								<td class="max-w-0 truncate font-mono text-xs opacity-70" title={event.message}>
									{#if !isOpen}{event.message}{/if}
								</td>
							</tr>
							{#if isOpen}
								<tr>
									<td colspan="7" class="align-top">
										<div class="flex items-start gap-2 pl-8 pr-2">
											<pre class="flex-1 whitespace-pre-wrap break-words font-mono text-xs opacity-80">{event.message}</pre>
											<CopyButton text={event.message} size="xs" label="Copy message" />
										</div>
									</td>
								</tr>
							{/if}
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
	{/if}
</div>

<style>
	:global(.row-highlight) {
		animation: row-flash 1.4s ease-out forwards;
	}
	@keyframes row-flash {
		0% {
			background: rgba(0, 118, 0, 0.25);
		}
		100% {
			background: transparent;
		}
	}
</style>
