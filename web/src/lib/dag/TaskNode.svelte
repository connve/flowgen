<script lang="ts">
	import Icon from '@iconify/svelte';
	import { Handle, Position, type NodeProps } from '@xyflow/svelte';
	import { connectorFor } from '$lib/connectors';
	import { formatDuration } from '$lib/toolRuns.svelte';
	import { getNodeStates } from './nodeStates';
	import { CIRCLE } from './controls';
	import type { Mark } from './parseFlow';
	import { markLabel } from '$lib/changePreview';
	import { m } from '$lib/paraglide/messages.js';

	interface TaskNodeData extends Record<string, unknown> {
		name: string;
		taskType: string;
		mark?: Mark;
		onadd?: (after: string) => void;
		onremove?: (taskName: string) => void;
	}

	let { data }: NodeProps & { data: TaskNodeData } = $props();

	const nodeStates = getNodeStates();
	let connector = $derived(connectorFor(data.taskType));
	let latest = $derived(nodeStates().get(data.name));

	const STATUS = {
		warn: { dot: 'bg-warning', label: m.level_warn() },
		error: { dot: 'bg-error', label: m.level_error() }
	};

	const FRAME = {
		added: 'border-primary bg-primary/5',
		removed: 'border-error bg-error/5',
		changed: 'border-warning bg-warning/5'
	};

	let frame = $derived(
		data.mark
			? FRAME[data.mark]
			: latest?.level === 'error'
				? 'border-error/50 bg-base-100'
				: 'border-base-content/10 bg-base-100'
	);
</script>

<div
	data-task={data.name}
	class="task-node relative flex w-[240px] items-center gap-2.5 rounded-xl border px-2.5 py-2 shadow-[0_1px_2px_rgb(0_0_0/0.05),0_2px_8px_rgb(0_0_0/0.04)] {frame}"
>
	{#if data.onremove}
		{@const remove = data.onremove}
		<button
			type="button"
			class="{CIRCLE} absolute -top-2.5 -right-2.5 hover:border-error hover:text-error"
			aria-label={m.form_task_remove_named({ name: data.name })}
			onclick={(e) => {
				e.stopPropagation();
				remove(data.name);
			}}
		>
			<Icon icon="tabler:x" class="h-3 w-3" />
		</button>
	{/if}
	{#if data.onadd}
		{@const add = data.onadd}
		<span class="pointer-events-none absolute top-full left-1/2 h-5 w-px bg-base-content/40"></span>
		<button
			type="button"
			class="{CIRCLE} absolute top-full left-1/2 mt-5 -translate-x-1/2 hover:border-primary hover:text-primary"
			aria-label={m.form_add_task_after({ name: data.name })}
			onclick={(e) => {
				e.stopPropagation();
				add(data.name);
			}}
		>
			<Icon icon="tabler:plus" class="h-3 w-3" />
		</button>
	{/if}
	<span class="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-base-200">
		<img src={connector.iconUrl} alt={connector.label} class="h-4 w-4" />
	</span>
	<div class="min-w-0 flex-1">
		<div
			class={[
				'truncate text-sm leading-tight font-medium',
				data.mark === 'removed' && 'text-error line-through decoration-error'
			]}
		>
			{data.name}
		</div>
		<div class="truncate font-mono text-xs leading-tight text-base-content/50">{data.taskType}</div>
	</div>
	{#if data.mark}
		<span class="sr-only">{markLabel(data.mark)}</span>
	{/if}
	{#if latest}
		<span class="pointer-events-none flex shrink-0 items-center gap-1.5 text-[11px] text-base-content/50 tabular-nums">
			{#if latest.duration_ms !== undefined}
				{formatDuration(latest.duration_ms)}
			{/if}
			{#if latest.level !== 'info'}
				<span class="h-2 w-2 rounded-full {STATUS[latest.level].dot}"></span>
				<span class="sr-only">{STATUS[latest.level].label}</span>
			{/if}
		</span>
	{/if}

	<Handle type="target" position={Position.Top} />
	<Handle type="source" position={Position.Bottom} />
</div>
