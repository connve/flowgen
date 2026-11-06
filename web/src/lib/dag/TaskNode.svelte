<script lang="ts">
	import Icon from '@iconify/svelte';
	import { Handle, Position, type NodeProps } from '@xyflow/svelte';
	import { connectorFor } from '$lib/connectors';
	import { formatDuration } from '$lib/toolRuns.svelte';
	import { getNodeStates } from './nodeStates';

	interface TaskNodeData extends Record<string, unknown> {
		name: string;
		taskType: string;
	}

	let { data }: NodeProps & { data: TaskNodeData } = $props();

	const nodeStates = getNodeStates();
	let connector = $derived(connectorFor(data.taskType));
	let latest = $derived(nodeStates().get(data.name));

	const MARK = {
		info: { icon: 'tabler:check', bg: 'bg-primary' },
		warn: { icon: 'tabler:exclamation-mark', bg: 'bg-warning' },
		error: { icon: 'tabler:x', bg: 'bg-error' }
	};
</script>

<div
	data-task={data.name}
	class="task-node relative flex w-[250px] items-center gap-2 rounded-md border border-base-300 bg-base-100 px-2 py-1.5 shadow-sm"
>
	<img src={connector.iconUrl} alt={connector.label} class="h-4 w-4 shrink-0" />
	<div class="min-w-0 flex-1">
		<div class="truncate font-mono text-xs font-medium leading-tight">{data.name}</div>
		<div class="truncate text-[10px] leading-tight opacity-60">{data.taskType}</div>
	</div>

	<!-- Fixed width so the node keeps its measured size when a status arrives. -->
	<div class="pointer-events-none flex w-10 shrink-0 flex-col items-center gap-1">
		<span
			class="inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full text-white {latest
				? MARK[latest.level].bg
				: 'invisible'}"
		>
			{#if latest}
				<Icon icon={MARK[latest.level].icon} class="h-3 w-3" />
			{/if}
		</span>
		<span class="w-full overflow-hidden text-center font-mono text-[10px] leading-none opacity-70">
			{#if latest?.duration_ms !== undefined}
				{formatDuration(latest.duration_ms)}
			{:else if latest}
				—
			{:else}
				&nbsp;
			{/if}
		</span>
	</div>

	<Handle type="target" position={Position.Top} />
	<Handle type="source" position={Position.Bottom} />
</div>
