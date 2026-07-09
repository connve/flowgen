<script lang="ts">
	import { base } from '$app/paths';
	import { Handle, Position, type NodeProps } from '@xyflow/svelte';
	import { connectorFor } from './moduleMap';

	interface TaskNodeData extends Record<string, unknown> {
		name: string;
		taskType: string;
	}

	let { data }: NodeProps & { data: TaskNodeData } = $props();

	let connector = $derived(connectorFor(data.taskType));
</script>

<div
	class="flex w-[170px] items-center gap-1.5 rounded-md border border-base-300 bg-base-100 px-2 py-1.5 shadow-sm"
>
	<img src="{base}/{connector.iconPath}" alt={connector.label} class="h-4 w-4 shrink-0" />
	<div class="min-w-0 flex-1">
		<div class="truncate font-mono text-xs font-medium leading-tight">{data.name}</div>
		<div class="truncate text-[10px] leading-tight opacity-60">{data.taskType}</div>
	</div>

	<Handle type="target" position={Position.Top} />
	<Handle type="source" position={Position.Bottom} />
</div>
