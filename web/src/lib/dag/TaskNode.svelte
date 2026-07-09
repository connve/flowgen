<script lang="ts">
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
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
	data-task={data.name}
	class="task-node flex w-[170px] items-center gap-1.5 rounded-md border border-base-300 bg-base-100 px-2 py-1.5 shadow-sm"
>
	<img src="{base}/{connector.iconPath}" alt={connector.label} class="h-4 w-4 shrink-0" />
	<div class="min-w-0 flex-1">
		<div class="truncate font-mono text-xs font-medium leading-tight">{data.name}</div>
		<div class="truncate text-[10px] leading-tight opacity-60">{data.taskType}</div>
	</div>
	<span class="level-mark hidden h-3 w-3 shrink-0 items-center justify-center rounded-full text-white">
		<Icon icon="tabler:check" class="level-icon-info h-2.5 w-2.5" />
		<Icon icon="tabler:exclamation-mark" class="level-icon-warning h-2.5 w-2.5" />
		<Icon icon="tabler:x" class="level-icon-error h-2.5 w-2.5" />
	</span>

	<Handle type="target" position={Position.Top} />
	<Handle type="source" position={Position.Bottom} />
</div>

<style>
	:global(.level-icon-info),
	:global(.level-icon-warning),
	:global(.level-icon-error) {
		display: none;
	}

	:global([data-task][data-level='info']) .level-mark {
		display: inline-flex;
		background-color: var(--color-primary);
	}
	:global([data-task][data-level='info']) :global(.level-icon-info) {
		display: inline-block;
	}

	:global([data-task][data-level='warning']) .level-mark {
		display: inline-flex;
		background-color: var(--color-warning);
	}
	:global([data-task][data-level='warning']) :global(.level-icon-warning) {
		display: inline-block;
	}

	:global([data-task][data-level='error']) .level-mark {
		display: inline-flex;
		background-color: var(--color-error);
	}
	:global([data-task][data-level='error']) :global(.level-icon-error) {
		display: inline-block;
	}
</style>
