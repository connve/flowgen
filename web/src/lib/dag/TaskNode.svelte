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
	class="task-node relative flex w-[250px] items-center gap-2 rounded-md border border-base-300 bg-base-100 px-2 py-1.5 shadow-sm"
>
	<img src="{base}/{connector.iconPath}" alt={connector.label} class="h-4 w-4 shrink-0" />
	<div class="min-w-0 flex-1">
		<div class="truncate font-mono text-xs font-medium leading-tight">{data.name}</div>
		<div class="truncate text-[10px] leading-tight opacity-60">{data.taskType}</div>
	</div>

	<!-- Always in normal flow (not position:absolute) so dagre's initial
	     measurement of the node already includes this width. The status/
	     duration set later by FlowInspector (data-level/data-duration,
	     after activity arrives) only toggles visibility of the CONTENTS
	     here — it never changes the node's box size, so no re-layout is
	     needed and neighbors never get overlapped. Fixed width (not
	     shrink-0 alone) keeps the level-mark circle clear of the node's
	     right border regardless of how long the name/task_type truncate to. -->
	<div class="side-panel pointer-events-none flex w-10 shrink-0 flex-col items-center gap-1">
		<span
			class="level-mark invisible inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full text-white"
		>
			<Icon icon="tabler:check" class="level-icon-info h-3 w-3" />
			<Icon icon="tabler:exclamation-mark" class="level-icon-warning h-3 w-3" />
			<Icon icon="tabler:x" class="level-icon-error h-3 w-3" />
		</span>
		<span
			class="duration-badge w-full overflow-hidden text-center font-mono text-[10px] leading-none opacity-70"
			>&nbsp;</span
		>
	</div>

	<Handle type="target" position={Position.Top} />
	<Handle type="source" position={Position.Bottom} />
</div>

<style>
	:global([data-task][data-level]) .level-mark,
	:global([data-task][data-duration]) .level-mark {
		visibility: visible;
	}

	:global(.level-icon-info),
	:global(.level-icon-warning),
	:global(.level-icon-error) {
		display: none;
	}

	:global([data-task][data-level='info']) .level-mark {
		background-color: var(--color-primary);
	}
	:global([data-task][data-level='info']) :global(.level-icon-info) {
		display: inline-block;
	}

	:global([data-task][data-level='warning']) .level-mark {
		background-color: var(--color-warning);
	}
	:global([data-task][data-level='warning']) :global(.level-icon-warning) {
		display: inline-block;
	}

	:global([data-task][data-level='error']) .level-mark {
		background-color: var(--color-error);
	}
	:global([data-task][data-level='error']) :global(.level-icon-error) {
		display: inline-block;
	}
</style>
