<script lang="ts">
	import Icon from '@iconify/svelte';
	import { BaseEdge, EdgeLabel, getSmoothStepPath, type EdgeProps } from '@xyflow/svelte';
	import { m } from '$lib/paraglide/messages.js';
	import { CIRCLE } from './controls';

	let { source, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data }: EdgeProps =
		$props();

	let path = $derived(
		getSmoothStepPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition })
	);
	let onadd = $derived(data?.onadd as ((after: string) => void) | undefined);
</script>

<BaseEdge path={path[0]} />
{#if onadd}
	{@const add = onadd}
	<EdgeLabel x={path[1]} y={path[2]} transparent>
		<button
			type="button"
			class="{CIRCLE} hover:border-primary hover:text-primary"
			aria-label={m.form_add_task_after({ name: source })}
			onclick={() => add(source)}
		>
			<Icon icon="tabler:plus" class="h-3 w-3" />
		</button>
	</EdgeLabel>
{/if}
