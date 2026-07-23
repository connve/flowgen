<script lang="ts">
	import { SvelteFlow, Background, Controls, type Node, type Edge } from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import { parseFlow } from './parseFlow';
	import TaskNode from './TaskNode.svelte';
	import DagLayout from './DagLayout.svelte';

	let {
		yaml,
		onNodeClick
	}: { yaml: string; onNodeClick?: (taskName: string) => void } = $props();

	// Nodes start stacked; `DagLayout` (inside SvelteFlow) positions them with
	// dagre once SvelteFlow has measured each node's real rendered size, so the
	// layout never depends on a hard-coded node width duplicated from the CSS.
	function build(dag: ReturnType<typeof parseFlow>): { nodes: Node[]; edges: Edge[] } {
		const nodes: Node[] = dag.nodes.map((n, i) => ({
			id: n.id,
			type: 'task',
			data: { name: n.name, taskType: n.taskType },
			position: { x: 0, y: i * 8 }
		}));
		const edges: Edge[] = dag.edges.map((e) => ({
			id: e.id,
			source: e.source,
			target: e.target,
			type: 'smoothstep',
			animated: false
		}));
		return { nodes, edges };
	}

	const nodeTypes = { task: TaskNode };

	let nodes = $state.raw<Node[]>([]);
	let edges = $state.raw<Edge[]>([]);

	$effect(() => {
		const built = build(parseFlow(yaml));
		nodes = built.nodes;
		edges = built.edges;
	});
</script>

<div class="dag-container h-full w-full">
	<SvelteFlow
		bind:nodes
		bind:edges
		{nodeTypes}
		minZoom={0.2}
		maxZoom={2}
		proOptions={{ hideAttribution: true }}
		onnodeclick={({ node }) => onNodeClick?.(node.id)}
	>
		<DagLayout {edges} />
		<Background />
		<Controls showLock={false} />
	</SvelteFlow>
</div>

<style>
	/* Svelte Flow ships light-only defaults for its controls and background —
	   theme them off the DaisyUI base tokens so both palettes look at home. */
	.dag-container :global(.svelte-flow) {
		background-color: var(--color-base-100);
	}
	.dag-container :global(.svelte-flow__controls) {
		box-shadow: none;
		border: 1px solid var(--color-base-300);
		border-radius: 6px;
		overflow: hidden;
	}
	.dag-container :global(.svelte-flow__controls-button) {
		background-color: var(--color-base-100);
		border-color: var(--color-base-300);
		color: var(--color-base-content);
		fill: currentColor;
	}
	.dag-container :global(.svelte-flow__controls-button:hover) {
		background-color: var(--color-base-200);
	}
	.dag-container :global(.svelte-flow__controls-button svg) {
		fill: currentColor;
	}
	.dag-container :global(.svelte-flow__background) {
		background-color: var(--color-base-100);
	}
	.dag-container :global(.svelte-flow__edge-path) {
		stroke: var(--color-base-content);
		stroke-opacity: 0.4;
	}
</style>
