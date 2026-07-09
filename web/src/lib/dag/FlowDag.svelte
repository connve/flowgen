<script lang="ts">
	import { SvelteFlow, Background, Controls, type Node, type Edge } from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import dagre from '@dagrejs/dagre';
	import { parseFlow } from './parseFlow';
	import TaskNode from './TaskNode.svelte';

	let {
		yaml,
		onNodeClick
	}: { yaml: string; onNodeClick?: (taskName: string) => void } = $props();

	const NODE_WIDTH = 170;
	const NODE_HEIGHT = 40;

	function layout(dag: ReturnType<typeof parseFlow>): { nodes: Node[]; edges: Edge[] } {
		const g = new dagre.graphlib.Graph();
		g.setDefaultEdgeLabel(() => ({}));
		g.setGraph({ rankdir: 'TB', nodesep: 24, ranksep: 36 });

		for (const n of dag.nodes) g.setNode(n.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
		for (const e of dag.edges) g.setEdge(e.source, e.target);

		dagre.layout(g);

		const nodes: Node[] = dag.nodes.map((n) => {
			const pos = g.node(n.id);
			return {
				id: n.id,
				type: 'task',
				data: { name: n.name, taskType: n.taskType },
				position: { x: pos.x - NODE_WIDTH / 2, y: pos.y - NODE_HEIGHT / 2 }
			};
		});
		const edges: Edge[] = dag.edges.map((e) => ({
			id: e.id,
			source: e.source,
			target: e.target,
			animated: false
		}));
		return { nodes, edges };
	}

	const nodeTypes = { task: TaskNode };
	let laidOut = $derived(layout(parseFlow(yaml)));
</script>

<div class="dag-container h-full w-full">
	<SvelteFlow
		nodes={laidOut.nodes}
		edges={laidOut.edges}
		{nodeTypes}
		fitView
		fitViewOptions={{ maxZoom: 1, padding: 0.2 }}
		minZoom={0.2}
		maxZoom={1.5}
		proOptions={{ hideAttribution: true }}
		onnodeclick={({ node }) => onNodeClick?.(node.id)}
	>
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
