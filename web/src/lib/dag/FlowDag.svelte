<script lang="ts">
	import { untrack } from 'svelte';
	import Icon from '@iconify/svelte';
	import {
		SvelteFlow,
		Background,
		Controls,
		ViewportPortal,
		type Node,
		type Edge
	} from '@xyflow/svelte';
	import { m } from '$lib/paraglide/messages.js';
	import '@xyflow/svelte/dist/style.css';
	import { parseFlow, type Dag } from './parseFlow';
	import { compareFlows } from '$lib/changePreview';
	import TaskNode from './TaskNode.svelte';
	import InsertEdge from './InsertEdge.svelte';
	import DagLayout from './DagLayout.svelte';
	import type { Activity } from '$lib/flow/ActivityDetails.svelte';
	import { setNodeStates, type NodeState } from './nodeStates';

	let {
		yaml,
		previous,
		activities,
		onNodeClick,
		onadd,
		onremove,
		onmove
	}: {
		yaml: string;
		previous?: string | null;
		/** The flow's activity, which marks each task with its latest status and duration. */
		activities?: Activity[];
		onNodeClick?: (taskName: string) => void;
		onadd?: (after: string | null) => void;
		onremove?: (taskName: string) => void;
		onmove?: (taskName: string, order: string[]) => void;
	} = $props();

	// Latest info/warn/error per task; debug and trace never replace it.
	let nodeStates = $derived.by(() => {
		const map = new Map<string, NodeState>();
		for (const a of activities ?? []) {
			if (!a.task) continue;
			if (a.level !== 'info' && a.level !== 'warn' && a.level !== 'error') continue;
			const prev = map.get(a.task);
			if (!prev || a.ts_ms >= prev.ts_ms) {
				map.set(a.task, { level: a.level, ts_ms: a.ts_ms, duration_ms: a.duration_ms });
			}
		}
		return map;
	});
	setNodeStates(() => nodeStates);

	// New nodes start stacked; `DagLayout` (inside SvelteFlow) positions them with
	// dagre once SvelteFlow has measured each node's real rendered size, so the
	// layout never depends on a hard-coded node width duplicated from the CSS.
	function build(dag: Dag, shown: Node[]): { nodes: Node[]; edges: Edge[] } {
		const placed = new Map(shown.map((node) => [node.id, node]));
		const nodes: Node[] = dag.nodes.map((n, i) => {
			const leaf = !dag.edges.some((e) => e.source === n.id);
			const data = { name: n.name, taskType: n.taskType, mark: n.mark, onadd: leaf ? onadd : undefined, onremove };
			const known = placed.get(n.id);
			if (known) return { ...known, data };
			const parentId = dag.edges.find((e) => e.target === n.id)?.source;
			const parent = parentId ? placed.get(parentId) : undefined;
			const position = parent
				? { x: parent.position.x, y: parent.position.y + (parent.measured?.height ?? 40) + 56 }
				: { x: 0, y: i * 8 };
			return { id: n.id, type: 'task', data, position };
		});
		const edges: Edge[] = dag.edges.map((e) => ({
			id: e.id,
			source: e.source,
			target: e.target,
			type: onadd ? 'insert' : 'smoothstep',
			data: { onadd },
			class: e.mark && `edge-${e.mark}`,
			animated: false
		}));
		return { nodes, edges };
	}

	const nodeTypes = { task: TaskNode };
	const edgeTypes = { insert: InsertEdge };

	let nodes = $state.raw<Node[]>([]);
	let edges = $state.raw<Edge[]>([]);
	let width = $state(0);
	let height = $state(0);
	let drops = $state(0);
	let dragging = $state(false);
	let startIndex = -1;
	let dropLine = $state<{ x: number; y: number; width: number } | null>(null);

	function topToBottom(shown: Node[]): string[] {
		return [...shown]
			.sort((a, b) => a.position.y - b.position.y || a.position.x - b.position.x)
			.map((node) => node.id);
	}

	function landing(dragged: Node): { x: number; y: number; width: number } | null {
		const height = (node: Node) => node.measured?.height ?? 0;
		const middle = dragged.position.y + height(dragged) / 2;
		const others = nodes
			.filter((node) => node.id !== dragged.id)
			.sort((a, b) => a.position.y - b.position.y);
		const index = others.filter((node) => node.position.y + height(node) / 2 < middle).length;
		if (index === startIndex || others.length === 0) return null;
		const above = others[index - 1];
		const below = others[index];
		const y = !above
			? below.position.y - 12
			: !below
				? above.position.y + height(above) + 12
				: (above.position.y + height(above) + below.position.y) / 2;
		const anchor = below ?? above;
		return { x: anchor.position.x, y, width: anchor.measured?.width ?? 240 };
	}
	let editing = $derived(onmove !== undefined);

	let dag = $derived(
		previous === undefined ? parseFlow(yaml) : (compareFlows(previous, yaml)?.dag ?? parseFlow(yaml))
	);

	$effect(() => {
		const built = build(dag, editing ? untrack(() => nodes) : []);
		nodes = built.nodes;
		edges = built.edges;
	});
</script>

<div
	class="dag-container relative h-full w-full"
	class:dragging
	class:editing
	bind:clientWidth={width}
	bind:clientHeight={height}
>
	{#if onadd && nodes.length === 0}
		{@const add = onadd}
		<button
			type="button"
			class="absolute top-1/2 left-1/2 z-10 flex h-8 -translate-x-1/2 -translate-y-1/2 items-center gap-1.5 rounded-full border border-base-content/40 bg-base-100 px-3 text-xs font-medium text-base-content/70 hover:border-primary hover:text-primary"
			onclick={() => add(null)}
		>
			<Icon icon="tabler:plus" class="h-3.5 w-3.5" />
			{m.form_add_task()}
		</button>
	{/if}
	<SvelteFlow
		bind:nodes
		bind:edges
		{nodeTypes}
		{edgeTypes}
		minZoom={0.2}
		maxZoom={2}
		panOnScroll
		proOptions={{ hideAttribution: true }}
		onnodeclick={({ node }) => onNodeClick?.(node.id)}
		onnodedragstart={({ targetNode }) => {
			dragging = true;
			startIndex = targetNode ? topToBottom(nodes).indexOf(targetNode.id) : -1;
		}}
		onnodedrag={({ targetNode }) => {
			if (targetNode && onmove) dropLine = landing(targetNode);
		}}
		onnodedragstop={({ targetNode }) => {
			dragging = false;
			dropLine = null;
			if (!targetNode || !onmove) return;
			onmove(targetNode.id, topToBottom(nodes));
			drops++;
		}}
	>
		<DagLayout {edges} {width} {height} {drops} keepView={editing} />
		{#if dropLine}
			<ViewportPortal target="front">
				<div
					class="pointer-events-none absolute h-0.5 rounded-full bg-primary"
					style="transform: translate({dropLine.x}px, {dropLine.y}px); width: {dropLine.width}px"
				></div>
			</ViewportPortal>
		{/if}
		<Background gap={24} patternColor="var(--color-base-300)" />
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
	.dag-container :global(.svelte-flow__handle) {
		opacity: 0;
	}
	.dag-container.editing:not(.dragging) :global(.svelte-flow__node) {
		transition: transform 150ms ease-out;
	}
	.dag-container.editing.dragging :global(.svelte-flow__edges),
	.dag-container.editing.dragging :global(.svelte-flow__edge-label) {
		opacity: 0;
	}
	.dag-container :global(.svelte-flow__edge-path) {
		stroke: var(--color-base-content);
		stroke-opacity: 0.4;
	}
	.dag-container :global(.edge-added .svelte-flow__edge-path) {
		stroke: var(--color-primary);
		stroke-opacity: 0.9;
	}
	.dag-container :global(.edge-removed .svelte-flow__edge-path) {
		stroke: var(--color-error);
		stroke-opacity: 0.8;
		stroke-dasharray: 4 4;
	}
</style>
