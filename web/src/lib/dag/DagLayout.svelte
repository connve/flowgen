<script lang="ts">
	import dagre from '@dagrejs/dagre';
	import { untrack } from 'svelte';
	import { useNodesInitialized, useSvelteFlow, type Edge } from '@xyflow/svelte';

	// Runs inside <SvelteFlow> so it can read each node's real rendered size.
	// Dagre needs node dimensions to lay out ranks; taking them from
	// `node.measured` (populated by SvelteFlow after the first render) keeps the
	// layout correct without duplicating the node's CSS size in code.
	let { edges, width, height }: { edges: Edge[]; width: number; height: number } = $props();

	// Below this zoom task names are unreadable; a larger graph scrolls instead.
	const READABLE_ZOOM = 0.75;
	const PADDING = 24;

	const nodesInitialized = useNodesInitialized();
	const { getNodes, updateNode, fitView, getViewport, setViewport } = useSvelteFlow();

	// Re-layout whenever the edge set changes or the nodes (re-)measure.
	// `laidOutKey` must reset when `nodesInitialized` drops to `false` —
	// SvelteFlow can reuse the pane across modal opens, so `nodesInitialized`
	// cycles false→true again for the same edge key on a second open; without
	// the reset, the stale key already matches and layout never reruns for
	// the freshly re-stacked nodes.
	let laidOutKey = $state('');

	$effect(() => {
		if (!nodesInitialized.current) {
			laidOutKey = '';
			return;
		}
		const key = edges.map((e) => e.id).join('|');
		if (laidOutKey === key) return;

		const nodes = getNodes();
		if (nodes.some((n) => !n.measured?.width || !n.measured?.height)) return;

		const g = new dagre.graphlib.Graph();
		g.setDefaultEdgeLabel(() => ({}));
		g.setGraph({ rankdir: 'TB', nodesep: 40, ranksep: 56, edgesep: 24 });
		for (const n of nodes) {
			g.setNode(n.id, { width: n.measured!.width!, height: n.measured!.height! });
		}
		for (const e of edges) g.setEdge(e.source, e.target);
		dagre.layout(g);

		for (const n of nodes) {
			const p = g.node(n.id);
			updateNode(n.id, { position: { x: p.x - p.width / 2, y: p.y - p.height / 2 } });
		}
		laidOutKey = key;
		show(g.graph().width!, g.graph().height!);
	});

	// Fits the graph when it stays readable, otherwise starts at its first task.
	function show(graphWidth: number, graphHeight: number) {
		const fit = Math.min(
			(width - 2 * PADDING) / graphWidth,
			(height - 2 * PADDING) / graphHeight
		);
		if (fit >= READABLE_ZOOM) {
			fitView({ maxZoom: 1.5, padding: 0.15 });
			return;
		}
		const zoom = READABLE_ZOOM;
		setViewport({ x: (width - graphWidth * zoom) / 2, y: PADDING, zoom });
	}

	// Keeps the graph's centre and zoom when the pane is resized.
	let previousWidth = 0;

	$effect(() => {
		const delta = width - previousWidth;
		const resized = previousWidth > 0 && delta !== 0;
		previousWidth = width;
		if (!resized || untrack(() => laidOutKey) === '') return;
		const { x, y, zoom } = getViewport();
		setViewport({ x: x + delta / 2, y, zoom }, { duration: 200 });
	});
</script>
