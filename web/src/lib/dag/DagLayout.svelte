<script lang="ts">
	import dagre from '@dagrejs/dagre';
	import { useNodesInitialized, useSvelteFlow, type Edge } from '@xyflow/svelte';

	// Runs inside <SvelteFlow> so it can read each node's real rendered size.
	// Dagre needs node dimensions to lay out ranks; taking them from
	// `node.measured` (populated by SvelteFlow after the first render) keeps the
	// layout correct without duplicating the node's CSS size in code.
	let { edges }: { edges: Edge[] } = $props();

	const nodesInitialized = useNodesInitialized();
	const { getNodes, updateNode, fitView } = useSvelteFlow();

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
		fitView({ maxZoom: 1.5, padding: 0.15 });
	});
</script>
