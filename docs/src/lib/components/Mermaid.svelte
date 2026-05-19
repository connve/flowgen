<script lang="ts">
	import { onMount } from 'svelte';

	interface Props {
		chart: string;
	}

	let { chart }: Props = $props();
	let container: HTMLDivElement;

	onMount(async () => {
		const mermaid = (await import('mermaid')).default;
		mermaid.initialize({
			startOnLoad: false,
			theme: 'dark',
			themeVariables: {
				primaryColor: '#6419e6',
				primaryTextColor: '#e5e7eb',
				primaryBorderColor: '#8b5cf6',
				lineColor: '#8b5cf6',
				secondaryColor: '#1e1e2e',
				tertiaryColor: '#1e1e2e',
				nodeTextColor: '#e5e7eb',
				edgeLabelBackground: '#1e1e2e'
			},
			flowchart: {
				curve: 'basis',
				padding: 20
			}
		});
		const { svg } = await mermaid.render('mermaid-' + crypto.randomUUID().slice(0, 8), chart);
		container.innerHTML = svg;
	});
</script>

<div bind:this={container} class="my-6 flex justify-center"></div>
