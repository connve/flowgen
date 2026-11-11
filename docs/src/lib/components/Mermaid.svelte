<script lang="ts">
	import { tick } from 'svelte';
	import { theme } from '@connve/ui';

	interface Props {
		chart: string;
	}

	let { chart }: Props = $props();
	let container: HTMLDivElement;

	// Mermaid reads only hex and rgb colors, so each theme token is painted and read back as rgb.
	function token(name: string): string {
		const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
		const context = document.createElement('canvas').getContext('2d', { willReadFrequently: true });
		if (!context) return value;
		context.fillStyle = value;
		context.fillRect(0, 0, 1, 1);
		const [r, g, b] = context.getImageData(0, 0, 1, 1).data;
		return `rgb(${r}, ${g}, ${b})`;
	}

	async function render(dark: boolean): Promise<string> {
		const mermaid = (await import('mermaid')).default;
		await tick();
		mermaid.initialize({
			startOnLoad: false,
			theme: 'base',
			darkMode: dark,
			fontFamily: 'inherit',
			themeVariables: {
				background: token('--color-base-100'),
				primaryColor: token('--color-base-200'),
				primaryTextColor: token('--color-base-content'),
				primaryBorderColor: token('--color-primary'),
				lineColor: token('--color-primary'),
				secondaryColor: token('--color-base-300'),
				tertiaryColor: token('--color-base-100'),
				clusterBkg: token('--color-base-100'),
				clusterBorder: token('--color-base-300'),
				nodeTextColor: token('--color-base-content'),
				edgeLabelBackground: token('--color-base-100')
			},
			flowchart: {
				curve: 'basis',
				padding: 20
			}
		});
		const { svg } = await mermaid.render('mermaid-' + crypto.randomUUID().slice(0, 8), chart);
		return svg;
	}

	$effect(() => {
		let current = true;
		render(theme.dark).then((svg) => {
			if (current) container.innerHTML = svg;
		});
		return () => {
			current = false;
		};
	});
</script>

<div bind:this={container} class="not-prose my-6 flex justify-center"></div>
