<script lang="ts">
	import { untrack } from 'svelte';

	/** `beat` changes when events flow; each change plays one wave, at most one at a time. */
	let { class: className = '', beat = 0 }: { class?: string; beat?: number } = $props();

	const MIN_GAP_MS = 1600;
	let wave = $state(0);
	let lastWave = 0;

	$effect(() => {
		if (!beat) return;
		untrack(() => {
			const now = Date.now();
			if (now - lastWave < MIN_GAP_MS) return;
			lastWave = now;
			wave += 1;
		});
	});

	const DOTS = [
		{ cx: 44.61, cy: 6.03, dx: -8, delay: 0 },
		{ cx: 61.65, cy: 15.42, dx: 8, delay: 0.1 },
		{ cx: 20.63, cy: 19.99, dx: 8, delay: 0.2 },
		{ cx: 35.24, cy: 33.96, dx: -8, delay: 0.3 },
		{ cx: 20.63, cy: 48.24, dx: 8, delay: 0.4 }
	];
</script>

<svg class="logo {className}" viewBox="0 0 86 55" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
	<g fill="none" stroke="currentColor" stroke-width="1.114" stroke-linecap="round" stroke-linejoin="round">
		<path d="M0.56 6.03H44.61" />
		<path d="M61.65 15.42H85.16" />
		<path d="M20.63 19.99H45.44L57.64 31.96H85.16" />
		<path d="M0.56 33.96H35.24" />
		<path d="M20.63 48.24H50.43L57.34 41.33H85.16" />
	</g>
	{#key wave}
		{#each DOTS as dot (dot.cx + dot.cy)}
			<circle
				class="logo-dot"
				cx={dot.cx}
				cy={dot.cy}
				r="6.03"
				style="--dx: {dot.dx}px; animation-delay: {dot.delay}s"
			/>
		{/each}
	{/key}
</svg>

<style>
	.logo-dot {
		fill: currentColor;
		animation: logo-slide 1.2s ease-in-out 1;
	}
	:global(a:hover) > .logo .logo-dot,
	.logo:hover .logo-dot {
		animation-iteration-count: infinite;
	}
	@keyframes logo-slide {
		0%,
		100% {
			transform: translateX(0);
			fill: currentColor;
		}
		50% {
			transform: translateX(var(--dx));
			fill: var(--color-accent);
		}
	}
	@media (prefers-reduced-motion: reduce) {
		.logo-dot {
			animation: none;
		}
	}
</style>
