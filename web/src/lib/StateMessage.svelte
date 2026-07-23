<script lang="ts">
	// Full-panel empty/error state with a brand glow. `notice` for "nothing
	// here yet", `oops` for a failed load.
	import Icon from '@iconify/svelte';

	let {
		tone = 'notice',
		title,
		message,
		icon
	}: {
		tone?: 'notice' | 'oops';
		title?: string;
		message: string;
		icon?: string;
	} = $props();

	const defaults = {
		notice: { title: 'Nothing here yet', icon: 'tabler:sparkles' },
		oops: { title: 'Oops, something went wrong', icon: 'tabler:mood-sad' }
	} as const;

	let resolvedTitle = $derived(title ?? defaults[tone].title);
	let resolvedIcon = $derived(icon ?? defaults[tone].icon);
</script>

<div class="state-message relative flex min-h-full flex-1 flex-col items-center justify-center gap-3 px-6 py-16 text-center">
	<div class="state-glow pointer-events-none absolute inset-0" aria-hidden="true">
		<div class="state-blob state-blob-1"></div>
		<div class="state-blob state-blob-2"></div>
		<div class="state-blob state-blob-3"></div>
	</div>

	<span class="state-icon relative flex h-12 w-12 items-center justify-center rounded-full">
		<Icon icon={resolvedIcon} class="h-6 w-6" />
	</span>

	<div class="relative">
		<h2 class="state-title text-lg font-semibold">{resolvedTitle}</h2>
		<p class="mt-1 max-w-md text-sm opacity-70">{message}</p>
	</div>
</div>

<style>
	/* Brand glow — matches the agents hero. */
	.state-glow {
		filter: blur(80px);
		opacity: 0.22;
	}
	:global(:root[data-theme='mydark']) .state-glow {
		opacity: 0.28;
	}
	.state-blob {
		position: absolute;
		border-radius: 9999px;
		mix-blend-mode: multiply;
	}
	:global(:root[data-theme='mydark']) .state-blob {
		mix-blend-mode: screen;
	}
	.state-blob-1 {
		top: 5%;
		left: 12%;
		width: 30vw;
		height: 30vw;
		background: radial-gradient(circle, #00e168 0%, transparent 70%);
	}
	.state-blob-2 {
		top: 25%;
		right: 12%;
		width: 28vw;
		height: 28vw;
		background: radial-gradient(circle, #00b4a6 0%, transparent 70%);
	}
	.state-blob-3 {
		bottom: 5%;
		left: 38%;
		width: 26vw;
		height: 26vw;
		background: radial-gradient(circle, #006b55 0%, transparent 75%);
	}

	.state-icon {
		background: color-mix(in oklab, var(--color-primary) 12%, transparent);
		color: var(--color-primary);
	}

	/* Accent-gradient title, matching connve.com's shimmering headline. */
	.state-title {
		background: linear-gradient(120deg, #006b55 0%, #00b4a6 50%, #00e168 100%);
		background-clip: text;
		-webkit-background-clip: text;
		color: transparent;
	}
</style>
