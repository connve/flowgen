<script lang="ts">
	// Single badge style for the whole admin: uppercase, small.
	// - neutral: outlined, monochrome, faded (used for type tags: flow, sql, json).
	// - success/error/warning: filled soft (bg-*/10 + text-*) — same look as the
	//   Copied confirmation, reads strongly against row background.
	interface Props {
		children: import('svelte').Snippet;
		variant?: 'neutral' | 'success' | 'error' | 'warning';
	}

	let { children, variant = 'neutral' }: Props = $props();

	let variantClass = $derived.by(() => {
		switch (variant) {
			case 'success':
				return 'border-transparent bg-primary/10 text-primary';
			case 'error':
				return 'border-transparent bg-error/10 text-error';
			case 'warning':
				return 'border-transparent bg-warning/10 text-warning';
			default:
				return 'border-current opacity-60';
		}
	});
</script>

<span
	class="inline-block whitespace-nowrap rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase leading-none tracking-wider {variantClass}"
>
	{@render children()}
</span>
