<script lang="ts">
	type Variant = 'neutral' | 'success' | 'warning' | 'error';

	let { variant = 'neutral', label }: { variant?: Variant; label: string } = $props();

	const DOT: Record<Variant, string> = {
		neutral: 'bg-base-content/30',
		success: 'bg-primary',
		warning: 'bg-warning',
		error: 'bg-error'
	};
</script>

<span class="inline-flex items-center gap-1.5 whitespace-nowrap {variant === 'error' ? 'text-error' : ''}">
	<span class="h-2 w-2 shrink-0 rounded-full {DOT[variant]}" aria-hidden="true"></span>
	{label}
</span>
