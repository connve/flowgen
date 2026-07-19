<script lang="ts">
	import { base } from '$app/paths';
	import type { NavItem } from '$lib/nav';

	let { prev, next }: { prev?: NavItem; next?: NavItem } = $props();
</script>

{#if prev || next}
	<nav class="prev-next not-prose" aria-label="Documentation pagination">
		{#if prev}
			<a href="{base}{prev.href}" rel="prev" class="prev-next-link prev-next-prev">
				<span class="prev-next-label">← Previous</span>
				<span class="prev-next-title">{prev.title}</span>
			</a>
		{:else}
			<span></span>
		{/if}
		{#if next}
			<a href="{base}{next.href}" rel="next" class="prev-next-link prev-next-next">
				<span class="prev-next-label">Next →</span>
				<span class="prev-next-title">{next.title}</span>
			</a>
		{/if}
	</nav>
{/if}

<style>
	.prev-next {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		margin-top: 3rem;
		padding-top: 1.5rem;
		border-top: 1px solid rgb(0 0 0 / 0.08);
	}
	.prev-next-link {
		display: flex;
		flex-direction: column;
		padding: 0.75rem 1rem;
		border: 1px solid rgb(0 0 0 / 0.1);
		border-radius: 0.5rem;
		text-decoration: none;
		color: inherit;
		min-width: 12rem;
		max-width: 20rem;
		transition:
			border-color 0.15s ease,
			transform 0.15s ease;
	}
	.prev-next-link:hover {
		border-color: #006b55;
		transform: translateY(-1px);
	}
	.prev-next-next {
		align-items: flex-end;
		text-align: right;
		margin-left: auto;
	}
	.prev-next-label {
		font-size: 0.75rem;
		opacity: 0.6;
	}
	.prev-next-title {
		font-weight: 500;
		font-size: 0.9375rem;
		margin-top: 0.25rem;
	}
</style>
