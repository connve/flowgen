<script lang="ts">
	import { codingAgents } from '@connve/ui';
</script>

<nav class="agent-clients not-prose" aria-label="Agents">
	{#each codingAgents as agent (agent.anchor)}
		<a href="#{agent.anchor}" class="agent-client">
			<svg viewBox="0 0 24 24" class="agent-client-icon" fill-rule="evenodd" aria-hidden="true">
				<path d={agent.path} />
			</svg>
			<span>{agent.name}</span>
		</a>
	{/each}
</nav>

<style>
	.agent-clients {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(10rem, 1fr));
		gap: 0.75rem;
		margin: 1.5rem 0;
	}
	.agent-client {
		display: flex;
		align-items: center;
		gap: 0.625rem;
		padding: 0.75rem 1rem;
		border: 1px solid var(--color-base-300);
		border-radius: 0.5rem;
		color: var(--color-base-content);
		font-size: 0.875rem;
		font-weight: 500;
		text-decoration: none;
		transition: border-color 0.15s;
	}
	.agent-client:hover {
		border-color: var(--color-primary);
	}
	.agent-client-icon {
		width: 1.25rem;
		height: 1.25rem;
		flex-shrink: 0;
		fill: currentColor;
	}
</style>
