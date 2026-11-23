<script lang="ts">
	import Icon from '@iconify/svelte';
	import { locationParts } from '$lib/flowDocument';
	import { fieldLabel } from '$lib/YamlFields.svelte';

	interface Issue {
		path?: string;
		location?: string | null;
		message: string;
	}

	interface Props {
		issues: Issue[];
		contentOf: (path?: string) => string | null | undefined;
		showPath?: boolean;
		onselect?: (task: string) => void;
	}

	let { issues, contentOf, showPath = false, onselect }: Props = $props();

	const ROW = 'flex w-full items-baseline gap-2 rounded-md px-2 py-1.5 text-left text-sm';
</script>

{#snippet row(issue: Issue, task: string | null, field: string)}
	<Icon icon="tabler:alert-circle" class="h-4 w-4 shrink-0 translate-y-0.5 text-error" />
	{#if showPath && issue.path}
		<span class="shrink-0 font-mono text-xs text-base-content/50">{issue.path}</span>
	{/if}
	{#if task}
		<span class="shrink-0 font-medium">{task}</span>
	{/if}
	{#if field}
		<span class="shrink-0 text-base-content/50">
			{field.split(' › ').map(fieldLabel).join(' › ')}
		</span>
	{/if}
	<span class="min-w-0 text-base-content/80">{issue.message}</span>
{/snippet}

<ul class="space-y-0.5">
	{#each issues as issue, i (i)}
		{@const parts = issue.location ? locationParts(issue.location, contentOf(issue.path)) : null}
		{@const task = parts?.task ?? null}
		<li>
			{#if task && onselect}
				<button type="button" class="{ROW} group hover:bg-error/10" onclick={() => onselect(task)}>
					{@render row(issue, task, parts?.field ?? '')}
					<Icon
						icon="tabler:chevron-right"
						class="ml-auto h-4 w-4 shrink-0 self-center text-base-content/30 group-hover:text-base-content/60"
					/>
				</button>
			{:else}
				<div class={ROW}>
					{@render row(issue, task, parts?.field ?? '')}
				</div>
			{/if}
		</li>
	{/each}
</ul>
