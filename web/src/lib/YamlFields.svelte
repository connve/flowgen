<script lang="ts" module>
	export type Fields = Record<string, unknown>;

	export function isFields(value: unknown): value is Fields {
		return typeof value === 'object' && value !== null && !Array.isArray(value);
	}

	const ACRONYMS = new Set(['ai', 'api', 'cdc', 'http', 'id', 'jwt', 'kv', 'llm', 'mcp', 'oci', 'sql', 'tls', 'ttl', 'uri', 'url']);

	/** A config key as a label, e.g. `credentials_path` → "Credentials path", `mcp_url` → "MCP URL". */
	export function fieldLabel(key: string): string {
		const label = key
			.split('_')
			.map((word) => (ACRONYMS.has(word) ? word.toUpperCase() : word))
			.join(' ');
		return label.charAt(0).toUpperCase() + label.slice(1);
	}
</script>

<script lang="ts">
	import Icon from '@iconify/svelte';
	import { highlight } from '$lib/highlight';
	import YamlFields from '$lib/YamlFields.svelte';

	interface Props {
		entries: Fields;
		/** A smaller label column, for an object inside another. */
		nested?: boolean;
		onResourceClick?: (key: string) => void;
	}

	let { entries, nested = false, onResourceClick }: Props = $props();

	/** Keys whose text is code, by the grammar it is highlighted with. */
	const CODE: Record<string, string> = {
		code: 'rhai',
		script: 'rhai',
		query: 'sql',
		sql: 'sql',
		statement: 'sql'
	};

	function resourceOf(value: unknown): string | null {
		if (!isFields(value)) return null;
		return Object.keys(value).length === 1 && typeof value.resource === 'string' ? value.resource : null;
	}

	/** Splits text into plain runs and `{{ … }}` templates. */
	function runs(text: string): { text: string; template: boolean }[] {
		return text
			.split(/(\{\{[\s\S]*?\}\})/)
			.filter((part) => part !== '')
			.map((part) => ({ text: part, template: part.startsWith('{{') }));
	}

	function isBlock(key: string, text: string): boolean {
		return key in CODE || text.includes('\n') || text.length > 120;
	}

	function isScalar(value: unknown): boolean {
		return !isFields(value) && !Array.isArray(value);
	}

	/** An object, other than a resource link, or a list holding objects. */
	function nests(value: unknown): boolean {
		if (Array.isArray(value)) return !value.every(isScalar);
		return isFields(value) && resourceOf(value) === null;
	}
</script>

{#snippet text(value: string)}{#each runs(value) as run, i (i)}{#if run.template}<span
				class="rounded bg-primary/10 px-1 font-mono text-[0.8125rem] text-primary">{run.text}</span
			>{:else}{run.text}{/if}{/each}{/snippet}

{#snippet value(key: string, item: unknown)}
	{@const resource = resourceOf(item)}
	{#if resource !== null}
		<button
			type="button"
			class="inline-flex max-w-full items-center gap-1.5 text-primary hover:underline"
			onclick={() => onResourceClick?.(resource)}
		>
			<Icon icon="tabler:file-code" class="h-4 w-4 shrink-0" />
			<span class="truncate">{resource}</span>
		</button>
	{:else if typeof item === 'string' && isBlock(key, item)}
		{@const code = CODE[key] ? highlight(item, CODE[key]) : null}
		{#if code !== null}
			<pre
				class="whitespace-pre-wrap break-words rounded-md bg-base-200 px-3 py-2 font-mono text-xs leading-relaxed"><code
					>{@html code}</code
				></pre>
		{:else}
			<p class="whitespace-pre-wrap break-words">{@render text(item)}</p>
		{/if}
	{:else if typeof item === 'string'}
		<span class="break-words">{@render text(item)}</span>
	{:else if typeof item === 'number' || typeof item === 'boolean'}
		<span class="font-mono text-[0.8125rem]">{String(item)}</span>
	{:else if item === null || item === undefined}
		<span class="text-base-content/40">—</span>
	{:else if Array.isArray(item) && item.every(isScalar)}
		<span class="flex flex-wrap gap-1">
			{#each item as entry, i (i)}
				<span class="chip-sm chip-neutral">{String(entry)}</span>
			{/each}
		</span>
	{:else if Array.isArray(item)}
		<div class="space-y-4">
			{#each item as entry, i (i)}
				<div>
					{@render value(key, entry)}
				</div>
			{/each}
		</div>
	{:else if isFields(item)}
		<YamlFields entries={item} nested {onResourceClick} />
	{/if}
{/snippet}

<!-- An object or a list of objects goes under its label, indented, so each level
     takes a little width instead of a label column. -->
<dl
	class="grid gap-x-6 gap-y-2 {nested
		? 'grid-cols-[minmax(0,8rem)_minmax(0,1fr)]'
		: 'grid-cols-[minmax(0,10rem)_minmax(0,1fr)]'}"
>
	{#each Object.entries(entries) as [key, item] (key)}
		{#if nests(item)}
			<dt class="col-span-2 text-base-content/50">{fieldLabel(key)}</dt>
			<dd class="col-span-2 min-w-0 pl-4">{@render value(key, item)}</dd>
		{:else}
			<dt class="min-w-0 break-words text-base-content/50">{fieldLabel(key)}</dt>
			<dd class="min-w-0">{@render value(key, item)}</dd>
		{/if}
	{/each}
</dl>
