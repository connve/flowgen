<script lang="ts" module>
	export interface FormContext {
		/** The workspace's resources, by key. */
		resources: string[];
		/** The named credentials, as the Connections tab lists them. */
		credentials: string[];
		/** The language of inline code in this task, such as `rhai` or `sql`. */
		language: string | null;
	}

	/** Where a credentials name points, in the folder flowgen mounts them. */
	export function credentialsPath(name: string): string {
		return `{{env.FLOWGEN_CREDENTIALS}}/${name}.json`;
	}

	/** The name in a credentials path written by `credentialsPath`. */
	export function credentialsName(path: unknown): string | null {
		const match = typeof path === 'string' ? /^\{\{env\.FLOWGEN_CREDENTIALS\}\}\/(.+)\.json$/.exec(path) : null;
		return match ? match[1] : null;
	}
</script>

<script lang="ts">
	import Icon from '@iconify/svelte';
	import { dump, load } from 'js-yaml';
	import { Segmented } from '@connve/ui';
	import CodeEditor from '$lib/CodeEditor.svelte';
	import { fieldLabel } from '$lib/YamlFields.svelte';
	import SchemaField from '$lib/form/SchemaField.svelte';
	import { fieldsOf, isDefault, options, type Field, type Schema } from '$lib/form/schema';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		field: Field;
		root: Schema;
		value: unknown;
		context: FormContext;
		/** A new value; `undefined` leaves the field out. */
		onchange: (value: unknown) => void;
	}

	let { field, root, value, context, onchange }: Props = $props();

	const INPUT =
		'input input-sm flex w-full items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary';
	const SELECT = 'select select-sm w-full border border-base-300 bg-base-100 outline-none focus:border-primary';

	let label = $derived(fieldLabel(field.name));
	let choices = $derived(options(field.schema) ?? []);

	function set(next: unknown) {
		onchange(isDefault(field, next) ? undefined : next);
	}

	function setNumber(text: string) {
		if (text.trim() === '') return set(undefined);
		const parsed = field.kind === 'integer' ? parseInt(text, 10) : parseFloat(text);
		if (!Number.isNaN(parsed)) set(parsed);
	}

	let list = $derived(Array.isArray(value) ? value.map(String) : []);
	let draft = $state('');

	function addItem() {
		const item = draft.trim();
		if (!item) return;
		set([...list, item]);
		draft = '';
	}

	let entries = $derived(
		value && typeof value === 'object' && !Array.isArray(value)
			? Object.entries(value as Record<string, unknown>).map(([key, entry]) => [key, String(entry ?? '')])
			: []
	);

	function setEntry(at: number, key: string, entry: string) {
		const next = entries.map((pair, index) => (index === at ? [key, entry] : pair));
		set(Object.fromEntries(next.filter(([name]) => name !== '')));
	}

	let object = $derived(
		value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {}
	);

	function setChild(name: string, child: unknown) {
		const next = { ...object };
		if (child === undefined) delete next[name];
		else next[name] = child;
		set(Object.keys(next).length ? next : undefined);
	}

	let isResource = $derived(
		value !== null && typeof value === 'object' && 'resource' in (value as Record<string, unknown>)
	);
	let inline = $derived(typeof value === 'string' ? value : '');

	let yamlText = $state('');
	$effect(() => {
		if (field.kind !== 'yaml') return;
		yamlText = value === undefined ? '' : dump(value, { lineWidth: -1 }).trimEnd();
	});

	function setYaml(text: string) {
		yamlText = text;
		if (text.trim() === '') return set(undefined);
		try {
			set(load(text));
		} catch {
			// Kept as typed until it parses.
		}
	}
</script>

<div class="space-y-1">
	<div class="flex items-baseline gap-1.5">
		<span class="text-sm font-medium">{label}</span>
		{#if field.required}
			<span class="text-xs text-error" aria-label={m.form_required()}>*</span>
		{/if}
	</div>

	{#if field.kind === 'boolean'}
		<input
			type="checkbox"
			class="toggle toggle-sm toggle-primary"
			aria-label={label}
			checked={Boolean(value ?? field.default)}
			onchange={(e) => set(e.currentTarget.checked)}
		/>
	{:else if field.kind === 'enum'}
		<select class={SELECT} aria-label={label} value={String(value ?? field.default ?? '')} onchange={(e) => set(e.currentTarget.value)}>
			{#if !field.required && field.default === undefined}
				<option value="">—</option>
			{/if}
			{#each choices as choice (choice.value)}
				<option value={choice.value}>{choice.value}</option>
			{/each}
		</select>
	{:else if field.kind === 'integer' || field.kind === 'number'}
		<label class={INPUT}>
			<input
				type="number"
				class="grow"
				aria-label={label}
				value={value ?? ''}
				placeholder={field.default === undefined ? '' : String(field.default)}
				oninput={(e) => setNumber(e.currentTarget.value)}
			/>
		</label>
	{:else if field.kind === 'credentials'}
		{@const name = credentialsName(value)}
		<select
			class={SELECT}
			aria-label={label}
			value={name ?? (value ? String(value) : '')}
			onchange={(e) => set(e.currentTarget.value ? credentialsPath(e.currentTarget.value) : undefined)}
		>
			<option value="">{m.form_credentials_choose()}</option>
			{#each context.credentials as credential (credential)}
				<option value={credential}>{credential}</option>
			{/each}
			{#if value && !name}
				<option value={String(value)}>{String(value)}</option>
			{/if}
		</select>
	{:else if field.kind === 'source'}
		<Segmented
			{label}
			options={[
				{ id: 'inline', label: m.form_inline() },
				{ id: 'resource', label: m.form_resource() }
			]}
			value={isResource ? 'resource' : 'inline'}
			onchange={(next) => set(next === 'resource' ? { resource: '' } : '')}
		/>
		{#if isResource}
			<select
				class={SELECT}
				aria-label={label}
				value={String((value as Record<string, unknown>).resource ?? '')}
				onchange={(e) => set({ resource: e.currentTarget.value })}
			>
				<option value="">{m.form_resource_choose()}</option>
				{#each context.resources as resource (resource)}
					<option value={resource}>{resource}</option>
				{/each}
			</select>
		{:else}
			<div class="h-40 overflow-hidden rounded-md border border-base-300">
				<CodeEditor bind:value={() => inline, (text) => set(text)} extension={context.language} {label} />
			</div>
		{/if}
	{:else if field.kind === 'list'}
		<div class="flex flex-wrap items-center gap-1.5">
			{#each list as item, index (index)}
				<span class="badge badge-sm gap-1">
					{item}
					<button type="button" aria-label={m.form_remove()} onclick={() => set(list.filter((_, at) => at !== index))}>
						<Icon icon="tabler:x" class="h-3 w-3" />
					</button>
				</span>
			{/each}
			<label class="input input-sm flex min-w-32 flex-1 items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary">
				<input
					type="text"
					class="grow"
					aria-label={label}
					bind:value={draft}
					onkeydown={(e) => {
						if (e.key !== 'Enter') return;
						e.preventDefault();
						addItem();
					}}
					onblur={addItem}
				/>
			</label>
		</div>
	{:else if field.kind === 'map'}
		<div class="space-y-1.5">
			{#each entries as [key, entry], index (index)}
				<div class="flex items-center gap-1.5">
					<label class={INPUT}>
						<input type="text" class="grow" aria-label={m.form_key()} value={key} onchange={(e) => setEntry(index, e.currentTarget.value, entry)} />
					</label>
					<label class={INPUT}>
						<input type="text" class="grow" aria-label={m.form_value()} value={entry} onchange={(e) => setEntry(index, key, e.currentTarget.value)} />
					</label>
					<button type="button" class="flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-base-content/50 hover:bg-base-200 hover:text-base-content" aria-label={m.form_remove()} onclick={() => set(Object.fromEntries(entries.filter((_, at) => at !== index)))}>
						<Icon icon="tabler:x" class="h-4 w-4" />
					</button>
				</div>
			{/each}
			<button type="button" class="flex h-8 items-center gap-1.5 rounded-md px-2 text-sm text-base-content/70 hover:bg-base-200" onclick={() => set(Object.fromEntries([...entries, ['', '']]))}>
				<Icon icon="tabler:plus" class="h-4 w-4" />
				{m.form_add()}
			</button>
		</div>
	{:else if field.kind === 'object'}
		<div class="space-y-3 border-l border-base-300 pl-3">
			{#each fieldsOf(root, field.schema) as child (child.name)}
				<SchemaField field={child} {root} value={object[child.name]} {context} onchange={(next) => setChild(child.name, next)} />
			{/each}
		</div>
	{:else if field.kind === 'yaml'}
		<div class="h-32 overflow-hidden rounded-md border border-base-300">
			<CodeEditor bind:value={() => yamlText, setYaml} extension="yaml" {label} />
		</div>
	{:else}
		<label class={INPUT}>
			<input
				type="text"
				class="grow"
				aria-label={label}
				value={value ?? ''}
				placeholder={field.kind === 'duration' ? '30s' : field.default === undefined ? '' : String(field.default)}
				onchange={(e) => set(e.currentTarget.value)}
			/>
		</label>
	{/if}

	{#if field.description}
		<p class="text-xs text-base-content/60">{field.description}</p>
	{/if}
</div>
