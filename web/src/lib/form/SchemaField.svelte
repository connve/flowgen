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
	import { Combobox, FormField, Segmented, Select, TokenField } from '@connve/ui';
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
	const EDITOR = 'max-h-80 min-h-12 overflow-auto rounded-md border border-base-300 bg-base-200';

	let label = $derived(fieldLabel(field.name));
	let placeholder = $derived(field.default == null ? '' : String(field.default));

	function setCredentials(text: string) {
		const name = text.trim();
		if (!name) return set(undefined);
		set(name.includes('/') ? name : credentialsPath(name));
	}
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

<FormField {label} required={field.required} help={field.description}>

	{#if field.kind === 'boolean'}
		<input
			type="checkbox"
			class="toggle toggle-sm toggle-primary"
			aria-label={label}
			checked={Boolean(value ?? field.default)}
			onchange={(e) => set(e.currentTarget.checked)}
		/>
	{:else if field.kind === 'enum'}
		<Select
			{label}
			value={String(value ?? field.default ?? '')}
			options={!field.required && field.default === undefined ? [{ value: '' }, ...choices] : choices}
			placeholder="—"
			onchange={(next) => set(next === '' ? undefined : next)}
		/>
	{:else if field.kind === 'integer' || field.kind === 'number'}
		<label class={INPUT}>
			<input
				type="number"
				class="grow [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
				aria-label={label}
				value={value ?? ''}
				placeholder={placeholder}
				oninput={(e) => setNumber(e.currentTarget.value)}
			/>
		</label>
	{:else if field.kind === 'credentials'}
		{@const name = credentialsName(value)}
		<Combobox
			{label}
			value={name ?? (value ? String(value) : '')}
			options={context.credentials}
			placeholder={m.form_credentials_choose()}
			onchange={setCredentials}
		/>
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
			<Combobox
				{label}
				value={String((value as Record<string, unknown>).resource ?? '')}
				options={context.resources}
				placeholder={m.form_resource_choose()}
				onchange={(resource) => set({ resource })}
			/>
		{:else}
			<div class={EDITOR}>
				<CodeEditor bind:value={() => inline, (text) => set(text)} extension={context.language} {label} compact />
			</div>
		{/if}
	{:else if field.kind === 'list'}
		<TokenField {label} value={list} onchange={(next) => set(next.length > 0 ? next : undefined)} />
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
		<div class="space-y-3 pl-3">
			{#each fieldsOf(root, field.schema) as child (child.name)}
				<SchemaField field={child} {root} value={object[child.name]} {context} onchange={(next) => setChild(child.name, next)} />
			{/each}
		</div>
	{:else if field.kind === 'yaml'}
		<div class={EDITOR}>
			<CodeEditor bind:value={() => yamlText, setYaml} extension="yaml" {label} compact />
		</div>
	{:else}
		<label class={INPUT}>
			<input
				type="text"
				class="grow"
				aria-label={label}
				value={value ?? ''}
				placeholder={field.kind === 'duration' && !placeholder ? '30s' : placeholder}
				onchange={(e) => set(e.currentTarget.value)}
			/>
		</label>
	{/if}
</FormField>
