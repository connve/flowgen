<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import CodeEditor from '$lib/CodeEditor.svelte';
	import { encryptCredential, errorMessage, type CredentialsField } from '$lib/api';
	import { authoringTargets, proposeFile } from '$lib/changes';
	import type { Connector } from '$lib/connectors';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		/** The credentials' name, as the connection's credentials path names them. */
		name: string;
		connector: Connector;
		/** Whether flowgen finds them: replacing them rather than adding. */
		found: boolean;
	}

	let { name, connector, found }: Props = $props();

	const INPUT =
		'input input-sm flex w-full items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary';

	let open = $state(false);
	let chosen = $state(0);
	let values = $state<Record<string, string | number>>({});
	let checked = $state<Record<string, boolean>>({});
	let file = $state('');
	let saving = $state(false);
	let failure = $state<string | null>(null);

	let forms = $derived(connector.credentials);
	let form = $derived(forms[chosen]);
	let other = $derived(forms.length === 0);
	let upload = $derived(form !== undefined && form.fields.length === 0);

	// The list of ways already names a field called like the chosen way.
	function labelled(label: string | undefined): boolean {
		return forms.length === 1 || label !== form?.label;
	}

	function inputType(field: CredentialsField): string {
		switch (field.kind) {
			case 'secret':
				return 'password';
			case 'number':
				return 'number';
			case 'url':
				return 'url';
			default:
				return 'text';
		}
	}

	function isUrl(text: string): boolean {
		try {
			const url = new URL(text);
			return url.protocol === 'https:' || url.protocol === 'http:';
		} catch {
			return false;
		}
	}

	/** Starts the chosen form over at its defaults, dropping anything typed. */
	function reset() {
		const fields = form?.fields ?? [];
		values = Object.fromEntries(
			fields
				.filter((field) => field.kind === 'choice' && field.required)
				.map((field) => [field.key, field.choices?.[0] ?? ''])
		);
		checked = Object.fromEntries(
			fields.filter((field) => field.kind === 'boolean').map((field) => [field.key, field.checked ?? false])
		);
		file = '';
		failure = null;
	}

	function value(field: CredentialsField): unknown {
		const typed = String(values[field.key] ?? '');
		const text = typed.trim();
		switch (field.kind) {
			case 'boolean':
				return checked[field.key] ?? field.checked ?? false;
			case 'number':
				return text === '' ? undefined : Number(text);
			case 'list': {
				const items = text.split('\n').map((item) => item.trim()).filter(Boolean);
				return items.length === 0 ? undefined : items;
			}
			case 'secret':
				return typed === '' ? undefined : typed;
			default:
				return text === '' ? undefined : text;
		}
	}

	let missing = $derived(
		form?.fields.filter((field) => field.required && value(field) === undefined) ?? []
	);
	let invalid = $derived(
		form?.fields.filter((field) => {
			const typed = value(field);
			return field.kind === 'url' && typeof typed === 'string' && !isUrl(typed);
		}) ?? []
	);

	function content(): string {
		if (other || upload) return file;
		const document: Record<string, unknown> = {};
		for (const field of form?.fields ?? []) {
			const found = value(field);
			if (found === undefined) continue;
			const keys = field.key.split('.');
			let node = document;
			for (const key of keys.slice(0, -1)) {
				node[key] ??= {};
				node = node[key] as Record<string, unknown>;
			}
			node[keys[keys.length - 1]] = found;
		}
		return JSON.stringify(document, null, 2);
	}

	let ready = $derived(
		(other || upload ? file.trim() !== '' : missing.length === 0 && invalid.length === 0) && !saving
	);

	async function read(event: Event) {
		const picked = (event.currentTarget as HTMLInputElement).files?.[0];
		file = picked ? await picked.text() : '';
	}

	async function save() {
		saving = true;
		failure = null;
		try {
			const encrypted = (
				await encryptCredential({
					body: { name, content: content(), connector: connector.name, form: other ? undefined : chosen }
				})
			).data;
			const change = await proposeFile(
				{ path: encrypted.path, content: encrypted.content ?? null },
				await authoringTargets(),
				{
					title: found
						? m.credentials_change_title_replace({ name })
						: m.credentials_change_title_add({ name })
				}
			);
			await goto(`${base}/changes/${encodeURIComponent(change.id)}`);
		} catch (err) {
			failure = errorMessage(err, m.credentials_add_failed());
		} finally {
			saving = false;
		}
	}
</script>

<div class="space-y-3">
	{#if open}
		{#if forms.length > 1}
			<label class="block space-y-1">
				<span class="text-xs text-base-content/60">{m.credentials_way()}</span>
				<select
					class="select select-sm w-full border border-base-300 bg-base-100 outline-none focus:border-primary"
					bind:value={chosen}
					onchange={reset}
				>
					{#each forms as option, index (option.label)}
						<option value={index}>{option.label}</option>
					{/each}
				</select>
			</label>
		{/if}

		{#if other}
			<div class="h-48 overflow-hidden rounded-md border border-base-300">
				<CodeEditor bind:value={file} extension="json" label={m.credentials_content()} />
			</div>
		{:else if upload}
			<label class="block space-y-1">
				{#if labelled(undefined)}
					<span class="text-xs text-base-content/60">{form?.label}</span>
				{/if}
				<input
					type="file"
					aria-label={form?.label}
					accept="application/json,.json"
					class="file-input file-input-sm w-full border border-base-300 bg-base-100 outline-none focus:border-primary"
					onchange={read}
				/>
			</label>
		{:else}
			{#each form?.fields ?? [] as field (field.key)}
				{#if field.kind === 'boolean'}
					<label class="flex items-center gap-2 text-sm">
						<input type="checkbox" class="checkbox checkbox-sm checkbox-primary" bind:checked={checked[field.key]} />
						{field.label}
					</label>
				{:else}
					<label class="block space-y-1">
						{#if labelled(field.label)}
							<span class="text-xs text-base-content/60">
								{field.label}{field.required ? '' : ` · ${m.credentials_optional()}`}
							</span>
						{/if}
						{#if field.kind === 'choice'}
							<select
								class="select select-sm w-full border border-base-300 bg-base-100 outline-none focus:border-primary"
								aria-label={field.label}
								bind:value={values[field.key]}
							>
								{#if !field.required}
									<option value="">{m.credentials_default()}</option>
								{/if}
								{#each field.choices ?? [] as choice (choice)}
									<option value={choice}>{choice}</option>
								{/each}
							</select>
						{:else if field.kind === 'list'}
							<textarea
								class="textarea textarea-sm w-full border border-base-300 bg-base-100 font-mono outline-none focus:border-primary"
								rows="3"
								aria-label={field.label}
								bind:value={values[field.key]}
							></textarea>
						{:else}
							<span class={INPUT}>
								<input
									type={inputType(field)}
									aria-label={field.label}
									autocomplete="off"
									spellcheck="false"
									bind:value={values[field.key]}
								/>
							</span>
							{#if invalid.includes(field)}
								<span class="block text-xs text-error">{m.credentials_invalid_url()}</span>
							{/if}
						{/if}
					</label>
				{/if}
			{/each}
		{/if}

		{#if failure}
			<p class="text-sm text-error">{failure}</p>
		{/if}
		<div class="flex justify-end gap-2">
			<button
				type="button"
				class="btn btn-ghost btn-sm"
				disabled={saving}
				onclick={() => {
					reset();
					open = false;
				}}
			>
				{m.common_cancel()}
			</button>
			<button type="button" class="btn btn-primary btn-sm gap-1.5" disabled={!ready} onclick={save}>
				<Icon icon="tabler:lock" class="h-4 w-4" />
				{m.common_save()}
			</button>
		</div>
	{:else}
		<button
			type="button"
			class={found ? 'btn btn-ghost btn-sm -ml-3 gap-1.5' : 'btn btn-primary btn-sm'}
			onclick={() => {
				reset();
				open = true;
			}}
		>
			{#if found}
				<Icon icon="tabler:replace" class="h-3.5 w-3.5" />
			{/if}
			{found ? m.credentials_replace() : m.credentials_add()}
		</button>
	{/if}
</div>
