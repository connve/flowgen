<script lang="ts">
	import { base } from '$app/paths';
	import { afterNavigate, beforeNavigate, goto } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import { load } from 'js-yaml';
	import Badge from '$lib/Badge.svelte';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import CodeEditor from '$lib/CodeEditor.svelte';
	import { apiUrl, encodePath, type AuthoringTarget, type ValidationIssue as Issue } from '$lib/api';
	import { stripFlowExtension, targetOf } from '$lib/changes';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const NEW_FLOW = `flow:
  labels:
    display_name: ""
    description: ""
  tasks:
    - generate:
        name: tick
        interval: 1m

    - log:
        name: print
`;

	// What the file opened with; edits beyond it are unsaved. A new object on
	// every load, so `path` and `content` reset even when the values repeat.
	let opened = $derived({
		path: data.file ?? newFilePath(data.targets, data.kind, data.folder),
		content: data.file ? (data.previous ?? '') : data.kind === 'resource' ? '' : NEW_FLOW
	});
	let path = $derived(opened.path);
	let content = $derived(opened.content);

	let validating = $state(false);
	let proposing = $state(false);
	let validation = $state<{ path: string; content: string; issues: Issue[] } | null>(null);
	let pendingUrl = $state<URL | null>(null);
	let keepEditingButton = $state<HTMLButtonElement | null>(null);
	let leaving = false;

	// Results describe the file they were run on, so any edit hides them.
	let issues = $derived(
		validation && validation.path === path && validation.content === content
			? validation.issues
			: null
	);
	let dirty = $derived(path !== opened.path || content !== opened.content);
	let isNew = $derived(data.file === null || data.previous === null);
	// Saving under another path adds a file there; the opened one stays.
	let updatesOpened = $derived(!isNew && path === data.file);
	let unchanged = $derived(updatesOpened && content === data.previous);
	let target = $derived(targetOf(data.targets, path));
	let isFlow = $derived(path.startsWith('flows/'));
	let segments = $derived.by(() => {
		const relative = path.replace(/^(flows|resources)\//, '');
		const name = isFlow ? stripFlowExtension(relative) : relative;
		return name.split('/').filter((segment) => segment !== '');
	});
	let leafName = $derived(segments[segments.length - 1] ?? '');
	let closeHref = $derived.by(() => {
		if (data.file === null) return data.kind === 'resource' ? `${base}/resources` : `${base}/`;
		if (data.file.startsWith('flows/')) {
			return `${base}/flows/${encodePath(stripFlowExtension(data.file.slice('flows/'.length)))}`;
		}
		return `${base}/resources/${encodePath(data.file.slice('resources/'.length))}`;
	});
	let label = $derived(isFlow ? displayName(content) : null);
	let placeholder = $derived(isFlow ? 'New flow' : 'New resource');

	// The safe answer has focus, so Enter keeps the edits.
	$effect(() => {
		keepEditingButton?.focus();
	});

	function displayName(yaml: string): string | null {
		try {
			const name = (load(yaml) as { flow?: { labels?: { display_name?: unknown } } } | null)?.flow
				?.labels?.display_name;
			return typeof name === 'string' ? name : null;
		} catch {
			return null;
		}
	}

	// Where a new file starts: the given folder, else the first target's folder.
	function newFilePath(
		targets: AuthoringTarget[],
		kind: 'flow' | 'resource',
		folder: string | null
	): string {
		const dir = kind === 'resource' ? 'resources/' : 'flows/';
		if (folder) return `${dir}${folder}/`;
		const first = targets[0];
		return first ? startPath(first, dir) : dir;
	}

	// Where a new file starts in `target`: its first prefix of the same kind.
	function startPath(target: AuthoringTarget, current: string): string {
		const dir = current.startsWith('resources/') ? 'resources/' : 'flows/';
		return target.paths.find((prefix) => prefix.startsWith(dir)) ?? target.paths[0] ?? current;
	}

	afterNavigate(() => {
		leaving = false;
	});

	// Unsaved edits hold any navigation until the user discards them; closing
	// the tab falls back to the browser's own prompt.
	beforeNavigate((navigation) => {
		if (leaving || !dirty) return;
		navigation.cancel();
		if (navigation.type !== 'leave' && navigation.to) pendingUrl = navigation.to.url;
	});

	function discard() {
		const url = pendingUrl;
		pendingUrl = null;
		if (!url) return;
		leaving = true;
		if (url.origin === location.origin) goto(url);
		else location.href = url.href;
	}

	// Returns whether the file has no issues.
	async function validate(): Promise<boolean> {
		const checked = { path, content };
		validating = true;
		let found: Issue[] = [];
		if (data.targets.length > 0 && !target) {
			found.push({ path: checked.path, message: 'No authoring target covers this path' });
		}
		if (checked.content.trim() === '') {
			found.push({ path: checked.path, message: 'The file is empty' });
		}
		try {
			const res = await fetch(apiUrl('api/workspace/validate'), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ files: [checked] })
			});
			if (!res.ok) throw new Error((await res.text()) || `HTTP ${res.status}`);
			found = [...found, ...((await res.json()).issues ?? [])];
		} catch (err) {
			const message = err instanceof Error ? err.message : 'Validation failed';
			found = [...found, { path: checked.path, message }];
		} finally {
			validating = false;
		}
		validation = { ...checked, issues: found };
		return found.length === 0;
	}

	async function save() {
		const saved = { path, content };
		const title = `${updatesOpened ? 'Update' : 'Add'} ${path}`;
		if (!(await validate())) return;
		await propose(title, saved, saved);
	}

	async function remove(file: string) {
		await propose(`Delete ${file}`, { path: file, content: null }, { path, content });
	}

	// A failure shows against the edits it was made from.
	async function propose(
		title: string,
		file: { path: string; content: string | null },
		from: { path: string; content: string }
	) {
		proposing = true;
		try {
			const res = await fetch(apiUrl('api/changes'), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ title, files: [file] })
			});
			if (!res.ok) throw new Error((await res.text()) || `HTTP ${res.status}`);
			const change = await res.json();
			leaving = true;
			goto(`${base}/changes/${encodeURIComponent(change.id)}`);
		} catch (err) {
			const message = err instanceof Error ? err.message : 'Failed to save';
			validation = { ...from, issues: [{ path: file.path, message }] };
		} finally {
			proposing = false;
		}
	}
</script>

<svelte:head>
	<title>{label || leafName || placeholder} | Flowgen</title>
</svelte:head>

<section class="p-6">
	<div class="mb-4">
		<div class="flex min-h-8 items-center justify-between gap-2">
			<div class="flex items-center gap-1.5 text-sm">
				{#if isFlow}
					<a href="{base}/" class="text-primary hover:underline">Flows</a>
				{:else}
					<a href="{base}/resources" class="text-primary hover:underline">Resources</a>
				{/if}
				{#each segments.slice(0, -1) as segment, i (i)}
					<span class="opacity-40">/</span>
					<span class="font-mono opacity-70">{segment}</span>
				{/each}
				<span class="opacity-40">/</span>
				{#if leafName}
					<span class="font-mono">{leafName}</span>
				{:else}
					<span>{placeholder}</span>
				{/if}
			</div>
			<div class="flex items-center gap-1">
				{#if issues !== null}
					<span class="mr-2">
						{#if issues.length === 0}
							<Badge variant="success">valid</Badge>
						{:else}
							<Badge variant="error">{issues.length} {issues.length === 1 ? 'issue' : 'issues'}</Badge>
						{/if}
					</span>
				{/if}
				{#if !isNew && data.file}
					{@const file = data.file}
					<div class="tooltip tooltip-bottom" data-tip="Delete">
						<button
							type="button"
							class="btn btn-ghost btn-sm btn-circle text-error"
							aria-label="Delete"
							disabled={proposing || !targetOf(data.targets, file)}
							onclick={() => remove(file)}
						>
							<Icon icon="tabler:trash" class="h-5 w-5" />
						</button>
					</div>
				{/if}
				<div class="tooltip tooltip-bottom" data-tip="Validate">
					<button
						type="button"
						class="btn btn-ghost btn-sm btn-circle"
						aria-label="Validate"
						disabled={validating}
						onclick={validate}
					>
						{#if validating}
							<span class="loading loading-spinner loading-sm"></span>
						{:else}
							<Icon icon="tabler:checklist" class="h-5 w-5" />
						{/if}
					</button>
				</div>
				<div class="tooltip tooltip-bottom" data-tip="Save">
					<button
						type="button"
						class="btn btn-ghost btn-sm btn-circle text-primary"
						aria-label="Save"
						disabled={proposing || validating || unchanged}
						onclick={save}
					>
						{#if proposing}
							<span class="loading loading-spinner loading-sm"></span>
						{:else}
							<Icon icon="tabler:device-floppy" class="h-5 w-5" />
						{/if}
					</button>
				</div>
				<span class="mx-2 h-5 w-px bg-base-300" aria-hidden="true"></span>
				<div class="tooltip tooltip-bottom" data-tip="Exit editor">
					<a href={closeHref} class="btn btn-ghost btn-sm btn-circle" aria-label="Exit editor">
						<Icon icon="tabler:x" class="h-5 w-5" />
					</a>
				</div>
			</div>
		</div>
		{#if isFlow}
			<h1 class="min-h-7 text-lg font-medium">{label ?? ''}</h1>
		{/if}
	</div>

	<div class="mb-3 flex items-center gap-2">
		{#if isNew && data.targets.length > 1}
			<select
				class="select select-sm w-auto border border-base-300 bg-base-100 outline-none focus:border-primary"
				aria-label="Authoring target"
				value={target?.name ?? ''}
				onchange={(e) => {
					const chosen = data.targets.find((t) => t.name === e.currentTarget.value);
					if (chosen) path = startPath(chosen, path);
				}}
			>
				{#each data.targets as option (option.name)}
					<option value={option.name}>{option.name}</option>
				{/each}
			</select>
		{/if}
		<label
			class="input input-sm flex w-full max-w-xl items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary"
		>
			<Icon icon="tabler:file" class="h-4 w-4 opacity-50" />
			<input
				type="text"
				class="font-mono"
				aria-label="Path"
				bind:value={path}
				placeholder="flows/orders/sync.yaml"
			/>
		</label>
	</div>

	<div class="flex h-[calc(100vh-16rem)] flex-col overflow-hidden rounded-lg border border-base-300 bg-base-100">
		{#if issues && issues.length > 0}
			<ul class="max-h-32 shrink-0 space-y-0.5 overflow-auto border-b border-error/40 bg-error/5 px-4 py-2">
				{#each issues as issue, i (i)}
					<li class="font-mono text-xs">
						{issue.location ? `${issue.location}: ` : ''}{issue.message}
					</li>
				{/each}
			</ul>
		{/if}
		{#key data}
			{#if isFlow}
				<FlowInspector bind:yaml={content} editable />
			{:else}
				<div class="flex h-10 shrink-0 items-center border-b border-base-200 px-4 text-xs font-medium opacity-70">
					Content
				</div>
				<CodeEditor
					bind:value={content}
					extension={path.split('.').pop() ?? null}
					label="File content"
				/>
			{/if}
		{/key}
	</div>
</section>

{#if pendingUrl}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
		role="dialog"
		aria-modal="true"
		aria-label="Discard changes"
		tabindex="-1"
		onclick={(e) => {
			if (e.target === e.currentTarget) pendingUrl = null;
		}}
		onkeydown={(e) => {
			if (e.key === 'Escape') pendingUrl = null;
		}}
	>
		<div class="w-full max-w-sm overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-xl">
			<div class="border-b border-base-200 px-4 py-3 text-sm font-medium">Discard changes?</div>
			<p class="px-4 py-3 text-sm opacity-70">
				Your edits to <span class="font-mono">{path}</span> are not saved.
			</p>
			<div class="flex justify-end gap-1 border-t border-base-200 px-3 py-2">
				<button
					type="button"
					class="btn btn-sm border-base-300 bg-base-100"
					bind:this={keepEditingButton}
					onclick={() => (pendingUrl = null)}
				>
					Keep editing
				</button>
				<button type="button" class="btn btn-error btn-soft btn-sm" onclick={discard}>
					Discard
				</button>
			</div>
		</div>
	</div>
{/if}
