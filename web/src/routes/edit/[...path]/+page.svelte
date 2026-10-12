<script lang="ts">
	import { base } from '$app/paths';
	import { afterNavigate, beforeNavigate, goto } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import { load } from 'js-yaml';
	import Badge from '$lib/Badge.svelte';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import CodeEditor from '$lib/CodeEditor.svelte';
	import {
		apiUrl,
		encodePath,
		errorMessage,
		failure,
		type AuthoringTarget,
		type Change,
		type ValidationIssue as Issue
	} from '$lib/api';
	import {
		draftChange,
		generatedTitle,
		loadChange,
		stripFlowExtension,
		targetOf
	} from '$lib/changes';
	import { m } from '$lib/paraglide/messages.js';
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
		content: data.file
			? (data.drafted ?? data.previous ?? '')
			: data.kind === 'resource'
				? ''
				: NEW_FLOW
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
	let unchanged = $derived(updatesOpened && content === opened.content);
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
	let placeholder = $derived(isFlow ? m.editor_new_flow() : m.editor_new_resource());
	let canDelete = $derived(data.file !== null && targetOf(data.targets, data.file) !== undefined);

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
			const all = [...new Set(data.targets.flatMap((t) => t.paths))];
			const kind = isFlow ? 'flows/' : 'resources/';
			const same = all.filter((folder) => folder.startsWith(kind));
			const folders = (same.length > 0 ? same : all).join(', ');
			found.push({ path: checked.path, message: m.editor_outside_targets({ folders }) });
		}
		if (checked.content.trim() === '') {
			found.push({ path: checked.path, message: m.editor_empty_file() });
		}
		try {
			const res = await fetch(apiUrl('api/workspace/validate'), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ files: [checked] })
			});
			if (!res.ok) throw await failure(res);
			found = [...found, ...((await res.json()).issues ?? [])];
		} catch (err) {
			const message = errorMessage(err, m.editor_validate_failed());
			found = [...found, { path: checked.path, message }];
		} finally {
			validating = false;
		}
		validation = { ...checked, issues: found };
		return found.length === 0;
	}

	async function save() {
		const saved = { path, content };
		const title = generatedTitle([{ ...saved, previous: updatesOpened ? data.previous : null }]);
		if (!(await validate())) return;
		await propose(title, saved, saved);
	}

	async function remove(file: string) {
		const deleted = { path: file, content: null };
		await propose(generatedTitle([deleted]), deleted, { path, content });
	}

	function send(method: 'POST' | 'PATCH', url: string, body: object): Promise<Response> {
		return fetch(apiUrl(url), {
			method,
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(body)
		});
	}

	// Adds the file to a pending change; null when the change can no longer
	// take it. A title the editor generated follows the files; one a person or
	// agent wrote stays.
	async function addToDraft(
		id: string,
		file: { path: string; content: string | null }
	): Promise<Change | null> {
		const url = `api/changes/${encodeURIComponent(id)}`;
		const before = await loadChange(id);
		const res = await send('POST', `${url}/files`, { files: [file] });
		if (res.status === 403 || res.status === 409) return null;
		if (!res.ok) throw await failure(res);
		const after: Change = await res.json();
		const title = generatedTitle(after.files);
		if (before && before.title === generatedTitle(before.files) && title !== after.title) {
			await send('PATCH', url, { title }).catch(() => undefined);
		}
		return after;
	}

	// Adds the file to the user's pending change in its target, or proposes a
	// new change when there is none or it was decided meanwhile. A failure shows
	// against the edits it was made from.
	async function propose(
		title: string,
		file: { path: string; content: string | null },
		from: { path: string; content: string }
	) {
		proposing = true;
		try {
			const draft = draftChange(data.pending, targetOf(data.targets, file.path)?.name);
			let change = draft ? await addToDraft(draft.id, file) : null;
			if (change === null) {
				const res = await send('POST', 'api/changes', { title, files: [file] });
				if (!res.ok) throw await failure(res);
				change = (await res.json()) as Change;
			}
			leaving = true;
			goto(`${base}/changes/${encodeURIComponent(change.id)}`);
		} catch (err) {
			const message = errorMessage(err, m.editor_save_failed());
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
					<a href="{base}/" class="text-primary hover:underline">{m.editor_flows_breadcrumb()}</a>
				{:else}
					<a href="{base}/resources" class="text-primary hover:underline">{m.resources_breadcrumb()}</a>
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
							<Badge variant="success">{m.editor_valid()}</Badge>
						{:else}
							<Badge variant="error">{m.editor_issue_count({ count: issues.length })}</Badge>
						{/if}
					</span>
				{/if}
				{#if !isNew && data.file}
					{@const file = data.file}
					<div
						class="tooltip tooltip-bottom"
						data-tip={canDelete ? m.common_delete() : m.editor_delete_read_only()}
					>
						<button
							type="button"
							class="btn btn-ghost btn-sm btn-circle text-error"
							aria-label={m.common_delete()}
							disabled={proposing || !canDelete}
							onclick={() => remove(file)}
						>
							<Icon icon="tabler:trash" class="h-5 w-5" />
						</button>
					</div>
				{/if}
				<div class="tooltip tooltip-bottom" data-tip={m.common_validate()}>
					<button
						type="button"
						class="btn btn-ghost btn-sm btn-circle"
						aria-label={m.common_validate()}
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
				<div
					class="tooltip tooltip-bottom"
					data-tip={unchanged ? m.editor_nothing_to_save() : m.common_save()}
				>
					<button
						type="button"
						class="btn btn-ghost btn-sm btn-circle text-primary"
						aria-label={m.common_save()}
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
				<div class="tooltip tooltip-bottom" data-tip={m.editor_exit()}>
					<a href={closeHref} class="btn btn-ghost btn-sm btn-circle" aria-label={m.editor_exit()}>
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
				aria-label={m.editor_target_label()}
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
				aria-label={m.editor_path_label()}
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
					{m.editor_content_heading()}
				</div>
				<CodeEditor
					bind:value={content}
					extension={path.split('.').pop() ?? null}
					label={m.editor_content_label()}
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
		aria-label={m.editor_discard_label()}
		tabindex="-1"
		onclick={(e) => {
			if (e.target === e.currentTarget) pendingUrl = null;
		}}
		onkeydown={(e) => {
			if (e.key === 'Escape') pendingUrl = null;
		}}
	>
		<div class="w-full max-w-sm overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-xl">
			<div class="border-b border-base-200 px-4 py-3 text-sm font-medium">{m.editor_discard_title()}</div>
			<p class="px-4 py-3 text-sm opacity-70">
				{m.editor_discard_message({ path })}
			</p>
			<div class="flex justify-end gap-1 border-t border-base-200 px-3 py-2">
				<button
					type="button"
					class="btn btn-sm border-base-300 bg-base-100"
					bind:this={keepEditingButton}
					onclick={() => (pendingUrl = null)}
				>
					{m.editor_keep_editing()}
				</button>
				<button type="button" class="btn btn-error btn-soft btn-sm" onclick={discard}>
					{m.editor_discard()}
				</button>
			</div>
		</div>
	</div>
{/if}
