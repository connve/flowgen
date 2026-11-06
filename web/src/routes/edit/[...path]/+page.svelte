<script lang="ts">
	import { base } from '$app/paths';
	import { afterNavigate, beforeNavigate, goto } from '$app/navigation';
	import { onDestroy } from 'svelte';
	import Icon from '@iconify/svelte';
	import { load } from 'js-yaml';
	import FlowInspector from '$lib/flow/FlowInspector.svelte';
	import FlowDetails from '$lib/flow/FlowDetails.svelte';
	import type { Activity } from '$lib/flow/ActivityDetails.svelte';
	import CodeEditor from '$lib/CodeEditor.svelte';
	import StatusDot from '$lib/StatusDot.svelte';
	import { activitiesFor, releaseFlowSubscription } from '$lib/activityStore.svelte';
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
	import { pageTitle } from '$lib/pageTitle';
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
	let resource = $state<string | null>(null);
	let activity = $state<Activity | null>(null);
	let activities = $derived(
		data.file?.startsWith('flows/') && !isNew
			? activitiesFor(stripFlowExtension(data.file.slice('flows/'.length)))
			: undefined
	);
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

	onDestroy(() => {
		releaseFlowSubscription();
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
	<title>{pageTitle(label || leafName || placeholder, isFlow ? m.nav_flows() : m.nav_resources())}</title>
</svelte:head>

<section class="flex h-[calc(100vh-4rem)] min-w-0">
<div class="flex min-w-0 flex-1 flex-col p-6">
	<div class="mb-4 shrink-0">
		<div class="flex min-h-8 items-center justify-between gap-2">
			<div class="min-w-0">
				<div class="flex items-center gap-1 text-xs text-base-content/50">
					{#if isFlow}
						<a href="{base}/" class="hover:text-base-content">{m.editor_flows_breadcrumb()}</a>
					{:else}
						<a href="{base}/resources" class="hover:text-base-content">{m.resources_breadcrumb()}</a>
					{/if}
					{#each segments.slice(0, -1) as segment, i (i)}
						<span class="opacity-60">/</span>
						<span>{segment}</span>
					{/each}
				</div>
				<h1 class="truncate text-[0.9375rem] font-semibold">
					{#if isFlow && label}
						{label}
					{:else if leafName}
						{leafName}
					{:else}
						<span class="font-normal text-base-content/50">{placeholder}</span>
					{/if}
				</h1>
			</div>
			<div class="flex items-center gap-2">
				{#if issues !== null}
					<span class="mr-2 text-sm text-base-content/70">
						<StatusDot
							variant={issues.length === 0 ? 'success' : 'error'}
							label={issues.length === 0 ? m.editor_valid() : m.editor_issue_count({ count: issues.length })}
						/>
					</span>
				{/if}
				<button type="button" class="btn btn-ghost btn-sm" disabled={validating} onclick={validate}>
					{#if validating}
						<span class="loading loading-spinner loading-xs"></span>
					{/if}
					{m.common_validate()}
				</button>
				<a href={closeHref} class="btn btn-ghost btn-sm">{m.common_cancel()}</a>
				<div class="tooltip tooltip-bottom" data-tip={unchanged ? m.editor_nothing_to_save() : undefined}>
					<button
						type="button"
						class="btn btn-primary btn-sm"
						disabled={proposing || validating || unchanged}
						onclick={save}
					>
						{#if proposing}
							<span class="loading loading-spinner loading-xs"></span>
						{/if}
						{m.common_save()}
					</button>
				</div>
				{#if !isNew && data.file}
					{@const file = data.file}
					<div class="dropdown dropdown-end">
						<button
							type="button"
							tabindex="0"
							class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content"
							aria-label={m.editor_more_actions()}
						>
							<Icon icon="tabler:dots" class="h-5 w-5" />
						</button>
						<ul
							tabindex="-1"
							class="menu dropdown-content z-10 mt-1 w-56 rounded-md border border-base-200 bg-base-100 p-1 text-sm shadow-lg"
						>
							<li class={canDelete ? '' : 'menu-disabled'}>
								<button
									type="button"
									class="text-error"
									disabled={proposing || !canDelete}
									onclick={() => remove(file)}
								>
									<Icon icon="tabler:trash" class="h-4 w-4" />
									{m.common_delete()}
								</button>
								{#if !canDelete}
									<span class="text-xs text-base-content/50">{m.editor_delete_read_only()}</span>
								{/if}
							</li>
						</ul>
					</div>
				{/if}
			</div>
		</div>
	</div>

	<div class="mb-3 flex shrink-0 items-center gap-2">
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

	<div class="flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100">
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
				<FlowInspector bind:yaml={content} {activities} editable bind:resource bind:activity />
			{:else}
				<div class="flex h-10 shrink-0 items-center px-4 text-xs font-medium text-base-content/50">
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
</div>
<FlowDetails bind:resource bind:activity />
</section>

{#if pendingUrl}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-4"
		role="dialog"
		aria-modal="true"
		aria-label={m.editor_discard_label()}
		tabindex="-1"
		onclick={(e) => {
			if (e.target === e.currentTarget) pendingUrl = null;
		}}
		onkeydown={(e) => {
			if (e.key !== 'Escape') return;
			e.stopPropagation();
			pendingUrl = null;
		}}
	>
		<div class="w-full max-w-sm space-y-4 rounded-xl bg-base-100 p-5 shadow-2xl">
			<h2 class="text-[0.9375rem] font-semibold">{m.editor_discard_title()}</h2>
			<p class="text-sm text-base-content/70">
				{m.editor_discard_message({ path })}
			</p>
			<div class="flex justify-end gap-2">
				<button
					type="button"
					class="btn btn-ghost btn-sm"
					bind:this={keepEditingButton}
					onclick={() => (pendingUrl = null)}
				>
					{m.editor_keep_editing()}
				</button>
				<button type="button" class="btn btn-error btn-sm" onclick={discard}>
					{m.editor_discard()}
				</button>
			</div>
		</div>
	</div>
{/if}
