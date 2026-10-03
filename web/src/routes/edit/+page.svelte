<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import Icon from '@iconify/svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { onMount, untrack } from 'svelte';
	import { apiUrl, type AuthoringTarget } from '$lib/api';
	import { authoringTargets, deployedContent, targetOf } from '$lib/changes';
	import type { components } from '$lib/api/generated';

	type Issue = components['schemas']['ValidationIssue'];

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

	// `?path=` edits that file; `?new=flow` or `?new=resource` starts a new one.
	let initialPath = $derived(page.url.searchParams.get('path'));
	let kind = $derived(page.url.searchParams.get('new'));

	let path = $state('');
	let content = $state('');
	let previous = $state<string | null>(null);
	let title = $state('');
	let description = $state('');
	let loading = $state(false);
	let loadError = $state<string | null>(null);
	let issues = $state<Issue[] | null>(null);
	let validating = $state(false);
	let proposing = $state(false);
	let proposeError = $state<string | null>(null);

	let isNew = $derived(initialPath === null || previous === null);
	let unchanged = $derived(!isNew && content === previous);
	let targets = $state<AuthoringTarget[]>([]);
	let target = $derived(targetOf(targets, path));

	onMount(() => {
		authoringTargets().then((found) => {
			targets = found;
			if (initialPath === null && (path === 'flows/' || path === 'resources/')) {
				const first = found[0];
				if (first) path = startPath(first, path);
			}
		});
	});

	// Where a new file starts in `target`: its first prefix of the same kind.
	function startPath(target: AuthoringTarget, current: string): string {
		const dir = current.startsWith('resources/') ? 'resources/' : 'flows/';
		return target.paths.find((prefix) => prefix.startsWith(dir)) ?? target.paths[0] ?? current;
	}

	// Resets the form whenever the URL names another file; a response for a
	// file the user already left is dropped.
	$effect(() => {
		const file = initialPath;
		const newKind = kind;
		previous = null;
		issues = null;
		loadError = null;
		proposeError = null;
		description = '';
		if (file === null) {
			const dir = newKind === 'resource' ? 'resources/' : 'flows/';
			const first = untrack(() => targets)[0];
			path = first ? startPath(first, dir) : dir;
			content = newKind === 'resource' ? '' : NEW_FLOW;
			title = '';
			loading = false;
			return;
		}
		path = file;
		content = '';
		loading = true;
		deployedContent(file)
			.then((deployed) => {
				if (file !== initialPath) return;
				previous = deployed;
				content = deployed ?? '';
				title = deployed === null ? `Add ${file}` : `Update ${file}`;
			})
			.catch((err) => {
				if (file === initialPath)
					loadError = err instanceof Error ? err.message : 'Failed to load the file';
			})
			.finally(() => {
				if (file === initialPath) loading = false;
			});
	});

	async function validate() {
		validating = true;
		try {
			const res = await fetch(apiUrl('api/workspace/validate'), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ files: [{ path, content }] })
			});
			if (!res.ok) throw new Error((await res.text()) || `HTTP ${res.status}`);
			issues = (await res.json()).issues ?? [];
		} catch (err) {
			issues = [{ path, message: err instanceof Error ? err.message : 'Validation failed' }];
		} finally {
			validating = false;
		}
	}

	async function propose(remove: boolean) {
		proposing = true;
		proposeError = null;
		try {
			const res = await fetch(apiUrl('api/changes'), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					title: title.trim() || (remove ? `Delete ${path}` : `Update ${path}`),
					description: description.trim() || undefined,
					files: [{ path, content: remove ? null : content }]
				})
			});
			if (!res.ok) throw new Error((await res.text()) || `HTTP ${res.status}`);
			const change = await res.json();
			goto(`${base}/changes/${encodeURIComponent(change.id)}`);
		} catch (err) {
			proposeError = err instanceof Error ? err.message : 'Failed to propose the change';
		} finally {
			proposing = false;
		}
	}
</script>

<svelte:head>
	<title>{isNew ? 'New file' : `Edit ${path}`} | Flowgen</title>
</svelte:head>

<section class="flex min-h-0 flex-1 flex-col gap-4 p-6">
	<div class="flex items-center gap-1.5 text-sm">
		{#if path.startsWith('resources/')}
			<a href="{base}/resources" class="text-primary hover:underline">Resources</a>
		{:else}
			<a href="{base}/" class="text-primary hover:underline">Flows</a>
		{/if}
		<span class="opacity-40">/</span>
		<span>{isNew ? 'New file' : 'Edit'}</span>
	</div>

	{#if loading}
		<div class="flex justify-center py-12">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if loadError}
		<StateMessage tone="oops" title="Failed to load the file" message={loadError} />
	{:else}
		<div class="grid gap-3 md:grid-cols-2">
			<label class="form-control">
				<span class="label-text mb-1 flex items-center gap-2 text-xs opacity-70">
					Path
					{#if target}
						<span class="badge badge-ghost badge-xs">{target.name}</span>
					{:else if targets.length > 0}
						<span class="text-error">not in any authoring target</span>
					{/if}
				</span>
				<div class="flex gap-1">
					{#if isNew && targets.length > 1}
						<select
							class="select select-sm border border-base-300"
							value={target?.name ?? ''}
							onchange={(e) => {
								const chosen = targets.find((t) => t.name === e.currentTarget.value);
								if (chosen) path = startPath(chosen, path);
							}}
						>
							{#each targets as option (option.name)}
								<option value={option.name}>{option.name}</option>
							{/each}
						</select>
					{/if}
					<input
						class="input input-sm flex-1 border border-base-300 font-mono"
						bind:value={path}
						placeholder="flows/orders/sync.yaml"
					/>
				</div>
			</label>
			<label class="form-control">
				<span class="label-text mb-1 text-xs opacity-70">Title</span>
				<input
					class="input input-sm border border-base-300"
					bind:value={title}
					placeholder="What this change does"
				/>
			</label>
		</div>
		<label class="form-control">
			<span class="label-text mb-1 text-xs opacity-70">Description (optional)</span>
			<input class="input input-sm border border-base-300" bind:value={description} />
		</label>

		<textarea
			class="textarea min-h-[24rem] flex-1 border border-base-300 font-mono text-xs leading-5"
			spellcheck="false"
			bind:value={content}
			oninput={() => (issues = null)}
		></textarea>

		{#if issues !== null}
			{#if issues.length === 0}
				<div class="alert alert-success text-sm">
					<Icon icon="tabler:check" class="h-5 w-5" />
					<span>Valid</span>
				</div>
			{:else}
				<div class="rounded-lg border border-error/40 bg-error/5 p-3 text-sm">
					<ul class="space-y-0.5">
						{#each issues as issue, i (i)}
							<li class="font-mono text-xs">
								{issue.location ? `${issue.location}: ` : ''}{issue.message}
							</li>
						{/each}
					</ul>
				</div>
			{/if}
		{/if}
		{#if proposeError}
			<div class="alert alert-error text-sm">{proposeError}</div>
		{/if}

		<div class="flex items-center gap-1">
			{#if !isNew}
				<button
					type="button"
					class="btn btn-ghost btn-sm text-error"
					disabled={proposing || !target}
					onclick={() => propose(true)}
				>
					<Icon icon="tabler:trash" class="h-4 w-4" />
					Propose delete
				</button>
			{/if}
			<div class="flex-1"></div>
			<button type="button" class="btn btn-ghost btn-sm" disabled={validating} onclick={validate}>
				{#if validating}
					<span class="loading loading-spinner loading-xs"></span>
				{:else}
					<Icon icon="tabler:checklist" class="h-4 w-4" />
				{/if}
				Validate
			</button>
			<button
				type="button"
				class="btn btn-primary btn-sm"
				disabled={proposing || unchanged || content.trim() === '' || !target}
				onclick={() => propose(false)}
			>
				{#if proposing}
					<span class="loading loading-spinner loading-xs"></span>
				{:else}
					<Icon icon="tabler:git-pull-request" class="h-4 w-4" />
				{/if}
				Propose change
			</button>
		</div>
	{/if}
</section>
