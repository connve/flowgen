<script lang="ts">
	import { base } from '$app/paths';
	import { afterNavigate, goto, invalidate } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import Actor from '$lib/Actor.svelte';
	import { IconButton, Inspector, InspectorSection, StateMessage, StatusDot } from '@connve/ui';
	import {
		approveChange,
		editChange,
		errorMessage,
		getWorkspaceFile,
		isStatus,
		proposeChange,
		rejectChange,
		type Change,
		type ChangeSummary
	} from '$lib/api';
	import { deploymentsStream } from '$lib/urls';
	import { auth } from '$lib/auth.svelte';
	import { actor, editUrl, ownLabel } from '$lib/changes';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';
	import { MINUTE, formatAbsolute, formatRelative } from '$lib/time';
	import type { PageProps } from './$types';

	// When a change waiting for deployment counts as slow.
	const DEPLOYMENT_SLOW_MS = 5 * MINUTE;

	let { data }: PageProps = $props();

	let previewedId = $state<string | null>(null);
	let previewed = $derived(data.changes.find((c) => c.id === previewedId) ?? null);
	let deciding = $state(false);
	let decideError = $state<string | null>(null);
	let now = $state(Date.now());

	// Pending and failed changes can be approved, and one left committing past
	// the commit timeout can be retried.
	let canApprove = $derived(
		data.change != null &&
			(data.change.status === 'pending' ||
				data.change.status === 'failed' ||
				data.change.stalled === true)
	);
	// Only the proposer edits a pending change; without sign-in, anyone.
	let canEdit = $derived(
		data.change != null &&
			data.change.status === 'pending' &&
			(!auth.enabled || data.change.proposedBy === ownLabel())
	);
	let editing = $state(false);
	let titleDraft = $state('');
	let descriptionDraft = $state('');
	let savingEdit = $state(false);
	let editError = $state<string | null>(null);
	let pendingCount = $derived(data.changes.filter((c) => c.status === 'pending').length);
	const onTheWay = (deployment: ChangeSummary['deployment']) =>
		deployment === 'waiting' || deployment === 'released';
	let deploying = $derived(
		data.change ? onTheWay(data.change.deployment) : data.changes.some((c) => onTheWay(c.deployment))
	);

	$effect(() => {
		if (!deploying) return;
		const deployments = new EventSource(deploymentsStream());
		deployments.addEventListener('open', () => void invalidate('app:changes'));
		deployments.addEventListener('deployment', () => void invalidate('app:changes'));
		const clock = setInterval(() => (now = Date.now()), MINUTE);
		return () => {
			deployments.close();
			clearInterval(clock);
		};
	});

	afterNavigate(() => {
		decideError = null;
		editing = false;
	});

	function startEdit(change: Change) {
		titleDraft = change.title;
		descriptionDraft = change.description ?? '';
		editError = null;
		editing = true;
	}

	async function saveEdit(id: string) {
		savingEdit = true;
		editError = null;
		try {
			await editChange({ path: { id }, body: { title: titleDraft, description: descriptionDraft } });
			editing = false;
			await invalidate('app:changes');
		} catch (err) {
			editError = errorMessage(err, m.change_edit_failed());
		} finally {
			savingEdit = false;
		}
	}

	let reverting = $state(false);
	let revertError = $state<string | null>(null);

	// A committed change is reverted by a new change restoring what each file
	// held before it; a file changed since cannot be restored without losing
	// that later edit, so the revert names it and stops.
	async function revert(change: Change) {
		reverting = true;
		revertError = null;
		try {
			const current = await Promise.all(
				change.files.map(async (file) => {
					const { data } = await getWorkspaceFile({ query: { path: file.path } });
					return data.content ?? null;
				})
			);
			const changed = change.files
				.filter((file, i) => (file.content ?? null) !== current[i])
				.map((file) => file.path);
			if (changed.length > 0) {
				revertError = m.change_revert_changed_since({ paths: changed.join(', ') });
				return;
			}
			const { data: created } = await proposeChange({
				body: {
					title: m.change_revert_title({ title: change.title }),
					files: change.files.map((file) => ({ path: file.path, content: file.previous ?? null }))
				}
			});
			await goto(`${base}/changes/${encodeURIComponent(created.id)}`);
		} catch (err) {
			revertError = errorMessage(err, m.change_revert_failed());
		} finally {
			reverting = false;
		}
	}

	// Approving sends the version on screen; when it was edited since, the
	// server refuses and the page reloads the change.
	async function decide(id: string, action: 'approve' | 'reject') {
		editing = false;
		deciding = true;
		decideError = null;
		const fallback = action === 'approve' ? m.change_approve_failed() : m.change_reject_failed();
		const updatedAt = data.change?.updatedAt ?? undefined;
		try {
			await (action === 'approve'
				? approveChange({ path: { id }, query: { updatedAt } })
				: rejectChange({ path: { id } }));
			await invalidate('app:changes');
		} catch (err) {
			if (isStatus(err, 409)) await invalidate('app:changes');
			decideError = errorMessage(err, fallback);
		} finally {
			deciding = false;
		}
	}

	type Variant = 'neutral' | 'success' | 'error' | 'warning';

	// One status for the whole lifecycle: a tracked committed change shows how far it got.
	function badge(change: Pick<ChangeSummary, 'status' | 'deployment'>): { label: string; variant: Variant } {
		switch (change.deployment) {
			case 'waiting':
				return { label: m.change_deployment_waiting(), variant: 'warning' };
			case 'released':
				return { label: m.change_deployment_released(), variant: 'warning' };
			case 'deployed':
				return { label: m.change_deployment_deployed(), variant: 'success' };
		}
		switch (change.status) {
			case 'pending':
				return { label: m.change_status_pending(), variant: 'warning' };
			case 'committing':
				return { label: m.change_status_committing(), variant: 'warning' };
			case 'committed':
				return { label: m.change_status_committed(), variant: 'success' };
			case 'failed':
				return { label: m.change_status_failed(), variant: 'error' };
			case 'rejected':
				return { label: m.change_status_rejected(), variant: 'neutral' };
		}
	}

	function lineClass(line: string): string {
		if (line.startsWith('+++') || line.startsWith('---')) return 'opacity-60';
		if (line.startsWith('@@')) return 'text-info opacity-80';
		if (line.startsWith('+')) return 'bg-primary/10 text-primary';
		if (line.startsWith('-')) return 'bg-error/10 text-error';
		return '';
	}
</script>

<svelte:head>
	<title>{pageTitle(data.change?.title, m.changes_breadcrumb())}</title>
</svelte:head>

<div class="flex h-[calc(100vh-4rem)] min-w-0">
<div class="flex min-h-0 min-w-0 flex-1 flex-col">
	<div class="shrink-0 bg-base-100 px-6 pb-4 pt-6">
		<div class="flex items-baseline gap-1.5 text-[0.9375rem] font-semibold">
			{#if data.change}
				<a href="{base}/changes" class="text-xs font-normal text-base-content/50 transition-colors hover:text-base-content"
					>{m.changes_breadcrumb()}</a
				>
			{:else}
				<span>{m.changes_breadcrumb()}</span>
				<span class="text-xs font-normal opacity-50">· {m.changes_pending_count({ count: pendingCount })}</span>
			{/if}
		</div>
	</div>

	<div class="flex min-h-0 flex-1 flex-col overflow-y-auto px-6 pb-6">
		{#if data.change}
			{@const change = data.change}
			{@const shown = badge(change)}
			{@const proposer = actor(change.proposedBy)}
			<div class="space-y-4">
				<div class="flex flex-wrap items-start justify-between gap-3">
					<div class="min-w-0 flex-1 space-y-1">
						{#if editing}
							<form
								class="max-w-3xl space-y-2"
								onsubmit={(e) => {
									e.preventDefault();
									saveEdit(change.id);
								}}
							>
								<label
									class="input input-sm flex w-full items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary"
								>
									<input
										type="text"
										aria-label={m.change_title_label()}
										placeholder={m.change_title_label()}
										bind:value={titleDraft}
										required
									/>
								</label>
								<textarea
									class="textarea textarea-sm w-full border border-base-300 bg-base-100 outline-none focus:border-primary"
									rows="3"
									aria-label={m.change_description_label()}
									placeholder={m.change_description_label()}
									bind:value={descriptionDraft}
								></textarea>
								{#if editError}
									<div class="text-sm text-error">{editError}</div>
								{/if}
								<div class="flex items-center justify-end gap-2">
									<button
										type="button"
										class="btn btn-ghost btn-sm"
										disabled={savingEdit}
										onclick={() => (editing = false)}
									>
										{m.common_cancel()}
									</button>
									<button type="submit" class="btn btn-primary btn-sm" disabled={savingEdit}>
										{m.common_save()}
									</button>
								</div>
							</form>
						{:else}
							<div class="flex items-center gap-2">
								<h1 class="text-[0.9375rem] font-semibold">{change.title}</h1>
								<span class="text-sm font-normal"><StatusDot variant={shown.variant} label={shown.label} /></span>
								{#if canEdit}
									<div class="tooltip tooltip-bottom" data-tip={m.change_edit_details()}>
										<button
											type="button"
											class="btn btn-ghost btn-xs btn-circle"
											aria-label={m.change_edit_details()}
											onclick={() => startEdit(change)}
										>
											<Icon icon="tabler:pencil" class="h-4 w-4" />
										</button>
									</div>
								{/if}
							</div>
						{/if}
						<div class="flex flex-wrap items-center gap-1 text-xs opacity-60">
							{#if proposer?.agent}
								<Icon icon="tabler:robot" class="h-3.5 w-3.5" aria-label={m.change_actor_agent()} />
							{/if}
							{proposer
								? m.change_proposed_by({ name: proposer.name, date: formatAbsolute(change.createdAt) })
								: m.change_proposed_at({ date: formatAbsolute(change.createdAt) })}
							{#if change.decidedBy && change.decidedAt}
								{@const decider = actor(change.decidedBy)}
								{@const when = formatRelative(change.decidedAt)}
								· {change.status === 'rejected'
									? decider
										? m.change_rejected_by({ name: decider.name, when })
										: m.change_rejected({ when })
									: decider
										? m.change_approved_by({ name: decider.name, when })
										: m.change_approved({ when })}
							{/if}
						</div>
						{#if change.description && !editing}
							<p class="max-w-3xl whitespace-pre-line text-sm">{change.description}</p>
						{/if}
					</div>
					{#if canApprove}
						<div class="flex items-center gap-2">
							{#if change.status !== 'committing'}
								<button
									type="button"
									class="btn btn-ghost btn-sm"
									disabled={deciding}
									onclick={() => decide(change.id, 'reject')}
								>
									{m.change_reject()}
								</button>
							{/if}
							<div
								class="tooltip tooltip-bottom"
								data-tip={change.issues.length > 0 ? m.change_approve_blocked() : undefined}
							>
								<button
									type="button"
									class="btn btn-primary btn-sm"
									disabled={deciding || change.issues.length > 0}
									onclick={() => decide(change.id, 'approve')}
								>
									{#if deciding}
										<span class="loading loading-spinner loading-xs"></span>
									{/if}
									{change.status === 'pending' ? m.change_approve() : m.common_retry()}
								</button>
							</div>
						</div>
					{:else if change.status === 'committed'}
						<button
							type="button"
							class="btn btn-ghost btn-sm gap-1.5"
							disabled={reverting}
							onclick={() => revert(change)}
						>
							{#if reverting}
								<span class="loading loading-spinner loading-xs"></span>
							{:else}
								<Icon icon="tabler:arrow-back-up" class="h-4 w-4" />
							{/if}
							{m.change_revert()}
						</button>
					{/if}
				</div>

				{#if revertError}
					<div class="rounded-lg border border-error/40 bg-error/5 p-3 text-sm">{revertError}</div>
				{/if}

				{#if decideError}
					<div class="rounded-lg border border-error/40 bg-error/5 p-3 text-sm">{decideError}</div>
				{/if}
				{#if change.error}
					<div class="flex items-start gap-2 rounded-lg border border-error/40 bg-error/5 p-3 text-sm">
						<Icon icon="tabler:alert-triangle" class="h-5 w-5 shrink-0 text-error" />
						<span>{m.change_commit_failed({ error: change.error })}</span>
					</div>
				{/if}
				{#if onTheWay(change.deployment) && change.decidedAt && now - change.decidedAt > DEPLOYMENT_SLOW_MS}
					<div class="flex items-center gap-2 rounded-lg border border-warning/40 bg-warning/5 p-3 text-sm">
						<Icon icon="tabler:clock-exclamation" class="h-5 w-5 shrink-0 text-attention" />
						<span>{m.change_deployment_slow()}</span>
						<a href="{base}/monitor/logs" class="ml-auto whitespace-nowrap text-primary hover:underline">
							{m.change_open_logs()}
						</a>
					</div>
				{/if}
				{#if change.status === 'committed' && Object.keys(change.result ?? {}).length > 0}
					<div class="rounded-lg bg-base-200/50 p-3 text-xs">
						{#each Object.entries(change.result ?? {}) as [key, value] (key)}
							<div class="flex gap-2">
								<span class="w-24 shrink-0 opacity-60">{key}</span>
								<span class="break-all font-mono">{typeof value === 'string' ? value : JSON.stringify(value)}</span>
							</div>
						{/each}
					</div>
				{/if}
				{#if change.issues.length > 0}
					<div class="rounded-lg border border-error/40 bg-error/5 p-3 text-sm">
						<div class="mb-1 font-medium text-error">{m.change_validation_issues()}</div>
						<ul class="space-y-0.5">
							{#each change.issues as issue, i (i)}
								<li class="font-mono text-xs">
									{issue.path}{issue.location ? ` · ${issue.location}` : ''}: {issue.message}
								</li>
							{/each}
						</ul>
					</div>
				{/if}

				{#each change.files as file (file.path)}
					<div class="overflow-hidden rounded-lg border border-base-200">
						<div class="flex items-center gap-2 px-3 py-2">
							<Icon
								icon={file.content == null ? 'tabler:file-minus' : file.previous == null ? 'tabler:file-plus' : 'tabler:file-diff'}
								class="h-4 w-4 opacity-70"
							/>
							<span class="font-mono text-xs">{file.path}</span>
							{#if file.content == null}
								<span class="text-xs text-error">{m.change_file_deleted()}</span>
							{:else if file.previous == null}
								<span class="text-xs text-primary">{m.change_file_new()}</span>
							{/if}
							{#if canEdit && file.content != null}
								<a
									href="{editUrl(base, file.path)}?change={encodeURIComponent(change.id)}"
									class="btn btn-ghost btn-xs ml-auto gap-1"
								>
									<Icon icon="tabler:pencil" class="h-3.5 w-3.5" />
									{m.common_edit()}
								</a>
							{/if}
						</div>
						<pre class="overflow-x-auto text-xs leading-5">{#each file.diff.split('\n') as line, i (i)}<div class="px-3 {lineClass(line)}">{line || ' '}</div>{/each}</pre>
					</div>
				{/each}
			</div>
		{:else if data.changes.length === 0}
			<StateMessage
				tone="notice"
				title={m.changes_empty_title()}
				message={m.changes_empty_message()}
			/>
		{:else}
			<div class="-mx-6 shrink-0 overflow-x-auto px-6">
				<table class="table table-sm w-full bg-base-100">
					<thead class="bg-base-100 text-xs font-medium text-base-content/50">
						<tr>
							<th>{m.changes_column_title()}</th>
							<th>{m.changes_column_status()}</th>
							{#if auth.enabled}
								<th>{m.changes_column_proposed_by()}</th>
							{/if}
							<th>{m.changes_column_files()}</th>
							<th>{m.changes_column_created()}</th>
						</tr>
					</thead>
					<tbody>
						{#each data.changes as item (item.id)}
							{@const shown = badge(item)}
							<tr
								class="cursor-pointer transition-colors hover:bg-base-200 {previewedId === item.id
									? 'bg-base-200'
									: ''}"
								onclick={() => (previewedId = item.id)}
								ondblclick={() => goto(`${base}/changes/${encodeURIComponent(item.id)}`)}
								onkeydown={(e) => {
									if (e.key === 'Enter' || e.key === ' ') {
										e.preventDefault();
										previewedId = item.id;
									}
								}}
								tabindex="0"
								role="button"
							>
								<td class="w-full max-w-0">
									<div class="truncate font-medium">{item.title}</div>
								</td>
								<td><StatusDot variant={shown.variant} label={shown.label} /></td>
								{#if auth.enabled}
									<td class="whitespace-nowrap text-sm opacity-70"><Actor label={item.proposedBy} /></td>
								{/if}
								<td class="text-sm tabular-nums opacity-70">{item.fileCount}</td>
								<td class="whitespace-nowrap text-sm opacity-70">
									{formatRelative(item.createdAt)}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	</div>
</div>

{#if previewed && !data.change}
	{@const shown = badge(previewed)}
	{@const href = `${base}/changes/${encodeURIComponent(previewed.id)}`}
	<Inspector title={previewed.title} label={m.change_details()} onclose={() => (previewedId = null)}>
		{#snippet subtitle()}
			<StatusDot variant={shown.variant} label={shown.label} />
		{/snippet}
		{#snippet actions()}
			<IconButton icon="tabler:arrow-up-right" label={m.change_open()} {href} />
		{/snippet}
		<dl class="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1">
			{#if auth.enabled}
				<dt class="text-base-content/50">{m.changes_column_proposed_by()}</dt>
				<dd><Actor label={previewed.proposedBy} /></dd>
			{/if}
			<dt class="text-base-content/50">{m.changes_column_created()}</dt>
			<dd>{formatAbsolute(previewed.createdAt)}</dd>
			<dt class="text-base-content/50">{m.change_field_target()}</dt>
			<dd>{previewed.target}</dd>
		</dl>
		<InspectorSection title={m.changes_column_files()}>
			<ul class="space-y-1">
				{#each previewed.paths as path (path)}
					<li class="[overflow-wrap:anywhere]">{path}</li>
				{/each}
			</ul>
		</InspectorSection>
	</Inspector>
{/if}
</div>
