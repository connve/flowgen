<script lang="ts">
	import { base } from '$app/paths';
	import { afterNavigate, goto, invalidate } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import Actor from '$lib/Actor.svelte';
	import { IconButton, Inspector, InspectorSection, Segmented, StateMessage, StatusDot } from '@connve/ui';
	import {
		approveChange,
		editChange,
		errorMessage,
		isStatus,
		proposeChange,
		rejectChange,
		type Change,
		type ChangeSummary,
		type TemplateSource
	} from '$lib/api';
	import { deploymentsStream } from '$lib/urls';
	import { auth } from '$lib/auth.svelte';
	import { actor, baseContent, editUrl, ownLabel, removeFromChange, viewUrl } from '$lib/changes';
	import IssueList from '$lib/IssueList.svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import FlowDag from '$lib/dag/FlowDag.svelte';
	import type { Mark } from '$lib/dag/parseFlow';
	import TaskChanges from '$lib/flow/TaskChanges.svelte';
	import { compareFlows, markLabel } from '$lib/changePreview';
	import { changeView } from '$lib/changeView.svelte';
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

	let comparisons = $derived(
		new Map(
			(data.change?.files ?? []).map((file) => [
				file.path,
				file.path.startsWith('flows/') && (file.content != null || file.previous != null)
					? compareFlows(file.previous ?? null, file.content ?? '')
					: null
			])
		)
	);
	let selectedTask = $state<{ path: string; name: string } | null>(null);
	const MARK_VARIANT: Record<Mark, Variant> = { added: 'success', removed: 'error', changed: 'warning' };

	afterNavigate(() => {
		decideError = null;
		editing = false;
		selectedTask = null;
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

	let removing = $state(false);
	let removeError = $state<string | null>(null);

	async function removeFile(id: string, path: string) {
		if (removing) return;
		removing = true;
		removeError = null;
		try {
			const change = await removeFromChange(id, path);
			if (change.status === 'pending') await invalidate('app:changes');
			else await goto(`${base}/changes`);
		} catch (err) {
			removeError = errorMessage(err, m.change_remove_failed());
		} finally {
			removing = false;
		}
	}

	let reverting = $state(false);
	let revertError = $state<string | null>(null);

	// Null when the change updated a template, whose earlier digest is not kept.
	function revertedTemplates(change: Change): TemplateSource[] | null {
		const reverted: TemplateSource[] = [];
		for (const template of change.templates ?? []) {
			if (template.removed) {
				reverted.push({ ...template, removed: false });
				continue;
			}
			const installed = change.files.every(
				(file) => !template.files.includes(file.path) || file.previous == null
			);
			if (!installed) return null;
			reverted.push({ ...template, removed: true });
		}
		return reverted;
	}

	async function changedSince(change: Change): Promise<string[]> {
		const current = await Promise.all(change.files.map((file) => baseContent(file.path)));
		return change.files
			.filter((file, i) => (file.content ?? null) !== current[i])
			.map((file) => file.path);
	}

	let editedLater = $state<string[]>([]);
	$effect(() => {
		const change = data.change;
		editedLater = [];
		if (change?.status !== 'committed') return;
		changedSince(change).then(
			(paths) => {
				if (data.change === change) editedLater = paths;
			},
			() => undefined
		);
	});

	// A committed change is reverted by a new change restoring what each file
	// held before it; a file changed since cannot be restored without losing
	// that later edit, so the revert names it and stops.
	async function revert(change: Change) {
		reverting = true;
		revertError = null;
		try {
			const changed = await changedSince(change);
			if (changed.length > 0) {
				editedLater = changed;
				return;
			}
			const { data: created } = await proposeChange({
				body: {
					title: m.change_revert_title({ title: change.title }),
					files: change.files.map((file) => ({ path: file.path, content: file.previous ?? null })),
					templates: revertedTemplates(change) ?? []
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
		const fallback = action === 'approve' ? m.change_deploy_failed() : m.change_reject_failed();
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
								data-tip={change.issues.length > 0 ? m.change_deploy_blocked() : undefined}
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
									{change.status === 'pending' ? m.change_deploy() : m.common_retry()}
								</button>
							</div>
						</div>
					{:else if change.status === 'committed' && revertedTemplates(change) !== null}
						<div
							class="tooltip tooltip-left"
							data-tip={editedLater.length > 0
								? m.change_revert_changed_since({ paths: editedLater.join(', ') })
								: undefined}
						>
							<button
								type="button"
								class="btn btn-ghost btn-sm gap-1.5"
								disabled={reverting || editedLater.length > 0}
								onclick={() => revert(change)}
							>
								{#if reverting}
									<span class="loading loading-spinner loading-xs"></span>
								{:else}
									<Icon icon="tabler:arrow-back-up" class="h-4 w-4" />
								{/if}
								{m.change_revert()}
							</button>
						</div>
					{/if}
				</div>

				{#if revertError}
					<div class="rounded-lg bg-error/5 p-3 text-sm">{revertError}</div>
				{/if}

				{#if decideError}
					<div class="rounded-lg bg-error/5 p-3 text-sm">{decideError}</div>
				{/if}
				{#if change.error}
					<div class="flex items-start gap-2 rounded-lg bg-error/5 p-3 text-sm">
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
					<div class="rounded-lg bg-error/5 p-2">
						<IssueList
							issues={change.issues}
							contentOf={(path) => change.files.find((file) => file.path === path)?.content}
							showPath={change.files.length > 1}
						/>
					</div>
				{/if}

				{#if removeError}
					<div class="rounded-lg bg-error/5 p-3 text-sm">{removeError}</div>
				{/if}
				{#each change.files as file (file.path)}
					{@const view = viewUrl(base, file.path)}
					{@const viewable =
						view !== null &&
						file.content != null &&
						(file.previous != null || change.deployment === 'deployed')}
					{@const graph = comparisons.get(file.path) != null}
					{@const markdown = file.content != null && file.path.endsWith('.md')}
					{@const shown = graph || markdown ? changeView.value : 'diff'}
					<div class="overflow-clip rounded-lg border border-base-200">
						<div class="flex items-center gap-2 px-3 py-2">
							<Icon
								icon={file.content == null ? 'tabler:file-minus' : file.previous == null ? 'tabler:file-plus' : 'tabler:file-diff'}
								class="h-4 w-4 opacity-70"
							/>
							{#if viewable}
								<a href={view} class="font-mono text-xs text-primary hover:underline">{file.path}</a>
							{:else}
								<span class="font-mono text-xs">{file.path}</span>
							{/if}
							{#if file.content == null}
								<span class="text-xs text-error">{m.change_file_deleted()}</span>
							{:else if file.previous == null}
								<span class="text-xs text-primary">{m.change_file_new()}</span>
							{/if}
							<div class="ml-auto flex items-center gap-1">
								{#if graph || markdown}
									<Segmented
										label={m.flow_view_label()}
										options={[
											{ id: 'preview', label: graph ? m.flow_graph_heading() : m.change_view_preview() },
											{ id: 'diff', label: m.change_view_diff() }
										]}
										value={shown}
										onchange={(next) => (changeView.value = next)}
									/>
								{/if}
								{#if canEdit}
									{@const template = change.templates?.find((t) => t.files.includes(file.path))}
									<div class="dropdown dropdown-end">
										<button
											type="button"
											tabindex="0"
											class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 hover:bg-base-200 hover:text-base-content"
											aria-label={m.editor_more_actions()}
										>
											<Icon icon="tabler:dots" class="h-5 w-5" />
										</button>
										<ul
											tabindex="-1"
											class="menu dropdown-content z-10 mt-1 w-56 rounded-md border border-base-200 bg-base-100 p-1 text-sm shadow-lg"
										>
											{#if file.content != null}
												<li>
													<a href="{editUrl(base, file.path)}?change={encodeURIComponent(change.id)}">
														<Icon icon="tabler:pencil" class="h-4 w-4" />
														{m.common_edit()}
													</a>
												</li>
											{/if}
											<li>
												<button type="button" class="text-error" onclick={() => removeFile(change.id, file.path)}>
													<Icon icon="tabler:x" class="h-4 w-4" />
													{template
														? m.change_remove_template({ name: template.slug })
														: m.change_remove_file()}
												</button>
											</li>
										</ul>
									</div>
								{/if}
							</div>
						</div>
						{#if shown === 'preview' && graph}
							<div class="h-80">
								<FlowDag
									yaml={file.content ?? ''}
									previous={file.previous ?? null}
									onNodeClick={(name) => (selectedTask = { path: file.path, name })}
								/>
							</div>
						{:else if shown === 'preview' && markdown && file.content != null}
							<ResourceViewer content={file.content} extension="md" />
						{:else}
							<pre class="overflow-x-auto text-xs leading-5">{#each file.diff.split('\n') as line, i (i)}<div class="px-3 {lineClass(line)}">{line || ' '}</div>{/each}</pre>
						{/if}
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

{#if data.change && selectedTask}
	{@const file = data.change.files.find((f) => f.path === selectedTask?.path)}
	{@const comparison = file ? comparisons.get(file.path) : null}
	{@const task = comparison?.tasks.get(selectedTask.name)}
	<Inspector title={selectedTask.name} label={m.change_task_details()} onclose={() => (selectedTask = null)}>
		{#snippet subtitle()}
			{#if task}
				<StatusDot variant={MARK_VARIANT[task.mark]} label={markLabel(task.mark)} />
			{/if}
		{/snippet}
		<TaskChanges mark={task?.mark} fields={task?.fields ?? []} />
	</Inspector>
{/if}

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
