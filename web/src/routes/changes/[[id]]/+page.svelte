<script lang="ts">
	import { base } from '$app/paths';
	import { afterNavigate, goto, invalidate } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import Badge from '$lib/Badge.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { apiUrl, type ChangeStatus } from '$lib/api';
	import { formatAbsolute, formatRelative } from '$lib/time';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let deciding = $state(false);
	let decideError = $state<string | null>(null);

	// Pending and failed changes can be published; one in `publishing` is
	// refused by the server until its publish timeout passed, so it offers none.
	let canApprove = $derived(
		data.change != null && (data.change.status === 'pending' || data.change.status === 'failed')
	);
	let pendingCount = $derived(data.changes.filter((c) => c.status === 'pending').length);

	afterNavigate(() => {
		decideError = null;
	});

	async function decide(id: string, action: 'approve' | 'reject') {
		deciding = true;
		decideError = null;
		try {
			const res = await fetch(apiUrl(`api/changes/${encodeURIComponent(id)}/${action}`), {
				method: 'POST'
			});
			if (!res.ok) throw new Error((await res.text()) || `HTTP ${res.status}`);
			await invalidate('app:changes');
		} catch (err) {
			decideError = err instanceof Error ? err.message : 'Request failed';
		} finally {
			deciding = false;
		}
	}

	function statusVariant(status: ChangeStatus): 'neutral' | 'success' | 'error' | 'warning' {
		switch (status) {
			case 'published':
				return 'success';
			case 'failed':
				return 'error';
			case 'pending':
			case 'publishing':
				return 'warning';
			default:
				return 'neutral';
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

<div class="flex min-h-0 flex-1 flex-col">
	<div class="shrink-0 border-b border-base-200 bg-base-100 px-6 pb-4 pt-6">
		<div class="flex items-center gap-1.5 text-sm">
			{#if data.change}
				<a href="{base}/changes" class="text-primary hover:underline">Changes</a>
			{:else}
				<span>Changes</span>
				<span class="text-xs opacity-50">· {pendingCount} pending</span>
			{/if}
		</div>
	</div>

	<div class="flex min-h-0 flex-1 flex-col overflow-y-auto p-6">
		{#if data.change}
			{@const change = data.change}
			<div class="space-y-4">
				<div class="flex flex-wrap items-start justify-between gap-3">
					<div class="space-y-1">
						<div class="flex items-center gap-2">
							<h1 class="text-lg font-semibold">{change.title}</h1>
							<Badge variant={statusVariant(change.status)}>{change.status}</Badge>
						</div>
						<div class="text-xs opacity-60">
							Proposed by {change.proposedBy} · {formatAbsolute(change.createdAt)}
							{#if change.decidedBy && change.decidedAt}
								· {change.status === 'rejected' ? 'rejected' : 'approved'} by {change.decidedBy}
								{formatRelative(change.decidedAt)}
							{/if}
						</div>
						{#if change.description}
							<p class="max-w-3xl whitespace-pre-line text-sm">{change.description}</p>
						{/if}
					</div>
					{#if canApprove}
						<div class="flex items-center gap-1">
							<div class="tooltip tooltip-bottom" data-tip="Reject">
								<button
									type="button"
									class="btn btn-ghost btn-sm btn-circle"
									aria-label="Reject"
									disabled={deciding}
									onclick={() => decide(change.id, 'reject')}
								>
									<Icon icon="tabler:x" class="h-5 w-5" />
								</button>
							</div>
							<div
								class="tooltip tooltip-bottom"
								data-tip={change.issues.length > 0
									? 'Fix the issues before approving'
									: change.status === 'pending'
										? 'Approve'
										: 'Retry'}
							>
								<button
									type="button"
									class="btn btn-ghost btn-sm btn-circle"
									aria-label={change.status === 'pending' ? 'Approve' : 'Retry'}
									disabled={deciding || change.issues.length > 0}
									onclick={() => decide(change.id, 'approve')}
								>
									{#if deciding}
										<span class="loading loading-spinner loading-sm"></span>
									{:else}
										<Icon icon={change.status === 'pending' ? 'tabler:check' : 'tabler:refresh'} class="h-5 w-5" />
									{/if}
								</button>
							</div>
						</div>
					{/if}
				</div>

				{#if decideError}
					<div class="rounded-lg border border-error/40 bg-error/5 p-3 text-sm">{decideError}</div>
				{/if}
				{#if change.error}
					<div class="flex items-start gap-2 rounded-lg border border-error/40 bg-error/5 p-3 text-sm">
						<Icon icon="tabler:alert-triangle" class="h-5 w-5 shrink-0 text-error" />
						<span>Publishing failed: {change.error}</span>
					</div>
				{/if}
				{#if change.status === 'published' && Object.keys(change.result ?? {}).length > 0}
					<div class="rounded-lg border border-base-300 p-3 text-xs">
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
						<div class="mb-1 font-medium text-error">Validation issues</div>
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
					<div class="overflow-hidden rounded-lg border border-base-300">
						<div class="flex items-center gap-2 border-b border-base-300 bg-base-200/50 px-3 py-2">
							<Icon
								icon={file.content == null ? 'tabler:file-minus' : file.previous == null ? 'tabler:file-plus' : 'tabler:file-diff'}
								class="h-4 w-4 opacity-70"
							/>
							<span class="font-mono text-xs">{file.path}</span>
							{#if file.content == null}
								<Badge variant="error">deleted</Badge>
							{:else if file.previous == null}
								<Badge variant="success">new</Badge>
							{/if}
						</div>
						<pre class="overflow-x-auto text-xs leading-5">{#each file.diff.split('\n') as line, i (i)}<div class="px-3 {lineClass(line)}">{line || ' '}</div>{/each}</pre>
					</div>
				{/each}
			</div>
		{:else if data.changes.length === 0}
			<StateMessage
				tone="notice"
				title="No changes"
				message="Flow and resource changes show up here for review."
			/>
		{:else}
			<div class="shrink-0 overflow-x-auto rounded-lg border border-base-300 bg-base-100">
				<table class="table table-sm w-full bg-base-100">
					<thead class="bg-base-100 text-xs uppercase tracking-wide">
						<tr>
							<th>Title</th>
							<th>Status</th>
							<th>Proposed by</th>
							<th class="text-right">Files</th>
							<th class="text-right">Created</th>
						</tr>
					</thead>
					<tbody>
						{#each data.changes as item (item.id)}
							<tr
								class="cursor-pointer transition-colors hover:bg-base-200"
								onclick={(e) => {
									if (!(e.target instanceof Element && e.target.closest('a')))
										goto(`${base}/changes/${encodeURIComponent(item.id)}`);
								}}
							>
								<td>
									<a href="{base}/changes/{encodeURIComponent(item.id)}" class="hover:underline">
										{item.title}
									</a>
								</td>
								<td><Badge variant={statusVariant(item.status)}>{item.status}</Badge></td>
								<td class="font-mono text-xs opacity-70">{item.proposedBy}</td>
								<td class="text-right text-sm opacity-70">{item.fileCount}</td>
								<td class="whitespace-nowrap text-right text-sm opacity-70">
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
