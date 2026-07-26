<script lang="ts">
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import FlowDag from '$lib/dag/FlowDag.svelte';
	import Badge from '$lib/Badge.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import ActivityPanel from '$lib/flow/ActivityPanel.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { apiUrl } from '$lib/api';
	import type { ActivityLevel } from '$lib/logRecord';

	interface Activity {
		flow: string;
		task: string | null;
		task_type: string | null;
		level: ActivityLevel;
		ts_ms: number;
		message: string;
		duration_ms?: number;
	}

	interface Props {
		yaml: string;
		activities?: Activity[];
	}

	let { yaml, activities = [] }: Props = $props();

	interface ResourcePreview {
		key: string;
		extension: string | null;
		content: string;
	}

	let previewKey = $state<string | null>(null);
	let previewContent = $state<ResourcePreview | null>(null);
	let previewLoading = $state(false);
	let previewError = $state<string | null>(null);

	function resourceHref(key: string): string {
		return `${base}/resources/${key.split('/').map(encodeURIComponent).join('/')}`;
	}

	let yamlPane = $state<HTMLElement | null>(null);
	let dagPane = $state<HTMLElement | null>(null);
	let activityPanel = $state<ActivityPanel | null>(null);
	let activityExpanded = $state(false);
	let highlightTimeout: ReturnType<typeof setTimeout> | null = null;
	let flashedEl: HTMLElement | null = null;

	// Latest-per-task snapshot: level for the status pill, duration for the
	// inline badge. Both feed the DAG so the user sees "just processed, took Xms".
	// The badge is an operational health signal, not a log viewer, so it
	// only ever reflects info/warning/error — a debug/trace event landing
	// after a task's last info!() must not blank out that "just succeeded"
	// signal, so those levels are skipped entirely when picking "latest".
	interface NodeState {
		level: 'info' | 'warning' | 'error';
		ts_ms: number;
		duration_ms?: number;
	}
	let nodeStates = $derived.by(() => {
		const map = new Map<string, NodeState>();
		for (const a of activities) {
			if (!a.task) continue;
			if (a.level !== 'info' && a.level !== 'warning' && a.level !== 'error') continue;
			const prev = map.get(a.task);
			if (!prev || a.ts_ms >= prev.ts_ms) {
				map.set(a.task, { level: a.level, ts_ms: a.ts_ms, duration_ms: a.duration_ms });
			}
		}
		return map;
	});

	function formatDuration(ms: number): string {
		if (ms < 1000) return `${ms}ms`;
		if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
		return `${Math.floor(ms / 60_000)}m`;
	}

	// Push node states into the DAG DOM as data attributes / text. TaskNode
	// renders the persistent status pill + duration badge from these.
	$effect(() => {
		if (!dagPane) return;
		const nodes = dagPane.querySelectorAll<HTMLElement>('[data-task]');
		for (const el of nodes) {
			const name = el.dataset.task;
			if (!name) continue;
			const st = nodeStates.get(name);
			const badge = el.querySelector<HTMLElement>('.duration-badge');
			if (st) {
				el.dataset.level = st.level;
				if (st.duration_ms !== undefined) {
					el.dataset.duration = String(st.duration_ms);
					if (badge) badge.textContent = formatDuration(st.duration_ms);
				} else {
					delete el.dataset.duration;
					if (badge) badge.textContent = '—';
				}
			} else {
				delete el.dataset.level;
				delete el.dataset.duration;
				if (badge) badge.textContent = '—';
			}
		}
	});

	function onNodeClick(name: string) {
		scrollToTask(name);
		if (!activityExpanded) activityExpanded = true;
		queueMicrotask(() => activityPanel?.scrollToLatestFor(name));
	}

	function onActivityRowClick(taskName: string) {
		scrollToTask(taskName);
	}

	function scrollToTask(name: string) {
		if (!yamlPane) return;
		const target = yamlPane.querySelector<HTMLElement>(`#task-${CSS.escape(name)}`);
		if (!target) return;
		target.scrollIntoView({ behavior: 'smooth', block: 'center' });

		// Restart the CSS animation reliably: remove class from any previously
		// flashed element (which may or may not be the same target), force a
		// reflow, then add the class again. Without the reflow, re-adding the
		// class to the same node in the same tick is a no-op — the animation
		// stays "finished" and nothing visual happens.
		if (flashedEl) flashedEl.classList.remove('task-flash');
		if (highlightTimeout) clearTimeout(highlightTimeout);
		void target.offsetWidth;
		target.classList.add('task-flash');
		flashedEl = target;
		highlightTimeout = setTimeout(() => {
			target.classList.remove('task-flash');
			if (flashedEl === target) flashedEl = null;
		}, 1600);
	}

	async function openResource(key: string) {
		previewKey = key;
		previewContent = null;
		previewError = null;
		previewLoading = true;
		try {
			const response = await fetch(apiUrl(`api/resources/${key}`));
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			previewContent = await response.json();
		} catch (err) {
			previewError = err instanceof Error ? err.message : 'Failed to load resource';
		} finally {
			previewLoading = false;
		}
	}

	function closeResource() {
		previewKey = null;
		previewContent = null;
		previewError = null;
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape' && previewKey) closeResource();
	}
</script>

<svelte:window on:keydown={onKeydown} />

<div class="flex min-h-0 flex-1 flex-col">
	<div class="grid min-h-0 flex-1 grid-cols-2 gap-0 divide-x divide-base-200">
		<div class="flex min-h-0 flex-col">
			<div class="flex h-10 shrink-0 items-center border-b border-base-200 bg-base-100 px-4 text-xs font-medium opacity-70">
				Graph
			</div>
			<div bind:this={dagPane} class="min-h-0 flex-1">
				<FlowDag {yaml} onNodeClick={onNodeClick} />
			</div>
		</div>
		<div class="flex min-h-0 flex-col">
			<div class="flex h-10 shrink-0 items-center justify-between border-b border-base-200 bg-base-100 px-4">
				<span class="text-xs font-medium opacity-70">Config</span>
				<CopyButton text={yaml} label="Copy YAML" />
			</div>
			<div bind:this={yamlPane} class="min-h-0 flex-1 overflow-auto bg-base-200">
				<ResourceViewer
					content={yaml}
					extension="yaml"
					onResourceClick={openResource}
					anchorTaskNames
				/>
			</div>
		</div>
	</div>
	<ActivityPanel
		bind:this={activityPanel}
		{activities}
		expanded={activityExpanded}
		onToggle={() => (activityExpanded = !activityExpanded)}
		onRowClick={onActivityRowClick}
	/>
</div>

{#if previewKey}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
		role="dialog"
		aria-modal="true"
		aria-label="Resource preview"
		onclick={(e) => {
			if (e.target === e.currentTarget) closeResource();
		}}
		onkeydown={(e) => {
			if (e.key === 'Escape') closeResource();
		}}
		tabindex="-1"
	>
		<div
			class="flex max-h-[85vh] w-full max-w-4xl flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-xl"
		>
			<div class="flex items-center justify-between border-b border-base-200 px-4 py-2">
				<div class="flex items-center gap-2">
					<span class="font-mono text-sm font-medium leading-none">{previewKey}</span>
					{#if previewContent?.extension}
						<Badge>{previewContent.extension}</Badge>
					{/if}
				</div>
				<div class="flex items-center gap-1">
					<div class="tooltip tooltip-left" data-tip="Open full page">
						<a
							href={resourceHref(previewKey)}
							class="btn btn-ghost btn-sm btn-circle"
							aria-label="Open full page"
						>
							<Icon icon="tabler:external-link" class="h-5 w-5" />
						</a>
					</div>
					<div class="tooltip tooltip-left" data-tip="Close">
						<button
							type="button"
							class="btn btn-ghost btn-sm btn-circle"
							aria-label="Close"
							onclick={closeResource}
						>
							<Icon icon="tabler:x" class="h-5 w-5" />
						</button>
					</div>
				</div>
			</div>
			<div class="flex items-center justify-end border-b border-base-200 bg-base-100 px-4 py-1">
				<CopyButton text={previewContent?.content} label="Copy" />
			</div>
			<div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-base-200">
				{#if previewLoading}
					<div class="flex items-center justify-center py-12">
						<span class="loading loading-spinner loading-md text-primary"></span>
					</div>
				{:else if previewError}
					<div class="flex-1">
						<StateMessage tone="oops" title="Failed to load resource" message={previewError} />
					</div>
				{:else if previewContent}
					<div class="min-h-0 flex-1 overflow-auto">
						<ResourceViewer
							content={previewContent.content}
							extension={previewContent.extension}
						/>
					</div>
				{/if}
			</div>
		</div>
	</div>
{/if}

<style>
	:global(.task-flash) {
		background: rgba(0, 118, 0, 0.22);
		border-radius: 3px;
		padding: 0 2px;
		animation: task-fade 1.6s ease-out forwards;
	}
	@keyframes task-fade {
		0% {
			background: rgba(0, 118, 0, 0.3);
		}
		100% {
			background: transparent;
		}
	}

</style>
