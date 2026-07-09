<script lang="ts">
	import Icon from '@iconify/svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import FlowDag from '$lib/dag/FlowDag.svelte';
	import Badge from '$lib/Badge.svelte';
	import { apiUrl } from '$lib/api';

	interface Props {
		yaml: string;
	}

	let { yaml }: Props = $props();

	let copied = $state(false);
	let copyTimeout: ReturnType<typeof setTimeout> | null = null;

	async function copyYaml() {
		try {
			await navigator.clipboard.writeText(yaml);
			copied = true;
			if (copyTimeout) clearTimeout(copyTimeout);
			copyTimeout = setTimeout(() => (copied = false), 1500);
		} catch {
			// Clipboard API refused (no permission, insecure context) —
			// silently no-op; the button just doesn't confirm.
		}
	}

	interface ResourcePreview {
		key: string;
		extension: string | null;
		content: string;
	}

	let previewKey = $state<string | null>(null);
	let previewContent = $state<ResourcePreview | null>(null);
	let previewLoading = $state(false);
	let previewError = $state<string | null>(null);
	let previewCopied = $state(false);
	let previewCopyTimeout: ReturnType<typeof setTimeout> | null = null;

	async function copyResource() {
		if (!previewContent) return;
		try {
			await navigator.clipboard.writeText(previewContent.content);
			previewCopied = true;
			if (previewCopyTimeout) clearTimeout(previewCopyTimeout);
			previewCopyTimeout = setTimeout(() => (previewCopied = false), 1500);
		} catch {
			// Clipboard refused (permissions, insecure context) — silent no-op.
		}
	}

	let yamlPane = $state<HTMLElement | null>(null);
	let highlightTimeout: ReturnType<typeof setTimeout> | null = null;

	let flashedEl: HTMLElement | null = null;

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

<div class="grid min-h-0 flex-1 grid-cols-2 gap-0 divide-x divide-base-200">
	<div class="flex min-h-0 flex-col">
		<div class="border-b border-base-200 bg-base-100 px-4 py-2 text-xs font-medium opacity-70">
			Graph
		</div>
		<div class="min-h-0 flex-1">
			<FlowDag {yaml} onNodeClick={scrollToTask} />
		</div>
	</div>
	<div class="flex min-h-0 flex-col">
		<div class="flex items-center justify-between border-b border-base-200 bg-base-100 px-4 py-1">
			<span class="text-xs font-medium opacity-70">Config</span>
			<button
				type="button"
				class="btn btn-ghost btn-xs gap-1"
				aria-label={copied ? 'Copied' : 'Copy YAML'}
				onclick={copyYaml}
			>
				{#if copied}
					<Icon icon="tabler:check" class="h-6 w-6 text-primary" />
					<span class="text-primary">Copied</span>
				{:else}
					<Icon icon="tabler:copy" class="h-6 w-6" />
					<span>Copy</span>
				{/if}
			</button>
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
					<button
						type="button"
						class="btn btn-ghost btn-sm gap-1"
						aria-label={previewCopied ? 'Copied' : 'Copy'}
						onclick={copyResource}
						disabled={!previewContent}
					>
						{#if previewCopied}
							<Icon icon="tabler:check" class="h-6 w-6 text-primary" />
							<span class="text-primary">Copied</span>
						{:else}
							<Icon icon="tabler:copy" class="h-6 w-6" />
							<span>Copy</span>
						{/if}
					</button>
					<button
						type="button"
						class="btn btn-ghost btn-sm btn-circle"
						aria-label="Close"
						onclick={closeResource}
					>
						<Icon icon="tabler:x" class="h-6 w-6" />
					</button>
				</div>
			</div>
			<div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-base-200">
				{#if previewLoading}
					<div class="flex items-center justify-center py-12">
						<span class="loading loading-spinner loading-md text-primary"></span>
					</div>
				{:else if previewError}
					<div class="alert alert-error m-4" role="alert">
						<span>{previewError}</span>
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
	/* Brief flash on the task name after a DAG node click, so the user
	   spots where the config pane scrolled to. */
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
