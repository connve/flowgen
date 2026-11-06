<script lang="ts">
	import { onMount, tick, untrack } from 'svelte';
	import { load } from 'js-yaml';
	import Icon from '@iconify/svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import CodeEditor from '$lib/CodeEditor.svelte';
	import FlowDag from '$lib/dag/FlowDag.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import type { Activity } from '$lib/flow/ActivityDetails.svelte';
	import ActivityPanel from '$lib/flow/ActivityPanel.svelte';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		yaml: string;
		activities?: Activity[];
		// Config becomes an editor bound to `yaml`, and the graph follows it.
		editable?: boolean;
		/** The resource clicked in the config, for the page's details. */
		resource?: string | null;
		/** The activity record clicked, for the page's details. */
		activity?: Activity | null;
		/** A task to scroll to and filter the activity by once the page opens. */
		focusTask?: string | null;
	}

	let {
		yaml = $bindable(),
		activities,
		editable = false,
		resource = $bindable(null),
		activity = $bindable(null),
		focusTask = null
	}: Props = $props();

	// While editing, the graph keeps the last YAML that parsed, so it does not
	// blank out mid-keystroke.
	let parsedYaml = $state(untrack(() => yaml));
	$effect(() => {
		if (!editable) return;
		const next = yaml;
		const timer = setTimeout(() => {
			if (parses(next)) parsedYaml = next;
		}, 300);
		return () => clearTimeout(timer);
	});

	function parses(source: string): boolean {
		try {
			load(source);
			return true;
		} catch {
			return false;
		}
	}
	let graphYaml = $derived(editable ? parsedYaml : yaml);

	let yamlPane = $state<HTMLElement | null>(null);
	let activityPanel = $state<ActivityPanel | null>(null);
	let activityExpanded = $state(false);
	$effect(() => {
		if (activity) untrack(() => (resource = null));
	});
	let configHidden = $state(false);

	const ROOMY_WIDTH = 1280;
	let windowWidth = $state(ROOMY_WIDTH);
	let narrow = $derived(windowWidth < ROOMY_WIDTH);
	let view = $state<'graph' | 'config'>(untrack(() => (editable ? 'config' : 'graph')));
	let showGraph = $derived(!narrow || view === 'graph');
	let showConfig = $derived(narrow ? view === 'config' : editable || !configHidden);

	onMount(() => {
		configHidden = localStorage.getItem('flowgen-config-hidden') === '1';
		if (focusTask) tick().then(() => onNodeClick(focusTask));
	});

	function toggleConfig() {
		configHidden = !configHidden;
		localStorage.setItem('flowgen-config-hidden', configHidden ? '1' : '0');
	}
	let highlightTimeout: ReturnType<typeof setTimeout> | null = null;
	let flashedEl: HTMLElement | null = null;


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

	function openResource(key: string) {
		activity = null;
		resource = key;
	}
</script>

<svelte:window bind:innerWidth={windowWidth} />

{#snippet viewSwitch()}
	<div
		class="flex items-center gap-0.5 rounded-full border border-base-300 bg-base-200/50 p-0.5 text-xs"
		role="group"
		aria-label={m.flow_view_label()}
	>
		{#each [{ id: 'graph' as const, label: m.flow_graph_heading() }, { id: 'config' as const, label: m.flow_config_heading() }] as opt (opt.id)}
			<button
				type="button"
				class="h-6 rounded-full px-3 transition-colors {view === opt.id
					? 'bg-base-100 font-medium shadow-sm'
					: 'text-base-content/60 hover:text-base-content'}"
				aria-pressed={view === opt.id}
				onclick={() => (view = opt.id)}
			>
				{opt.label}
			</button>
		{/each}
	</div>
{/snippet}

<div class="flex min-h-0 flex-1 flex-col">
	<div class="flex min-h-0 flex-1">
	<div
		class="grid min-h-0 min-w-0 flex-1 gap-0 divide-x divide-base-200 {showGraph && showConfig
			? 'grid-cols-2'
			: 'grid-cols-1'}"
	>
		<div class="flex min-h-0 flex-col {showGraph ? '' : 'hidden'}">
			<div class="flex h-10 shrink-0 items-center justify-between bg-base-100 px-4">
				{#if narrow}
					{@render viewSwitch()}
				{:else}
					<span class="text-xs font-medium text-base-content/50">{m.flow_graph_heading()}</span>
				{/if}
				{#if !editable && !narrow}
					<div
						class="tooltip tooltip-left"
						data-tip={configHidden ? m.flow_config_expand() : m.flow_config_collapse()}
					>
						<button
							type="button"
							class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/50 transition-colors hover:bg-base-200 hover:text-base-content"
							aria-label={configHidden ? m.flow_config_expand() : m.flow_config_collapse()}
							aria-pressed={configHidden}
							onclick={toggleConfig}
						>
							<Icon
								icon={configHidden
									? 'tabler:layout-sidebar-right-expand'
									: 'tabler:layout-sidebar-right-collapse'}
								class="h-5 w-5"
							/>
						</button>
					</div>
				{/if}
			</div>
			<div class="min-h-0 flex-1">
				<FlowDag yaml={graphYaml} {activities} onNodeClick={onNodeClick} />
			</div>
		</div>
		<div class="flex min-h-0 flex-col {showConfig ? '' : 'hidden'}">
			<div class="flex h-10 shrink-0 items-center justify-between bg-base-100 px-4">
				{#if narrow}
					{@render viewSwitch()}
				{:else}
					<span class="text-xs font-medium text-base-content/50">{m.flow_config_heading()}</span>
				{/if}
				<CopyButton text={yaml} label={m.flow_copy_yaml()} />
			</div>
			<div bind:this={yamlPane} class="flex min-h-0 flex-1 flex-col overflow-auto bg-base-200">
				{#if editable}
					<CodeEditor bind:value={yaml} extension="yaml" label={m.flow_config_editor_label()} />
				{:else}
					<ResourceViewer
						content={yaml}
						extension="yaml"
						onResourceClick={openResource}
						anchorTaskNames
					/>
				{/if}
			</div>
		</div>
	</div>
	</div>
	{#if activities}
		<ActivityPanel
			bind:this={activityPanel}
			bind:selected={activity}
			{activities}
			expanded={activityExpanded}
			onToggle={() => (activityExpanded = !activityExpanded)}
			onRowClick={onActivityRowClick}
		/>
	{/if}
</div>

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
