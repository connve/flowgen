<script lang="ts">
	import { onMount, tick, untrack } from 'svelte';
	import { load } from 'js-yaml';
	import Icon from '@iconify/svelte';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import CodeEditor from '$lib/CodeEditor.svelte';
	import FlowDag from '$lib/dag/FlowDag.svelte';
	import { CopyButton, IconButton, Segmented } from '@connve/ui';
	import type { Activity } from '$lib/flow/ActivityDetails.svelte';
	import ActivityPanel from '$lib/flow/ActivityPanel.svelte';
	import ConfigFields from '$lib/flow/ConfigFields.svelte';
	import ConfigViewSwitch from '$lib/ConfigViewSwitch.svelte';
	import { configView } from '$lib/configView.svelte';
	import { getFlowSchema } from '$lib/api';
	import FlowForm from '$lib/form/FlowForm.svelte';
	import { FlowDocument } from '$lib/flowDocument';
	import type { Schema } from '$lib/form/schema';
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
	const TYPING_PAUSE_MS = 300;
	let parsedYaml = $state(untrack(() => yaml));
	$effect(() => {
		if (!editable) return;
		const next = yaml;
		const pause = configView.value === 'fields' ? 0 : TYPING_PAUSE_MS;
		const timer = setTimeout(() => {
			if (parses(next)) parsedYaml = next;
		}, pause);
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
	const COMPACT_WIDTH = 768;
	let windowWidth = $state(ROOMY_WIDTH);
	let narrow = $derived(windowWidth < ROOMY_WIDTH);
	let compact = $derived(windowWidth < COMPACT_WIDTH);
	let view = $state<'graph' | 'config'>(untrack(() => (editable ? 'config' : 'graph')));
	let showGraph = $derived(!narrow || view === 'graph');
	let showConfig = $derived(narrow ? view === 'config' : editable || !configHidden);

	let maximized = $state(false);
	// On a phone the graph and config fold to their header while the activity is open.
	let folded = $derived(compact && activities !== undefined && activityExpanded);

	onMount(() => {
		configHidden = localStorage.getItem('flowgen-config-hidden') === '1';
		if (window.innerWidth < COMPACT_WIDTH && !editable) activityExpanded = true;
		if (focusTask) tick().then(() => onNodeClick(focusTask));
	});

	function toggleActivity() {
		activityExpanded = !activityExpanded;
		if (!activityExpanded) maximized = false;
	}

	function toggleConfig() {
		configHidden = !configHidden;
		localStorage.setItem('flowgen-config-hidden', configHidden ? '1' : '0');
	}
	let highlightTimeout: ReturnType<typeof setTimeout> | null = null;
	let flashedEl: HTMLElement | null = null;

	// The flow schema the form is built from, fetched once the form is shown.
	let flowSchema = $state<Schema | null>(null);
	let flowForm = $state<FlowForm | null>(null);
	let picking = $state<number | 'end' | false>(false);

	function addFromGraph(after: string | null) {
		configView.value = 'fields';
		if (narrow) view = 'config';
		const index = after === null ? -1 : indexOf(after);
		picking = index < 0 ? 'end' : index;
	}

	function indexOf(taskName: string): number {
		return new FlowDocument(yaml).tasks().findIndex((task) => task.name === taskName);
	}

	function editFlow(change: (flow: FlowDocument) => void) {
		const next = new FlowDocument(yaml);
		if (!next.valid) return;
		change(next);
		yaml = next.toString();
	}

	function removeFromGraph(taskName: string) {
		const index = indexOf(taskName);
		if (index >= 0) editFlow((flow) => flow.removeTask(index));
	}

	function moveInGraph(taskName: string, order: string[]) {
		const from = indexOf(taskName);
		const to = order.indexOf(taskName);
		if (from >= 0 && to >= 0 && from !== to) editFlow((flow) => flow.moveTask(from, to));
	}
	$effect(() => {
		if (!editable || configView.value !== 'fields' || flowSchema) return;
		getFlowSchema()
			.then(({ data }) => (flowSchema = data as Schema))
			.catch(() => (configView.value = 'yaml'));
	});


	// A new object on every click, so clicking the same task again opens and
	// scrolls to it. The form exists only once its schema has loaded.
	let formReveal = $state<{ task: string } | null>(null);
	$effect(() => {
		const reveal = formReveal;
		const form = flowForm;
		if (!reveal || !form) return;
		// Measures after the form opens the task and closes the previous one.
		form.reveal(reveal.task).then(() => {
			const target = yamlPane?.querySelector<HTMLElement>(`#form-task-${CSS.escape(reveal.task)}`);
			if (target) scrollPaneTo(target, 'start');
			// Done, so switching the view back to the form does not scroll again.
			if (formReveal === reveal) formReveal = null;
		});
	});

	// Scrolls only the config pane. `scrollIntoView` also scrolls every
	// scrollable ancestor up to the page, which shifts the whole editor.
	function scrollPaneTo(target: HTMLElement, block: 'start' | 'center') {
		if (!yamlPane) return;
		const top =
			target.getBoundingClientRect().top - yamlPane.getBoundingClientRect().top + yamlPane.scrollTop;
		const offset = block === 'center' ? (yamlPane.clientHeight - target.offsetHeight) / 2 : 8;
		yamlPane.scrollTo({ top: Math.max(0, top - offset), behavior: 'smooth' });
	}

	function onNodeClick(name: string) {
		if (editable && configView.value === 'fields') {
			formReveal = { task: name };
			return;
		}
		scrollToTask(name);
		if (!activityExpanded) activityExpanded = true;
		queueMicrotask(() => activityPanel?.scrollToLatestFor(name));
	}

	export function showTask(name: string) {
		configView.value = 'fields';
		if (narrow) view = 'config';
		onNodeClick(name);
	}

	function onActivityRowClick(taskName: string) {
		scrollToTask(taskName);
	}

	function scrollToTask(name: string) {
		if (!yamlPane) return;
		const target = yamlPane.querySelector<HTMLElement>(`#task-${CSS.escape(name)}`);
		if (!target) return;
		scrollPaneTo(target, 'center');

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
	<Segmented
		label={m.flow_view_label()}
		options={[
			{ id: 'graph', label: m.flow_graph_heading() },
			{ id: 'config', label: m.flow_config_heading() }
		]}
		bind:value={view}
		onchange={() => {
			if (folded) toggleActivity();
		}}
	/>
{/snippet}

{#snippet foldToggle()}
	{#if compact && activities}
		<IconButton
			icon={folded ? 'tabler:chevron-down' : 'tabler:chevron-up'}
			label={folded ? m.flow_view_expand() : m.flow_view_collapse()}
			tooltip="left"
			onclick={toggleActivity}
		/>
	{/if}
{/snippet}

<div class="flex min-h-0 flex-1 flex-col">
	<div class="flex {maximized ? 'hidden' : folded ? 'shrink-0' : 'min-h-0 flex-1'}">
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
				{@render foldToggle()}
			</div>
			<div class="min-h-0 flex-1 {folded ? 'hidden' : ''}">
				<FlowDag
					yaml={graphYaml}
					{activities}
					{onNodeClick}
					onadd={editable ? addFromGraph : undefined}
					onremove={editable ? removeFromGraph : undefined}
					onmove={editable ? moveInGraph : undefined}
				/>
			</div>
		</div>
		<div class="flex min-h-0 flex-col {showConfig ? '' : 'hidden'}">
			<div class="flex h-10 shrink-0 items-center justify-between bg-base-100 px-4">
				{#if narrow}
					{@render viewSwitch()}
				{:else}
					<span class="text-xs font-medium text-base-content/50">{m.flow_config_heading()}</span>
				{/if}
				<div class="flex items-center gap-2">
					<ConfigViewSwitch />
					<CopyButton text={yaml} label={m.flow_copy_yaml()} />
					{@render foldToggle()}
				</div>
			</div>
			<div
				bind:this={yamlPane}
				class="min-h-0 flex-1 flex-col overflow-auto {folded ? 'hidden' : 'flex'} {configView.value === 'fields'
					? 'bg-base-100'
					: 'bg-base-200'}"
			>
				{#if editable && configView.value === 'fields' && flowSchema}
					<FlowForm bind:this={flowForm} bind:yaml schema={flowSchema} bind:picking />
				{:else if editable}
					<CodeEditor bind:value={yaml} extension="yaml" label={m.flow_config_editor_label()} />
				{:else if configView.value === 'fields'}
					<ConfigFields {yaml} onResourceClick={openResource} />
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
			bind:maximized
			fill={maximized || folded}
			onToggle={toggleActivity}
			onRowClick={onActivityRowClick}
		/>
	{/if}
</div>

<style>
	:global(.task-flash) {
		background: color-mix(in oklab, var(--color-secondary) 22%, transparent);
		border-radius: 3px;
		padding: 0 2px;
		animation: task-fade 1.6s ease-out forwards;
	}
	@keyframes task-fade {
		0% {
			background: color-mix(in oklab, var(--color-secondary) 30%, transparent);
		}
		100% {
			background: transparent;
		}
	}

</style>
