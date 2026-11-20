<script lang="ts">
	import Icon from '@iconify/svelte';
	import { onMount } from 'svelte';
	import { listConnections, listResources } from '$lib/api';
	import { connectorFor } from '$lib/connectors';
	import { FlowDocument, type Path } from '$lib/flowDocument';
	import SchemaField, { type FormContext } from '$lib/form/SchemaField.svelte';
	import TaskPicker from '$lib/form/TaskPicker.svelte';
	import { fieldsOf, taskSchemas, type Field, type Schema } from '$lib/form/schema';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		yaml: string;
		schema: Schema;
		/** The task to open, such as one clicked in the graph. */
		focusTask?: string | null;
	}

	let { yaml = $bindable(), schema, focusTask = null }: Props = $props();

	let flow = $derived(new FlowDocument(yaml));
	let tasks = $derived(flow.valid ? flow.tasks() : []);
	let schemas = $derived(taskSchemas(schema));
	let known = $derived(new Set(schemas.keys()));

	let open = $state<string | null>(null);
	let picking = $state<number | null | false>(false);
	let renaming = $state<number | null>(null);
	let renameDraft = $state('');
	let resources = $state<string[]>([]);
	let credentials = $state<string[]>([]);

	onMount(async () => {
		const [found, connections] = await Promise.allSettled([listResources(), listConnections()]);
		if (found.status === 'fulfilled') resources = found.value.data.map((resource) => resource.key);
		if (connections.status === 'fulfilled') {
			const names = connections.value.data.flatMap((connection) =>
				connection.credentialsName ? [connection.credentialsName] : []
			);
			credentials = [...new Set(names)].sort();
		}
	});

	$effect(() => {
		if (focusTask) open = focusTask;
	});

	/** The language of a task's inline code, by its type. */
	function languageOf(taskType: string): string | null {
		if (taskType === 'script') return 'rhai';
		if (/(^|_)(query|sql|cdc|write)(_|$)/.test(taskType)) return 'sql';
		if (taskType.startsWith('mcp_') || taskType.startsWith('ai_')) return 'md';
		return null;
	}

	function contextFor(taskType: string): FormContext {
		return { resources, credentials, language: languageOf(taskType) };
	}

	function edit(change: (flow: FlowDocument) => void) {
		const next = new FlowDocument(yaml);
		change(next);
		yaml = next.toString();
	}

	function setTask(index: number, path: Path, value: unknown) {
		edit((next) => next.setTaskValue(index, path, value));
	}

	function fieldsFor(taskType: string): { main: Field[]; advanced: Field[] } {
		const taskSchema = schemas.get(taskType);
		const fields = taskSchema ? fieldsOf(schema, taskSchema).filter((field) => field.name !== 'name') : [];
		return {
			main: fields.filter((field) => !field.advanced),
			advanced: fields.filter((field) => field.advanced)
		};
	}

	/** A name for a new task of `taskType` that no task has yet. */
	function freshName(taskType: string): string {
		const names = new Set(tasks.map((task) => task.name));
		const base = taskType.split('_').pop() ?? taskType;
		if (!names.has(base)) return base;
		let suffix = 2;
		while (names.has(`${base}_${suffix}`)) suffix++;
		return `${base}_${suffix}`;
	}

	function addTask(taskType: string) {
		const after = picking === false ? null : picking;
		const name = freshName(taskType);
		edit((next) => next.addTask(taskType, name, after));
		picking = false;
		open = name;
	}

	function rename(index: number) {
		const name = renameDraft.trim();
		if (name && name !== tasks[index].name && !tasks.some((task) => task.name === name)) {
			edit((next) => next.renameTask(index, name));
			open = name;
		}
		renaming = null;
	}

	const FLOW_LABELS: { key: string; kind: 'string' | 'list' }[] = [
		{ key: 'display_name', kind: 'string' },
		{ key: 'description', kind: 'string' },
		{ key: 'tags', kind: 'list' }
	];
</script>

{#if !flow.valid}
	<p class="p-4 text-sm text-base-content/60">{m.form_yaml_invalid()}</p>
{:else}
	<div class="space-y-6 p-4">
		<section class="space-y-3">
			{#each FLOW_LABELS as label (label.key)}
				<SchemaField
					field={{ name: label.key, schema: { type: label.kind === 'list' ? 'array' : 'string' }, kind: label.kind, required: false, advanced: false }}
					root={schema}
					value={flow.flowValue(['labels', label.key])}
					context={contextFor('')}
					onchange={(value) => edit((next) => next.setFlowValue(['labels', label.key], value))}
				/>
			{/each}
		</section>

		<section class="space-y-2">
			<h3 class="text-xs font-medium text-base-content/50">{m.form_tasks()}</h3>
			{#each tasks as task, index (task.name + index)}
				{@const connector = connectorFor(task.type)}
				{@const fields = fieldsFor(task.type)}
				<div id="form-task-{task.name}" class="rounded-md border border-base-200">
					<div class="flex h-10 items-center gap-2 px-2">
						<button
							type="button"
							class="flex min-w-0 flex-1 items-center gap-2 text-left"
							aria-expanded={open === task.name}
							onclick={() => (open = open === task.name ? null : task.name)}
						>
							<img src={connector.iconUrl} alt="" class="h-4 w-4 shrink-0" />
							{#if renaming === index}
								<input
									class="input input-xs w-full border border-base-300 bg-base-100 outline-none focus:border-primary"
									aria-label={m.form_task_rename()}
									bind:value={renameDraft}
									onclick={(e) => e.stopPropagation()}
									onkeydown={(e) => {
										if (e.key === 'Enter') rename(index);
										if (e.key === 'Escape') renaming = null;
									}}
									onblur={() => rename(index)}
								/>
							{:else}
								<span class="truncate text-sm font-medium">{task.name}</span>
								<span class="truncate font-mono text-xs text-base-content/50">{task.type}</span>
							{/if}
						</button>
						<div class="dropdown dropdown-end">
							<button type="button" tabindex="0" class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 hover:bg-base-200 hover:text-base-content" aria-label={m.editor_more_actions()}>
								<Icon icon="tabler:dots" class="h-5 w-5" />
							</button>
							<ul tabindex="-1" class="menu dropdown-content z-10 mt-1 w-48 rounded-md border border-base-200 bg-base-100 p-1 text-sm shadow-lg">
								<li>
									<button type="button" onclick={() => { renaming = index; renameDraft = task.name; }}>
										<Icon icon="tabler:pencil" class="h-4 w-4" />
										{m.form_task_rename()}
									</button>
								</li>
								<li>
									<button type="button" onclick={() => (picking = index)}>
										<Icon icon="tabler:plus" class="h-4 w-4" />
										{m.form_add_task_after()}
									</button>
								</li>
								<li>
									<button type="button" class="text-error" onclick={() => edit((next) => next.removeTask(index))}>
										<Icon icon="tabler:trash" class="h-4 w-4" />
										{m.form_task_remove()}
									</button>
								</li>
							</ul>
						</div>
					</div>
					{#if open === task.name}
						<div class="space-y-4 border-t border-base-200 p-3">
							{#each fields.main as field (field.name)}
								<SchemaField {field} root={schema} value={task.values[field.name]} context={contextFor(task.type)} onchange={(value) => setTask(index, [field.name], value)} />
							{/each}
							{#if fields.advanced.length}
								<details class="group">
									<summary class="flex cursor-pointer list-none items-center gap-1.5 text-xs font-medium text-base-content/50">
										<Icon icon="tabler:chevron-right" class="h-3.5 w-3.5 transition-transform group-open:rotate-90" />
										{m.form_advanced()}
									</summary>
									<div class="mt-3 space-y-4">
										{#each fields.advanced as field (field.name)}
											<SchemaField {field} root={schema} value={task.values[field.name]} context={contextFor(task.type)} onchange={(value) => setTask(index, [field.name], value)} />
										{/each}
									</div>
								</details>
							{/if}
						</div>
					{/if}
				</div>
			{/each}
			<button type="button" class="flex h-9 items-center gap-1.5 rounded-md px-2 text-sm text-base-content/70 hover:bg-base-200" onclick={() => (picking = tasks.length ? tasks.length - 1 : null)}>
				<Icon icon="tabler:plus" class="h-4 w-4" />
				{m.form_add_task()}
			</button>
		</section>
	</div>
{/if}

{#if picking !== false}
	<TaskPicker {known} onpick={addTask} onclose={() => (picking = false)} />
{/if}
