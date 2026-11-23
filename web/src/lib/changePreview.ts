// What a change does to a flow: the tasks it adds, removes or changes, the
// wiring between them, and each task's settings before and after. Tasks match
// by name, so a renamed task shows as removed and added.

import { FlowDocument, parentsOf, type Task } from '$lib/flowDocument';
import type { Dag, DagEdge, DagNode, Mark } from '$lib/dag/parseFlow';
import { m } from '$lib/paraglide/messages.js';

export function markLabel(mark: Mark): string {
	switch (mark) {
		case 'added':
			return m.change_task_added();
		case 'removed':
			return m.change_task_removed();
		case 'changed':
			return m.change_task_changed();
	}
}

export const TAKES_EVENTS_FROM = 'takes_events_from';

export interface FieldChange {
	key: string;
	before: unknown;
	after: unknown;
}

export interface FlowComparison {
	/** The new flow's graph, with removed tasks and their wiring kept where they were. */
	dag: Dag;
	tasks: Map<string, { mark: Mark; fields: FieldChange[] }>;
}

/** Compares a flow with its previous version, `null` for a new file; `null`
 * when either does not parse. */
export function compareFlows(previous: string | null, current: string): FlowComparison | null {
	const before = previous === null ? [] : tasksOf(previous);
	const after = tasksOf(current);
	if (before === null || after === null) return null;

	const old = new Map(before.map((task) => [task.name, task]));
	const kept = new Set(after.map((task) => task.name));
	const tasks = new Map<string, { mark: Mark; fields: FieldChange[] }>();

	const nodes: DagNode[] = after.map((task, index) => {
		const was = old.get(task.name);
		const fields = fieldChanges(was ?? null, task);
		if (was) {
			const from = parentsOf(before, before.indexOf(was));
			const to = parentsOf(after, index);
			if (from.join() !== to.join()) fields.unshift({ key: TAKES_EVENTS_FROM, before: from, after: to });
		}
		const mark: Mark | undefined = !was ? 'added' : fields.length > 0 ? 'changed' : undefined;
		if (mark) tasks.set(task.name, { mark, fields });
		return { id: task.name, name: task.name, taskType: task.type, mark };
	});
	const removed = new Set<string>();
	for (const task of before) {
		if (kept.has(task.name)) continue;
		removed.add(task.name);
		tasks.set(task.name, { mark: 'removed', fields: fieldChanges(task, null) });
		nodes.push({ id: task.name, name: task.name, taskType: task.type, mark: 'removed' });
	}

	const added = (name: string) => previous !== null && !old.has(name);
	const edges: DagEdge[] = [
		...edgesOf(after).map((edge) => (added(edge.target) ? { ...edge, mark: 'added' as const } : edge)),
		...edgesOf(before)
			.filter((edge) => removed.has(edge.source) || removed.has(edge.target))
			.map((edge) => ({ ...edge, mark: 'removed' as const }))
	];
	return { dag: { nodes, edges }, tasks };
}

function tasksOf(source: string): Task[] | null {
	const flow = new FlowDocument(source);
	return flow.valid ? flow.tasks() : null;
}

function edgesOf(tasks: Task[]): DagEdge[] {
	return tasks.flatMap((task, index) =>
		parentsOf(tasks, index).map((parent) => ({ id: `${parent}->${task.name}`, source: parent, target: task.name }))
	);
}

/** The settings that differ, in the new task's order, then the removed ones. */
function fieldChanges(before: Task | null, after: Task | null): FieldChange[] {
	const changes: FieldChange[] = [];
	if (before && after && before.type !== after.type) {
		changes.push({ key: 'type', before: before.type, after: after.type });
	}
	const keys = new Set([...Object.keys(after?.values ?? {}), ...Object.keys(before?.values ?? {})]);
	for (const key of keys) {
		if (key === 'name' || key === 'depends_on') continue;
		const was = before?.values[key];
		const is = after?.values[key];
		if (JSON.stringify(was) !== JSON.stringify(is)) changes.push({ key, before: was, after: is });
	}
	return changes;
}
