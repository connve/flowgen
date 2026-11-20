// A flow file edited in place: each change touches only the value it sets,
// so comments, order and formatting of the rest stay as they were.

import { isMap, isSeq, parseDocument, type Document, type YAMLMap, type YAMLSeq } from 'yaml';

const TASKS = ['flow', 'tasks'];
const DEPENDS_ON = 'depends_on';

/** Prints as flows are written: long lines whole, `[a, b]` without padding. */
const PRINT = { lineWidth: 0, flowCollectionPadding: false };

export type Path = (string | number)[];

export interface Task {
	/** The YAML tag, such as `http_request`. */
	type: string;
	name: string;
	/** The task's settings, as plain values. */
	values: Record<string, unknown>;
}

export class FlowDocument {
	#doc: Document;

	constructor(source: string) {
		this.#doc = parseDocument(source);
	}

	/** Whether the YAML parsed; a form edits only a document that did. */
	get valid(): boolean {
		return this.#doc.errors.length === 0;
	}

	toString(): string {
		return this.#doc.toString(PRINT);
	}

	/** A value of the flow, such as `['labels', 'display_name']`. */
	flowValue(path: Path): unknown {
		return toJs(this.#doc.getIn(['flow', ...path]));
	}

	/** Sets a value of the flow; `undefined` removes it. */
	setFlowValue(path: Path, value: unknown): void {
		this.#set(['flow', ...path], value);
	}

	tasks(): Task[] {
		return this.#taskItems().map((item) => {
			const [type, settings] = entryOf(item);
			const values = (toJs(settings) ?? {}) as Record<string, unknown>;
			return { type, name: String(values.name ?? ''), values };
		});
	}

	/** Sets a setting of the task at `index`; `undefined` removes it. */
	setTaskValue(index: number, path: Path, value: unknown): void {
		const [type] = entryOf(this.#taskItems()[index]);
		this.#set([...TASKS, index, type, ...path], value);
	}

	/** Adds a task of `type` after the task at `after`, or first. */
	addTask(type: string, name: string, after: number | null): number {
		const tasks = this.#doc.getIn(TASKS, true);
		const node = this.#doc.createNode({ [type]: { name } });
		const index = after === null ? 0 : after + 1;
		if (isSeq(tasks)) {
			(tasks as YAMLSeq).items.splice(index, 0, node);
		} else {
			this.#doc.setIn(TASKS, this.#doc.createNode([{ [type]: { name } }]));
		}
		return index;
	}

	/** Removes the task at `index`; tasks that took events from it take them
	 * from where it took its own. */
	removeTask(index: number): void {
		const tasks = this.tasks();
		const removed = tasks[index];
		const parents = parentsOf(tasks, index);
		tasks.forEach((task, at) => {
			const dependsOn = task.values[DEPENDS_ON];
			if (at === index || !Array.isArray(dependsOn) || !dependsOn.includes(removed.name)) return;
			const next = dependsOn.flatMap((name) => (name === removed.name ? parents : [name]));
			this.setTaskValue(at, [DEPENDS_ON], [...new Set(next)]);
		});
		this.#doc.deleteIn([...TASKS, index]);
	}

	/** Renames the task at `index`, and the `depends_on` that name it. */
	renameTask(index: number, name: string): void {
		const tasks = this.tasks();
		const previous = tasks[index].name;
		this.setTaskValue(index, ['name'], name);
		tasks.forEach((task, at) => {
			const dependsOn = task.values[DEPENDS_ON];
			if (at === index || !Array.isArray(dependsOn) || !dependsOn.includes(previous)) return;
			this.setTaskValue(
				at,
				[DEPENDS_ON],
				dependsOn.map((dependency) => (dependency === previous ? name : dependency))
			);
		});
	}

	#taskItems(): YAMLMap[] {
		const tasks = this.#doc.getIn(TASKS, true);
		return isSeq(tasks) ? ((tasks as YAMLSeq).items.filter(isMap) as YAMLMap[]) : [];
	}

	#set(path: Path, value: unknown): void {
		if (value === undefined) {
			this.#doc.deleteIn(path);
			return;
		}
		this.#doc.setIn(path, value);
	}
}

/** The tasks the task at `index` takes events from: its `depends_on`, else
 * the task before it. */
export function parentsOf(tasks: Task[], index: number): string[] {
	const dependsOn = tasks[index].values[DEPENDS_ON];
	if (Array.isArray(dependsOn)) return dependsOn.map(String);
	return index > 0 ? [tasks[index - 1].name] : [];
}

function entryOf(item: YAMLMap): [string, unknown] {
	const pair = item.items[0];
	return [String(toJs(pair?.key) ?? ''), pair?.value];
}

function toJs(node: unknown): unknown {
	if (node && typeof node === 'object' && 'toJSON' in node) {
		return (node as { toJSON: () => unknown }).toJSON();
	}
	return node;
}
