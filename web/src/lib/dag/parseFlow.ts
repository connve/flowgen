import { load } from 'js-yaml';

export interface DagNode {
	id: string;
	name: string;
	taskType: string;
}

export interface DagEdge {
	id: string;
	source: string;
	target: string;
}

export interface Dag {
	nodes: DagNode[];
	edges: DagEdge[];
}

// A parsed task carries every YAML key of its top-level object plus the
// task type that owned it (`{ oci_sync: {...} }` → taskType `oci_sync`).
interface RawTask {
	taskType: string;
	name: string;
	dependsOn?: string[];
}

interface FlowRoot {
	flow?: { tasks?: unknown };
}

function asStringArray(v: unknown): string[] | undefined {
	if (!Array.isArray(v)) return undefined;
	const out: string[] = [];
	for (const item of v) {
		if (typeof item === 'string') out.push(item);
	}
	return out;
}

function parseTasks(raw: unknown): RawTask[] {
	if (!Array.isArray(raw)) return [];
	const tasks: RawTask[] = [];
	for (const entry of raw) {
		if (!entry || typeof entry !== 'object') continue;
		// Flowgen tasks are single-key objects: `{ oci_sync: {name, ...} }`.
		const keys = Object.keys(entry as Record<string, unknown>);
		if (keys.length === 0) continue;
		const taskType = keys[0];
		const body = (entry as Record<string, unknown>)[taskType];
		if (!body || typeof body !== 'object') continue;
		const b = body as Record<string, unknown>;
		const name = typeof b.name === 'string' ? b.name : `${taskType}_${tasks.length}`;
		tasks.push({ taskType, name, dependsOn: asStringArray(b.depends_on) });
	}
	return tasks;
}

// depends_on semantics: explicit list → those edges. No depends_on and not the
// first task → implicit edge from previous task. Matches flowgen builder.
export function parseFlow(yaml: string): Dag {
	let doc: FlowRoot;
	try {
		doc = load(yaml) as FlowRoot;
	} catch {
		return { nodes: [], edges: [] };
	}

	const tasks = parseTasks(doc?.flow?.tasks);
	const nodes: DagNode[] = tasks.map((t) => ({ id: t.name, name: t.name, taskType: t.taskType }));

	const edges: DagEdge[] = [];
	tasks.forEach((task, i) => {
		if (task.dependsOn && task.dependsOn.length > 0) {
			for (const parent of task.dependsOn) {
				edges.push({ id: `${parent}->${task.name}`, source: parent, target: task.name });
			}
		} else if (i > 0) {
			const parent = tasks[i - 1].name;
			edges.push({ id: `${parent}->${task.name}`, source: parent, target: task.name });
		}
	});

	return { nodes, edges };
}
