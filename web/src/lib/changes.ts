import { apiUrl, encodePath, type ChangeSummary } from '$lib/api';

// Whether `web.authoring` is set, so New and Edit can propose changes.
export async function authoringEnabled(): Promise<boolean> {
	try {
		const res = await fetch(apiUrl('api/config'));
		if (!res.ok) return false;
		return (await res.json()).authoring === true;
	} catch {
		return false;
	}
}

// Link to the editor for a workspace path.
export function editUrl(base: string, path: string): string {
	return `${base}/edit?path=${encodeURIComponent(path)}`;
}

// Workspace path of a deployed flow; flows are proposed as `.yaml`.
export function flowFilePath(identity: string): string {
	return `flows/${identity}.yaml`;
}

// The deployed content behind a workspace path, or null when it is new.
export async function deployedContent(path: string): Promise<string | null> {
	const [dir, ...rest] = path.split('/');
	const tail = rest.join('/');
	let url: string;
	if (dir === 'flows') url = `api/flows/${encodePath(tail.replace(/\.(ya?ml|json)$/, ''))}`;
	else if (dir === 'resources') url = `api/resources/${encodePath(tail)}`;
	else return null;
	const res = await fetch(apiUrl(url));
	if (res.status === 404) return null;
	if (!res.ok) throw new Error(`HTTP ${res.status}`);
	const body = await res.json();
	return dir === 'flows' ? (body.yaml ?? null) : (body.content ?? null);
}

// Pending workspace changes, or none when authoring is not configured or the
// store is unreachable — callers only decorate pages with them.
export async function pendingChanges(): Promise<ChangeSummary[]> {
	try {
		const res = await fetch(apiUrl('api/changes'));
		if (!res.ok) return [];
		const changes: ChangeSummary[] = (await res.json()).changes ?? [];
		return changes.filter((change) => change.status === 'pending');
	} catch {
		return [];
	}
}

// Whether `change` touches a flow.
export function touchesFlows(change: ChangeSummary): boolean {
	return change.paths.some((path) => path.startsWith('flows/'));
}

// Whether `change` touches a resource.
export function touchesResources(change: ChangeSummary): boolean {
	return change.paths.some((path) => path.startsWith('resources/'));
}

// Whether `change` touches the resource with `key`.
export function touchesResource(change: ChangeSummary, key: string): boolean {
	return change.paths.includes(`resources/${key}`);
}

// Whether `change` touches the flow file for `identity` (any flow extension).
export function touchesFlow(change: ChangeSummary, identity: string): boolean {
	return change.paths.some((path) =>
		['yaml', 'yml', 'json'].some((ext) => path === `flows/${identity}.${ext}`)
	);
}
