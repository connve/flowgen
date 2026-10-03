import {
	apiUrl,
	encodePath,
	type AuthoringTarget,
	type ChangeSummary,
	type ConfigInfo
} from '$lib/api';

// The running config, fetched once per page load and shared by every caller.
let configInfo: Promise<ConfigInfo | null> | null = null;

function loadConfigInfo(): Promise<ConfigInfo | null> {
	configInfo ??= fetch(apiUrl('api/config'))
		.then((res) => (res.ok ? (res.json() as Promise<ConfigInfo>) : null))
		.catch(() => null);
	return configInfo;
}

// Where changes can be proposed; empty when `web.authoring` is off.
export async function authoringTargets(): Promise<AuthoringTarget[]> {
	return (await loadConfigInfo())?.authoringTargets ?? [];
}

// Whether `web.authoring` is set, so New can propose changes.
export async function authoringEnabled(): Promise<boolean> {
	return (await loadConfigInfo())?.authoring ?? false;
}

// Whether `path` is `folder` or lies under it, compared on whole segments.
function within(path: string, folder: string): boolean {
	if (!path.startsWith(folder)) return false;
	const rest = path.slice(folder.length);
	return folder === '' || rest === '' || rest.startsWith('/');
}

// The target owning a workspace path: of the targets covering it, the one with
// the most specific folder, as the server decides.
export function targetOf(targets: AuthoringTarget[], path: string): AuthoringTarget | undefined {
	let owner: AuthoringTarget | undefined;
	let ownerLength = -1;
	for (const target of targets) {
		for (const prefix of target.paths) {
			const folder = prefix.replace(/\/+$/, '');
			if (within(path, folder) && folder.length >= ownerLength) {
				owner = target;
				ownerLength = folder.length;
			}
		}
	}
	return owner;
}

// Workspace path of a deployed flow; the file extension is not known, so `.yaml`.
export function flowFilePath(identity: string): string {
	return `flows/${identity}.yaml`;
}

// Link to the editor for a workspace path.
export function editUrl(base: string, path: string): string {
	return `${base}/edit?path=${encodeURIComponent(path)}`;
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
