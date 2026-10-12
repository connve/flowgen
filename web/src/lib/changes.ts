import { error } from '@sveltejs/kit';
import {
	apiUrl,
	encodePath,
	failure,
	type AuthoringTarget,
	type Change,
	type ChangeSummary,
	type ConfigInfo
} from '$lib/api';
import { auth } from '$lib/auth.svelte';
import { m } from '$lib/paraglide/messages.js';

// The running config, fetched once and shared by every caller; a failed fetch
// is retried by the next caller.
let configInfo: Promise<ConfigInfo | null> | null = null;

function loadConfigInfo(fetcher: typeof fetch): Promise<ConfigInfo | null> {
	configInfo ??= fetcher(apiUrl('api/config'))
		.then((res) => (res.ok ? (res.json() as Promise<ConfigInfo>) : null))
		.catch(() => null)
		.then((info) => {
			if (info === null) configInfo = null;
			return info;
		});
	return configInfo;
}

// Where changes can be proposed; empty when `web.authoring` is off.
export async function authoringTargets(fetcher: typeof fetch = fetch): Promise<AuthoringTarget[]> {
	return (await loadConfigInfo(fetcher))?.authoringTargets ?? [];
}

export async function authoringEnabled(fetcher: typeof fetch = fetch): Promise<boolean> {
	return (await loadConfigInfo(fetcher))?.authoring ?? false;
}

// Null when the config did not load.
export async function agentsEnabled(fetcher: typeof fetch = fetch): Promise<boolean | null> {
	return (await loadConfigInfo(fetcher))?.agents ?? null;
}

// How the server labels the current user on a change: the email claim, else
// the user id; `anonymous` without sign-in.
export function ownLabel(): string {
	const user = auth.user;
	if (!user) return 'anonymous';
	const email = user.claims.email;
	return typeof email === 'string' && email.trim() !== '' ? email.trim() : user.user_id;
}

// The current user's most recently edited pending change in `target`, which
// the editor adds files to.
export function draftChange(
	pending: ChangeSummary[],
	target: string | undefined
): ChangeSummary | undefined {
	const me = ownLabel();
	const edited = (change: ChangeSummary) => change.updatedAt ?? change.createdAt;
	let latest: ChangeSummary | undefined;
	for (const change of pending) {
		if (change.target !== target || change.proposedBy !== me) continue;
		if (!latest || edited(change) > edited(latest)) latest = change;
	}
	return latest;
}

// A change with its files, or null when it cannot be loaded.
export async function loadChange(id: string, fetcher: typeof fetch = fetch): Promise<Change | null> {
	const res = await fetcher(apiUrl(`api/changes/${encodeURIComponent(id)}`));
	return res.ok ? res.json() : null;
}

// The content `path` has in a pending change, or null when the change deletes it.
export async function draftContent(
	id: string,
	path: string,
	fetcher: typeof fetch = fetch
): Promise<string | null> {
	const change = await loadChange(id, fetcher);
	return change?.files.find((file) => file.path === path)?.content ?? null;
}

type TitledFile = { path: string; content?: string | null; previous?: string | null };

function fileTitle(file: TitledFile): string {
	if (file.content == null) return m.editor_change_title_delete({ path: file.path });
	if (file.previous == null) return m.editor_change_title_add({ path: file.path });
	return m.editor_change_title_update({ path: file.path });
}

// The title the editor gives a change: its first file, then how many more.
export function generatedTitle(files: TitledFile[]): string {
	const [first, ...rest] = files;
	if (!first) return '';
	return rest.length === 0
		? fileTitle(first)
		: m.change_title_more({ title: fileTitle(first), count: rest.length });
}

// Who proposed or decided a change, from the server's label: `key:<name>` for a
// machine key, `anonymous` without sign-in (null), else the user.
export function actor(label: string): { name: string; agent: boolean } | null {
	if (label === 'anonymous') return null;
	if (label.startsWith('key:')) return { name: label.slice('key:'.length), agent: true };
	return { name: label, agent: false };
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

export function stripFlowExtension(path: string): string {
	return path.replace(/\.(ya?ml|json)$/, '');
}

export function editUrl(base: string, path: string): string {
	return `${base}/edit/${encodePath(path)}`;
}

// The version a change to `path` is based on: what runs, with published
// changes not deployed yet on top; null when the file does not exist.
export async function baseContent(
	path: string,
	fetcher: typeof fetch = fetch
): Promise<string | null> {
	const res = await fetcher(apiUrl(`api/workspace/file?path=${encodeURIComponent(path)}`));
	if (res.status === 404) return null;
	if (!res.ok) error(res.status, (await failure(res)).message);
	const body = await res.json();
	return body.content ?? null;
}

// Pending workspace changes, or none when authoring is not configured or the
// store is unreachable — callers only decorate pages with them.
export async function pendingChanges(fetcher: typeof fetch = fetch): Promise<ChangeSummary[]> {
	try {
		const res = await fetcher(apiUrl('api/changes'));
		if (!res.ok) return [];
		const changes: ChangeSummary[] = (await res.json()).changes ?? [];
		return changes.filter((change) => change.status === 'pending');
	} catch {
		return [];
	}
}

export function touchesResource(change: ChangeSummary, key: string): boolean {
	return change.paths.includes(`resources/${key}`);
}

// Whether `change` touches the flow file for `identity`, whatever its extension.
export function touchesFlow(change: ChangeSummary, identity: string): boolean {
	return change.paths.some((path) =>
		['yaml', 'yml', 'json'].some((ext) => path === `flows/${identity}.${ext}`)
	);
}
