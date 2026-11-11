import {
	authoringTargets,
	baseContent,
	copyPath,
	draftChange,
	draftContent,
	pendingChanges,
	targetOf
} from '$lib/changes';
import { listHubs } from '$lib/hubs';
import type { PageLoad } from './$types';

// `/edit/<path>` edits that file; `/edit?new=flow` or `?new=resource` starts a
// new one, inside `&folder=` when given, or a copy of the file `&from=` names.
// A file already in the user's pending change, or in the pending change
// `?change=` names, opens with its pending content, and saving adds to that change.
export const load: PageLoad = async ({ params, url, fetch }) => {
	const file = params.path || null;
	const from = file ? null : url.searchParams.get('from');
	const [targets, previous, pending, available, copied] = await Promise.all([
		authoringTargets(fetch),
		file ? baseContent(file, fetch) : null,
		pendingChanges(fetch),
		file || from ? Promise.resolve([]) : listHubs(fetch),
		from ? baseContent(from, fetch) : null
	]);
	const copy =
		from && copied !== null ? { path: await copyPath(from, pending, fetch), content: copied } : null;
	const named = pending.find((change) => change.id === url.searchParams.get('change'));
	const draft = named ?? (file ? draftChange(pending, targetOf(targets, file)?.name) : undefined);
	const drafted =
		file && draft?.paths.includes(file) ? await draftContent(draft.id, file, fetch) : null;
	return {
		file,
		kind: url.searchParams.get('new') === 'resource' ? ('resource' as const) : ('flow' as const),
		folder: url.searchParams.get('folder'),
		targets,
		previous,
		pending,
		drafted,
		draftId: named?.id ?? null,
		hubs: available,
		copy
	};
};
