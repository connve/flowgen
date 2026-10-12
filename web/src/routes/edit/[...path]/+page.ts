import {
	authoringTargets,
	baseContent,
	draftChange,
	draftContent,
	pendingChanges,
	targetOf
} from '$lib/changes';
import type { PageLoad } from './$types';

// `/edit/<path>` edits that file; `/edit?new=flow` or `?new=resource` starts a
// new one, inside `&folder=` when given. A file already in the user's pending
// change opens with its pending content.
export const load: PageLoad = async ({ params, url, fetch }) => {
	const file = params.path || null;
	const [targets, previous, pending] = await Promise.all([
		authoringTargets(fetch),
		file ? baseContent(file, fetch) : null,
		pendingChanges(fetch)
	]);
	const draft = file ? draftChange(pending, targetOf(targets, file)?.name) : undefined;
	const drafted =
		file && draft?.paths.includes(file) ? await draftContent(draft.id, file, fetch) : null;
	return {
		file,
		kind: url.searchParams.get('new') === 'resource' ? ('resource' as const) : ('flow' as const),
		folder: url.searchParams.get('folder'),
		targets,
		previous,
		pending,
		drafted
	};
};
