import { authoringTargets, deployedContent } from '$lib/changes';
import type { PageLoad } from './$types';

// `/edit/<path>` edits that file; `/edit?new=flow` or `?new=resource` starts a
// new one, inside `&folder=` when given.
export const load: PageLoad = async ({ params, url, fetch }) => {
	const file = params.path || null;
	const [targets, previous] = await Promise.all([
		authoringTargets(fetch),
		file ? deployedContent(file, fetch) : null
	]);
	return {
		file,
		kind: url.searchParams.get('new') === 'resource' ? ('resource' as const) : ('flow' as const),
		folder: url.searchParams.get('folder'),
		targets,
		previous
	};
};
