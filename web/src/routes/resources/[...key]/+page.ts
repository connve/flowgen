import { error } from '@sveltejs/kit';
import { apiUrl, encodePath, type ResourceContent } from '$lib/api';
import { authoringTargets, pendingChanges, touchesResource } from '$lib/changes';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch }) => {
	const key = params.key;
	const [res, targets, pending] = await Promise.all([
		fetch(apiUrl(`api/resources/${encodePath(key)}`)),
		authoringTargets(fetch),
		pendingChanges(fetch)
	]);
	if (!res.ok) {
		error(res.status, res.status === 404 ? `No resource '${key}'` : `Failed to load resource '${key}'`);
	}
	const resource: ResourceContent = await res.json();
	return {
		key,
		resource,
		targets,
		pending: pending.filter((change) => touchesResource(change, key))
	};
};
