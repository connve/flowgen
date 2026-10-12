import { error } from '@sveltejs/kit';
import { apiUrl, encodePath, failure, type ResourceContent } from '$lib/api';
import { authoringTargets, pendingChanges, touchesResource } from '$lib/changes';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch }) => {
	const key = params.key;
	const [res, targets, pending] = await Promise.all([
		fetch(apiUrl(`api/resources/${encodePath(key)}`)),
		authoringTargets(fetch),
		pendingChanges(fetch)
	]);
	if (res.status === 404) error(404, m.resource_not_found({ key }));
	if (!res.ok) error(res.status, (await failure(res)).message);
	const resource: ResourceContent = await res.json();
	return {
		key,
		resource,
		targets,
		pending: pending.filter((change) => touchesResource(change, key))
	};
};
