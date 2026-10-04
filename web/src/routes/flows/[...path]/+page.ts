import { error } from '@sveltejs/kit';
import { apiUrl, encodePath, type FlowDetail } from '$lib/api';
import { authoringTargets, pendingChanges, touchesFlow } from '$lib/changes';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch }) => {
	const identity = params.path;
	const [res, targets, pending] = await Promise.all([
		fetch(apiUrl(`api/flows/${encodePath(identity)}`)),
		authoringTargets(fetch),
		pendingChanges(fetch)
	]);
	if (!res.ok) {
		error(res.status, res.status === 404 ? `No flow '${identity}'` : `Failed to load flow '${identity}'`);
	}
	const detail: FlowDetail = await res.json();
	return {
		identity,
		detail,
		targets,
		pending: pending.filter((change) => touchesFlow(change, identity))
	};
};
