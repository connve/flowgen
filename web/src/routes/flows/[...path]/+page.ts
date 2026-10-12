import { error } from '@sveltejs/kit';
import { apiUrl, encodePath, errorMessage, failure, type FlowDetail } from '$lib/api';
import { authoringTargets, pendingChanges, touchesFlow } from '$lib/changes';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch }) => {
	const identity = params.path;
	const [res, targets, pending] = await Promise.all([
		fetch(apiUrl(`api/flows/${encodePath(identity)}`)).catch((err) =>
			error(503, errorMessage(err, m.flow_load_failed({ path: identity })))
		),
		authoringTargets(fetch),
		pendingChanges(fetch)
	]);
	if (res.status === 404) {
		error(404, m.flow_not_found({ path: identity }));
	}
	if (!res.ok) {
		error(res.status, (await failure(res)).message);
	}
	const detail: FlowDetail = await res.json();
	return {
		identity,
		detail,
		targets,
		pending: pending.filter((change) => touchesFlow(change, identity))
	};
};
