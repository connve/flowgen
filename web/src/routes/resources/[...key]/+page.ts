import { getResource, loadError } from '$lib/api';
import { authoringTargets, pendingChanges, touchesResource } from '$lib/changes';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch }) => {
	const key = params.key;
	const [resource, targets, pending] = await Promise.all([
		getResource({ path: { key }, fetch }).then(
			({ data }) => data,
			(err) => loadError(err, m.common_server_unreachable(), m.resource_not_found({ key }))
		),
		authoringTargets(fetch),
		pendingChanges(fetch)
	]);
	return {
		key,
		resource,
		targets,
		pending: pending.filter((change) => touchesResource(change, key))
	};
};
