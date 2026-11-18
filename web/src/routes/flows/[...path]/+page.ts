import { getFlow, loadError } from '$lib/api';
import { authoringTargets, pendingChanges, touchesFlow } from '$lib/changes';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch }) => {
	const identity = params.path;
	const [detail, targets, pending] = await Promise.all([
		getFlow({ path: { path: identity }, fetch }).then(
			({ data }) => data,
			(err) =>
				loadError(err, m.flow_load_failed({ path: identity }), m.flow_not_found({ path: identity }))
		),
		authoringTargets(fetch),
		pendingChanges(fetch)
	]);
	return {
		identity,
		detail,
		targets,
		pending: pending.filter((change) => touchesFlow(change, identity))
	};
};
