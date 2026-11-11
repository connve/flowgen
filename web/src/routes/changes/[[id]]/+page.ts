import { getChange, listChanges, loadError } from '$lib/api';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch, depends }) => {
	depends('app:changes');
	if (params.id) {
		const id = params.id;
		const change = await getChange({ path: { id }, fetch }).then(
			({ data }) => data,
			(err) => loadError(err, m.common_server_unreachable(), m.change_not_found({ id }))
		);
		return { change, changes: [] };
	}
	const changes = await listChanges({ fetch }).then(
		({ data }) => data.changes ?? [],
		(err) => loadError(err, m.common_server_unreachable(), m.changes_not_enabled())
	);
	return { change: null, changes };
};
