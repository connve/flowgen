import { error } from '@sveltejs/kit';
import { apiUrl, type Change, type ChangeSummary } from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch, depends }) => {
	depends('app:changes');
	if (params.id) {
		const res = await fetch(apiUrl(`api/changes/${encodeURIComponent(params.id)}`));
		if (!res.ok) {
			error(res.status, res.status === 404 ? `No change '${params.id}'` : 'Failed to load the change');
		}
		const change: Change = await res.json();
		return { change, changes: [] };
	}
	const res = await fetch(apiUrl('api/changes'));
	if (res.status === 404) error(404, 'Authoring is not configured (web.authoring).');
	if (!res.ok) error(res.status, 'Failed to load changes');
	const changes: ChangeSummary[] = (await res.json()).changes ?? [];
	return { change: null, changes };
};
