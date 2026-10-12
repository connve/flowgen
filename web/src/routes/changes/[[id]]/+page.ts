import { error } from '@sveltejs/kit';
import { apiUrl, failure, type Change, type ChangeSummary } from '$lib/api';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ params, fetch, depends }) => {
	depends('app:changes');
	if (params.id) {
		const id = params.id;
		const res = await fetch(apiUrl(`api/changes/${encodeURIComponent(id)}`));
		if (res.status === 404) error(404, m.change_not_found({ id }));
		if (!res.ok) error(res.status, (await failure(res)).message);
		const change: Change = await res.json();
		return { change, changes: [] };
	}
	const res = await fetch(apiUrl('api/changes'));
	if (res.status === 404) error(404, m.changes_not_enabled());
	if (!res.ok) error(res.status, (await failure(res)).message);
	const changes: ChangeSummary[] = (await res.json()).changes ?? [];
	return { change: null, changes };
};
