import { base } from '$app/paths';
import { redirect } from '@sveltejs/kit';
import { encodePath } from '$lib/api';
import { listHubs } from '$lib/hubs';
import type { PageLoad } from './$types';

// `/new?kind=flow` or `?kind=resource` starts a new file from a hub's
// template, inside `&folder=` when given; without a hub, from a blank file.
export const load: PageLoad = async ({ url, fetch }) => {
	const kind = url.searchParams.get('kind') === 'resource' ? ('resource' as const) : ('flow' as const);
	const folder = url.searchParams.get('folder');
	const hubs = await listHubs(fetch);
	if (hubs.length === 0) {
		redirect(307, `${base}/edit?new=${kind}${folder ? `&folder=${encodePath(folder)}` : ''}`);
	}
	return { kind, folder, hubs };
};
