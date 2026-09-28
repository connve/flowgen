import { redirect } from '@sveltejs/kit';
import { base } from '$app/paths';
import type { PageLoad } from './$types';

export const load: PageLoad = ({ url }) => {
	redirect(307, `${base}/monitor/logs${url.search}`);
};
