import { redirect } from '@sveltejs/kit';
import { base } from '$app/paths';
import type { PageLoad } from './$types';

export const load: PageLoad = ({ params }) => {
	redirect(308, params.id ? `${base}/agents/${encodeURIComponent(params.id)}` : `${base}/agents`);
};
