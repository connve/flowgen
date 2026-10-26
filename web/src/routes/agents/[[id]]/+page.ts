import { error } from '@sveltejs/kit';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ parent }) => {
	const { agents } = await parent();
	if (agents === null) error(503, m.common_config_unavailable());
	if (!agents) error(404, m.agents_not_enabled());
};
