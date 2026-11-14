import { errorMessage, listConnections } from '$lib/api';
import { credentialsEnabled } from '$lib/changes';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

// Returns the error instead of throwing so the page keeps refreshing.
export const load: PageLoad = async ({ fetch, depends }) => {
	depends('app:connections');
	const credentials = await credentialsEnabled(fetch);
	try {
		return { connections: (await listConnections({ fetch })).data, error: null, credentials };
	} catch (err) {
		return {
			connections: null,
			error: errorMessage(err, m.connections_load_failed()),
			credentials
		};
	}
};
