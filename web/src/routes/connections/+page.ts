import { apiUrl, errorMessage, failure, type Connection } from '$lib/api';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

// Returns the error instead of throwing so the page keeps refreshing.
export const load: PageLoad = async ({ fetch, depends }) => {
	depends('app:connections');
	try {
		const res = await fetch(apiUrl('api/connections'));
		if (!res.ok) return { connections: null, error: (await failure(res)).message };
		const connections: Connection[] = await res.json();
		return { connections, error: null };
	} catch (err) {
		return { connections: null, error: errorMessage(err, m.connections_load_failed()) };
	}
};
