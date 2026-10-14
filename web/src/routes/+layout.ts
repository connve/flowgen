import { checkSession } from '$lib/auth.svelte';
import { agentsEnabled } from '$lib/changes';
import { loadConnectors } from '$lib/connectors';
import type { LayoutLoad } from './$types';

// The UI is a single-page app: the backend serves the built files and pages
// load their data from its API in the browser.
export const ssr = false;

// The login page calls no API, since a 401 would send the browser back to the
// identity provider it just returned from.
export const load: LayoutLoad = async ({ fetch, route }) => {
	if (route.id === '/login') return { agents: false, login: true };
	await checkSession(fetch);
	const [agents] = await Promise.all([agentsEnabled(fetch), loadConnectors(fetch)]);
	return { agents, login: false };
};
