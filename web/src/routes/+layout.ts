import { checkSession } from '$lib/auth.svelte';
import type { LayoutLoad } from './$types';

// The UI is a single-page app: the backend serves the built files and pages
// load their data from its API in the browser.
export const ssr = false;

export const load: LayoutLoad = async ({ fetch }) => {
	await checkSession(fetch);
};
