import { base } from '$app/paths';
import { apiUrl } from '$lib/api';
import { redirect } from '@sveltejs/kit';
import type { PageLoad } from './$types';

// A signed-in user has nothing to sign in to. `/auth/me` never triggers the
// redirect to the identity provider.
export const load: PageLoad = async ({ fetch }) => {
	let signedIn = false;
	try {
		signedIn = (await fetch(apiUrl('auth/me'))).ok;
	} catch {
		// Unreachable server: stay on the page.
	}
	if (signedIn) redirect(307, `${base}/`);
};
