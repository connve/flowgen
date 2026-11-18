import { base } from '$app/paths';
import { authMe } from '$lib/api';
import { redirect } from '@sveltejs/kit';
import type { PageLoad } from './$types';

// A signed-in user has nothing to sign in to. `/auth/me` never triggers the
// redirect to the identity provider.
export const load: PageLoad = async ({ fetch }) => {
	const signedIn = await authMe({ fetch }).then(
		() => true,
		() => false
	);
	if (signedIn) redirect(307, `${base}/`);
};
