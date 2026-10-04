import type { ClientInit } from '@sveltejs/kit';
import { interceptUnauthorized } from '$lib/auth.svelte';

export const init: ClientInit = () => {
	interceptUnauthorized();
};
