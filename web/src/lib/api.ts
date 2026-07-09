// The mount prefix is baked into the SvelteKit build via `paths.base` in
// svelte.config.js (PUBLIC_BASE at build time). `base` therefore already
// matches `web.path` in flowgen config, so an API call is just `base +
// path`.

import { base } from '$app/paths';

export function apiUrl(path: string): string {
	const clean = path.startsWith('/') ? path : `/${path}`;
	return `${base}${clean}`;
}
