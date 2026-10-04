import { apiUrl, type UserContext } from '$lib/api';

// `user` is null both when logged out and when auth is not configured;
// `enabled` tells them apart (GET /auth/me 404s without `web.auth`), since
// only a logged-out user with auth enabled should be sent to /auth/login.
// `checked` turns true once the UI may render.
export const auth = $state<{ user: UserContext | null; enabled: boolean; checked: boolean }>({
	user: null,
	enabled: false,
	checked: false
});

let resolveChecked: () => void;
const sessionChecked = new Promise<void>((resolve) => {
	resolveChecked = resolve;
});

// When `web.auth` is configured the API answers 401 once the session is gone
// and cannot be refreshed; every such response goes to the login page. It is
// installed before any page loads. A 401 that lands before the session check
// waits for it, and nothing redirects without auth enabled: a 401 can also
// come from an upstream, such as an AI provider rejecting its key.
export function interceptUnauthorized() {
	const authPrefix = `${apiUrl('auth')}/`;
	const realFetch = window.fetch.bind(window);
	window.fetch = async (...args: Parameters<typeof fetch>) => {
		const response = await realFetch(...args);
		const url = args[0] instanceof Request ? args[0].url : String(args[0]);
		if (response.status === 401 && !url.startsWith(authPrefix)) {
			await sessionChecked;
			if (auth.enabled) {
				const here = window.location.pathname + window.location.search + window.location.hash;
				window.location.href = `${apiUrl('auth/login')}?return_to=${encodeURIComponent(here)}`;
			}
		}
		return response;
	};
}

export async function checkSession(fetcher: typeof fetch) {
	try {
		const res = await fetcher(apiUrl('auth/me'));
		auth.enabled = res.status !== 404;
		if (res.ok) auth.user = await res.json();
	} catch {
		// Without an answer the user counts as logged out and nothing redirects.
	} finally {
		resolveChecked();
		// A logged-out user with auth enabled is about to be redirected, so
		// the UI stays blank instead of flashing before the navigation.
		if (!auth.enabled || auth.user) auth.checked = true;
	}
}
