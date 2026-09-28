import { redirect } from '@sveltejs/kit';
import { base } from '$app/paths';

export function load() {
	redirect(307, `${base}/monitor/logs`);
}
