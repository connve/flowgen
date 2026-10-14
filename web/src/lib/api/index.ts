import { base } from '$app/paths';
import { m } from '$lib/paraglide/messages.js';
import type { components } from './generated';

// The mount prefix is baked into the SvelteKit build via `paths.base` in
// svelte.config.js (PUBLIC_BASE at build time). `base` therefore already
// matches `web.path` in flowgen config, so an API call is just `base +
// path`.
export function apiUrl(path: string): string {
	const clean = path.startsWith('/') ? path : `/${path}`;
	return `${base}${clean}`;
}

// URL-encode a slash-delimited flow or resource path so it survives fetch and goto.
export function encodePath(path: string): string {
	return path.split('/').map(encodeURIComponent).join('/');
}

// A failed response as the server explains it: its text, or the `error.message`
// of a JSON body. Without either, the status and where to look.
export async function failure(res: Response): Promise<Error> {
	const body = (await res.text().catch(() => '')).trim();
	return new Error(serverReason(body) ?? m.common_failed_with_status({ status: res.status }));
}

function serverReason(body: string): string | null {
	if (body === '' || body.startsWith('<')) return null;
	try {
		const json = JSON.parse(body);
		const message = json?.error?.message ?? json?.message ?? json?.error;
		return typeof message === 'string' && message !== '' ? message : null;
	} catch {
		return body;
	}
}

// What to show for a failed request; browsers word a lost connection as
// "Failed to fetch", "Load failed" or "NetworkError…".
export function errorMessage(err: unknown, fallback: string): string {
	if (err instanceof TypeError && /^(Failed to fetch|Load failed|NetworkError)/.test(err.message)) {
		return m.common_server_unreachable();
	}
	return err instanceof Error && err.message ? err.message : fallback;
}

type Schemas = components['schemas'];

export type FlowSummary = Schemas['FlowSummary'];
export type FlowDetail = Schemas['FlowDetail'];
export type FlowStatus = Schemas['FlowStatus'];
export type FlowSummarySource = Schemas['FlowSummary']['source'];
export type ResourceSummary = Schemas['ResourceSummary'];
export type ResourceContent = Schemas['ResourceContent'];
export type VersionInfo = Schemas['VersionInfo'];
export type ClusterStatus = Schemas['ClusterStatus'];
export type Connection = Schemas['Connection'];
export type Connector = Schemas['Connector'];
export type PodStatus = Schemas['PodStatus'];
export type ConfigInfo = Schemas['ConfigInfo'];
export type LogRecord = Schemas['LogRecord'];
export type LogSpan = Schemas['LogSpan'];
export type KeyValue = Schemas['KeyValue'];
export type UserContext = Schemas['UserContext'];
export type Change = Schemas['Change'];
export type ChangeSummary = Schemas['ChangeSummary'];
export type ChangeStatus = Schemas['ChangeStatus'];
export type AuthoringTarget = Schemas['AuthoringTarget'];
export type ValidationIssue = Schemas['ValidationIssue'];
