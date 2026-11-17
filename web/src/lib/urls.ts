import { apiUrl, type LogLevel } from '$lib/api';

// Server addresses the browser opens itself: event streams, sign-in links and
// images. Calls go through the generated functions in `$lib/api`.

export const flowsStream = (): string => apiUrl('api/flows/stream');

export const sessionsStream = (): string => apiUrl('api/agents/sessions/stream');

export const deploymentsStream = (): string => apiUrl('api/changes/stream');

export const replyStream = (session: string): string =>
	apiUrl(`api/agents/sessions/${encodeURIComponent(session)}/reply`);

export function logsStream(query: { flow?: string; levels?: readonly LogLevel[] }): string {
	const params = new URLSearchParams();
	if (query.flow) params.set('flow', query.flow);
	for (const level of query.levels ?? []) params.append('levels', level);
	const search = params.toString();
	return apiUrl(`api/logs/stream${search ? `?${search}` : ''}`);
}

export const authPrefix = (): string => `${apiUrl('auth')}/`;

export const loginUrl = (returnTo?: string): string =>
	`${apiUrl('auth/login')}${returnTo ? `?return_to=${encodeURIComponent(returnTo)}` : ''}`;

export const logoutUrl = (): string => apiUrl('auth/logout');

export const connectorIconUrl = (name: string): string =>
	apiUrl(`api/connectors/${encodeURIComponent(name)}/icon`);

/** How to connect a coding agent to flowgen, in the docs. */
export const AGENT_SETUP_URL = 'https://connve.com/docs/flowgen/getting-started/agent-setup';
