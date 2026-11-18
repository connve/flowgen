import { base } from '$app/paths';
import { error } from '@sveltejs/kit';
import { m } from '$lib/paraglide/messages.js';
import { client } from './generated/client.gen';
import type * as Types from './generated/types.gen';

export * from './generated/sdk.gen';

// A failed call, with the server's reason as its message.
export class ApiError extends Error {
	constructor(
		message: string,
		readonly status: number
	) {
		super(message);
	}
}

client.interceptors.error.use((error, response) => {
	if (error instanceof Error || response === undefined) return error;
	return new ApiError(
		serverReason(error) ?? m.common_failed_with_status({ status: response.status }),
		response.status
	);
});

// Whether `err` is a failed call that answered `status`.
export function isStatus(err: unknown, status: number): boolean {
	return err instanceof ApiError && err.status === status;
}

// Fails a page load with a failed call's status and reason; `notFound` words a 404.
export function loadError(err: unknown, fallback: string, notFound?: string): never {
	if (err instanceof ApiError) error(err.status, err.status === 404 && notFound ? notFound : err.message);
	error(503, errorMessage(err, fallback));
}

// The address of a server path under the UI's mount prefix, for streams and links.
export function apiUrl(path: string): string {
	const clean = path.startsWith('/') ? path : `/${path}`;
	return `${base}${clean}`;
}

// URL-encode a slash-delimited flow or resource path so it survives fetch and goto.
export function encodePath(path: string): string {
	return path.split('/').map(encodeURIComponent).join('/');
}

function serverReason(body: unknown): string | null {
	if (typeof body === 'string') {
		const text = body.trim();
		if (text === '' || text.startsWith('<')) return null;
		try {
			return serverReason(JSON.parse(text));
		} catch {
			return text;
		}
	}
	if (typeof body !== 'object' || body === null) return null;
	const { error, message } = body as { error?: unknown; message?: unknown };
	const nested = typeof error === 'object' && error !== null ? (error as { message?: unknown }).message : error;
	const reason = typeof nested === 'string' && nested !== '' ? nested : message;
	return typeof reason === 'string' && reason !== '' ? reason : null;
}

// What to show for a failed request; browsers word a lost connection as
// "Failed to fetch", "Load failed" or "NetworkError…".
export function errorMessage(err: unknown, fallback: string): string {
	if (err instanceof TypeError && /^(Failed to fetch|Load failed|NetworkError)/.test(err.message)) {
		return m.common_server_unreachable();
	}
	return err instanceof Error && err.message ? err.message : fallback;
}

export type FlowSummary = Types.FlowSummary;
export type FlowDetail = Types.FlowDetail;
export type FlowStatus = Types.FlowStatus;
export type FlowSummarySource = Types.FlowSummary['source'];
export type ResourceSummary = Types.ResourceSummary;
export type ResourceContent = Types.ResourceContent;
export type VersionInfo = Types.VersionInfo;
export type ClusterStatus = Types.ClusterStatus;
export type Connection = Types.Connection;
export type Connector = Types.Connector;
export type CredentialsForm = Types.CredentialsForm;
export type CredentialsField = Types.CredentialsField;
export type AgentTool = Types.AgentTool;
export type PodStatus = Types.PodStatus;
export type ConfigInfo = Types.ConfigInfo;
export type LogRecord = Types.LogRecord;
export type LogLevel = Types.LogLevel;
export type LogSpan = Types.LogSpan;
export type KeyValue = Types.KeyValue;
export type UserContext = Types.UserContext;
export type Change = Types.Change;
export type ChangeSummary = Types.ChangeSummary;
export type ChangeProposal = Types.ChangeProposal;
export type ChangeStatus = Types.ChangeStatus;
export type AuthoringTarget = Types.AuthoringTarget;
export type ValidationIssue = Types.ValidationIssue;
export type Hub = Types.Hub;
export type HubTemplate = Types.HubTemplate;
export type InstalledTemplate = Types.InstalledTemplate;
export type SessionMessage = Types.SessionMessage;
export type Session = Types.Session;
export type SessionSummary = Types.SessionSummary;
export type ToolStep = Types.ToolStep;
