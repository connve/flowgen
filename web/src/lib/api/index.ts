import { base } from '$app/paths';
import type { components } from './generated';

// The mount prefix is baked into the SvelteKit build via `paths.base` in
// svelte.config.js (PUBLIC_BASE at build time). `base` therefore already
// matches `web.path` in flowgen config, so an API call is just `base +
// path`.
export function apiUrl(path: string): string {
	const clean = path.startsWith('/') ? path : `/${path}`;
	return `${base}${clean}`;
}

type Schemas = components['schemas'];

export type FlowSummary = Schemas['FlowSummary'];
export type FlowDetail = Schemas['FlowDetail'];
export type FlowStatus = Schemas['FlowStatus'];
export type FlowSummarySource = Schemas['FlowSummary']['source'];
export type ResourceSummary = Schemas['ResourceSummary'];
export type ResourceContent = Schemas['ResourceContent'];
export type VersionInfo = Schemas['VersionInfo'];
export type ConfigInfo = Schemas['ConfigInfo'];
export type LogRecord = Schemas['LogRecord'];
export type LogSpan = Schemas['LogSpan'];
export type KeyValue = Schemas['KeyValue'];
