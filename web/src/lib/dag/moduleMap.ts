// Maps a flowgen task type (the YAML key: `oci_sync`, `nats_kv_store`, …) to
// the connector module and icon that represents it in the DAG view.
//
// Keep this table in sync with the `TaskType` enum in
// `flowgen/app/src/config.rs`. A missing entry falls back to the `core` icon.

export interface Connector {
	module: string;
	iconPath: string;
	label: string;
}

const ICON_BASE = 'icons';

function icon(name: string): string {
	return `${ICON_BASE}/${name}`;
}

const CONNECTORS: Record<string, Connector> = {
	ai: { module: 'ai', iconPath: icon('ai.svg'), label: 'AI' },
	braze: { module: 'braze', iconPath: icon('braze.png'), label: 'Braze' },
	core: { module: 'core', iconPath: icon('core.svg'), label: 'Core' },
	gcp: { module: 'gcp', iconPath: icon('gcp.svg'), label: 'Google Cloud' },
	git: { module: 'git', iconPath: icon('git.svg'), label: 'Git' },
	http: { module: 'http', iconPath: icon('http.svg'), label: 'HTTP' },
	mongodb: { module: 'mongodb', iconPath: icon('mongodb.svg'), label: 'MongoDB' },
	mssql: { module: 'mssql', iconPath: icon('mssql.svg'), label: 'MSSQL' },
	nats: { module: 'nats', iconPath: icon('nats.svg'), label: 'NATS' },
	'object-store': {
		module: 'object-store',
		iconPath: icon('object-store.svg'),
		label: 'Object Store'
	},
	oci: { module: 'oci', iconPath: icon('oci.svg'), label: 'OCI' },
	salesforce: { module: 'salesforce', iconPath: icon('salesforce.svg'), label: 'Salesforce' }
};

const TASK_TO_MODULE: Record<string, string> = {
	// core
	convert: 'core',
	iterate: 'core',
	log: 'core',
	script: 'core',
	buffer: 'core',
	generate: 'core',
	// ai
	ai_completion: 'ai',
	mcp_tool: 'ai',
	mcp_resource: 'ai',
	mcp_prompt: 'ai',
	llm_proxy: 'ai',
	// braze
	braze_export_users_ids: 'braze',
	// gcp
	gcp_bigquery_query: 'gcp',
	gcp_bigquery_storage_read: 'gcp',
	gcp_bigquery_job: 'gcp',
	gcp_bigquery_storage_write: 'gcp',
	// git
	git_sync: 'git',
	// http
	http_request: 'http',
	http_endpoint: 'http',
	html_scrape: 'http',
	// mongodb
	mongodb_collection: 'mongodb',
	mongodb_change_stream: 'mongodb',
	// mssql
	mssql_query: 'mssql',
	// nats
	nats_jetstream_subscriber: 'nats',
	nats_jetstream_publisher: 'nats',
	nats_kv_store: 'nats',
	// object-store
	object_store: 'object-store',
	// oci
	oci_sync: 'oci',
	// salesforce
	salesforce_pubsubapi_subscriber: 'salesforce',
	salesforce_pubsubapi_publisher: 'salesforce',
	salesforce_bulkapi_query_job: 'salesforce',
	salesforce_restapi_sobject: 'salesforce',
	salesforce_restapi_composite: 'salesforce',
	salesforce_restapi_search: 'salesforce',
	salesforce_soapapi_merge: 'salesforce',
	salesforce_toolingapi: 'salesforce'
};

export function connectorFor(taskType: string): Connector {
	const module = TASK_TO_MODULE[taskType] ?? 'core';
	return CONNECTORS[module] ?? CONNECTORS.core;
}

/// Looks up a connector by its module name directly (`salesforce`, `gcp`,
/// `nats`, …). Returns `null` when no connector matches — the caller decides
/// on the fallback (e.g. a generic folder icon).
export function connectorByModule(name: string): Connector | null {
	return CONNECTORS[name] ?? null;
}
