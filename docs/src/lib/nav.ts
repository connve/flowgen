export interface NavItem {
	title: string;
	href: string;
}

export interface NavSubsection {
	title: string;
	items: NavItem[];
}

export interface NavSection {
	title: string;
	icon?: string;
	items: NavItem[];
	subsections?: NavSubsection[];
}

export function flattenNav(sections: NavSection[]): NavItem[] {
	const flat: NavItem[] = [];
	for (const section of sections) {
		for (const item of section.items) flat.push(item);
		if (section.subsections) {
			for (const sub of section.subsections) {
				for (const item of sub.items) flat.push(item);
			}
		}
	}
	return flat;
}

export function prevNext(sections: NavSection[], relativePath: string): { prev?: NavItem; next?: NavItem } {
	const flat = flattenNav(sections);
	const idx = flat.findIndex((item) => item.href === relativePath);
	if (idx === -1) return {};
	return {
		prev: idx > 0 ? flat[idx - 1] : undefined,
		next: idx < flat.length - 1 ? flat[idx + 1] : undefined
	};
}

export const navigation: NavSection[] = [
	{
		title: 'Getting Started',
		items: [
			{ title: 'Why Flowgen', href: '/getting-started/why-flowgen' },
			{ title: 'Installation', href: '/getting-started/installation' },
			{ title: 'Quick Start', href: '/getting-started/quickstart' },
			{ title: 'WireGuard Gateway', href: '/getting-started/wireguard-gateway' }
		]
	},
	{
		title: 'Concepts',
		items: [
			{ title: 'Flows', href: '/concepts/flows' },
			{ title: 'Tasks', href: '/concepts/tasks' },
			{ title: 'Events', href: '/concepts/events' },
			{ title: 'Templating', href: '/concepts/templating' },
			{ title: 'Resources', href: '/concepts/resources' },
			{ title: 'Caching', href: '/concepts/caching' },
			{ title: 'Retry', href: '/concepts/retry' },
			{ title: 'Credentials', href: '/concepts/credentials' },
			{ title: 'Authentication', href: '/concepts/auth' },
			{ title: 'Sandboxing', href: '/concepts/sandboxing' },
			{ title: 'Telemetry', href: '/concepts/telemetry' },
			{ title: 'Configuration', href: '/concepts/configuration' }
		]
	},
	{
		title: 'Core Tasks',
		icon: '/icons/core.svg',
		items: [
			{ title: 'Overview', href: '/core' },
			{ title: 'Script (Rhai)', href: '/core/script' },
			{ title: 'Convert', href: '/core/convert' },
			{ title: 'Iterate', href: '/core/iterate' },
			{ title: 'Buffer', href: '/core/buffer' },
			{ title: 'Generate', href: '/core/generate' },
			{ title: 'Log', href: '/core/log' }
		]
	},
	{
		title: 'AI',
		icon: '/icons/ai.svg',
		items: [
			{ title: 'Overview', href: '/ai' },
			{ title: 'AI Completion', href: '/ai/completion' },
			{ title: 'AI Gateway', href: '/ai/gateway' },
			{ title: 'MCP', href: '/ai/mcp' }
		]
	},
	{
		title: 'Braze',
		icon: '/icons/braze.png',
		items: [
			{ title: 'Overview', href: '/braze' },
			{ title: 'Export Users by IDs', href: '/braze/export/users' }
		]
	},
	{
		title: 'Git',
		icon: '/icons/git.svg',
		items: [
			{ title: 'Overview', href: '/git' },
			{ title: 'Git Sync', href: '/git/sync' }
		]
	},
	{
		title: 'Google Cloud',
		icon: '/icons/gcp.svg',
		items: [
			{ title: 'Overview', href: '/gcp' },
			{ title: 'BigQuery Query', href: '/gcp/bigquery-query' },
			{ title: 'BigQuery Storage', href: '/gcp/bigquery-storage' },
			{ title: 'BigQuery Jobs', href: '/gcp/bigquery-jobs' }
		]
	},
	{
		title: 'HTTP',
		icon: '/icons/http.svg',
		items: [
			{ title: 'Overview', href: '/http' },
			{ title: 'Endpoint', href: '/http/endpoint' },
			{ title: 'Request', href: '/http/request' }
		]
	},
	{
		title: 'MSSQL',
		icon: '/icons/mssql.svg',
		items: [
			{ title: 'Overview', href: '/mssql' },
			{ title: 'Query', href: '/mssql/query' }
		]
	},
	{
		title: 'NATS JetStream',
		icon: '/icons/nats.svg',
		items: [
			{ title: 'Overview', href: '/nats' },
			{ title: 'Subscriber', href: '/nats/subscriber' },
			{ title: 'Publisher', href: '/nats/publisher' },
			{ title: 'KV Store', href: '/nats/kv-store' }
		]
	},
	{
		title: 'Object Store',
		icon: '/icons/object-store.svg',
		items: [
			{ title: 'Overview', href: '/object-store' },
			{ title: 'Object Store', href: '/object-store/object-store' }
		]
	},
	{
		title: 'OCI',
		icon: '/icons/oci.svg',
		items: [
			{ title: 'Overview', href: '/oci' },
			{ title: 'OCI Sync', href: '/oci/sync' }
		]
	},
	{
		title: 'Salesforce',
		icon: '/icons/salesforce.svg',
		items: [
			{ title: 'Overview', href: '/salesforce' },
			{ title: 'PubSub API', href: '/salesforce/pubsub' },
			{ title: 'REST API', href: '/salesforce/rest' },
			{ title: 'Bulk API', href: '/salesforce/bulk' },
			{ title: 'Tooling API', href: '/salesforce/tooling' },
			{ title: 'Merge (SOAP)', href: '/salesforce/merge' }
		],
		subsections: [
			{
				title: 'Guides',
				items: [
					{ title: 'CDC Replication', href: '/salesforce/guides/cdc-replication' },
					{ title: 'Data Export', href: '/salesforce/guides/data-export' },
					{ title: 'Data Activation', href: '/salesforce/guides/data-activation' },
					{ title: 'REST API', href: '/salesforce/guides/rest-api' },
					{ title: 'Deduplication', href: '/salesforce/guides/deduplication' }
				]
			}
		]
	},
];
