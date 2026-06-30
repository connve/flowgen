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
			{ title: 'Webhook', href: '/http/webhook' },
			{ title: 'Request', href: '/http/request' }
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
		title: 'MongoDB',
		icon: '/icons/mongodb.svg',
		items: [
			{ title: 'Reader', href: '/mongo/reader' },
			{ title: 'Writer', href: '/mongo/writer' },
			{ title: 'Change Stream', href: '/mongo/change_stream' }
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
		title: 'AI',
		icon: '/icons/ai.svg',
		items: [
			{ title: 'Overview', href: '/ai' },
			{ title: 'AI Completion', href: '/ai/completion' },
			{ title: 'AI Gateway', href: '/ai/gateway' },
			{ title: 'MCP Tools', href: '/ai/mcp' }
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
];
