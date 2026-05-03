export interface NavItem {
	title: string;
	href: string;
}

export interface NavSection {
	title: string;
	icon?: string;
	items: NavItem[];
}

export const navigation: NavSection[] = [
	{
		title: 'Getting Started',
		items: [
			{ title: 'Why Flowgen', href: '/docs/getting-started/why-flowgen' },
			{ title: 'Installation', href: '/docs/getting-started/installation' },
			{ title: 'Quick Start', href: '/docs/getting-started/quickstart' }
		]
	},
	{
		title: 'Concepts',
		items: [
			{ title: 'Flows', href: '/docs/concepts/flows' },
			{ title: 'Tasks', href: '/docs/concepts/tasks' },
			{ title: 'Events', href: '/docs/concepts/events' },
			{ title: 'Templating', href: '/docs/concepts/templating' },
			{ title: 'Resources', href: '/docs/concepts/resources' },
			{ title: 'Caching', href: '/docs/concepts/caching' },
			{ title: 'Retry', href: '/docs/concepts/retry' },
			{ title: 'Credentials', href: '/docs/concepts/credentials' },
			{ title: 'Authentication', href: '/docs/concepts/auth' },
			{ title: 'Sandboxing', href: '/docs/concepts/sandboxing' },
			{ title: 'Telemetry', href: '/docs/concepts/telemetry' },
			{ title: 'Configuration', href: '/docs/concepts/configuration' }
		]
	},
	{
		title: 'NATS JetStream',
		icon: '/icons/nats.svg',
		items: [
			{ title: 'Subscriber', href: '/docs/nats/subscriber' },
			{ title: 'Publisher', href: '/docs/nats/publisher' },
			{ title: 'KV Store', href: '/docs/nats/kv-store' }
		]
	},
	{
		title: 'Salesforce',
		icon: '/icons/salesforce.svg',
		items: [
			{ title: 'PubSub API', href: '/docs/salesforce/pubsub' },
			{ title: 'REST API', href: '/docs/salesforce/rest' },
			{ title: 'Bulk API', href: '/docs/salesforce/bulk' },
			{ title: 'Tooling API', href: '/docs/salesforce/tooling' }
		]
	},
	{
		title: 'Google Cloud',
		icon: '/icons/gcp.svg',
		items: [
			{ title: 'BigQuery Query', href: '/docs/gcp/bigquery-query' },
			{ title: 'BigQuery Storage', href: '/docs/gcp/bigquery-storage' },
			{ title: 'BigQuery Jobs', href: '/docs/gcp/bigquery-jobs' }
		]
	},
	{
		title: 'HTTP',
		icon: '/icons/http.svg',
		items: [
			{ title: 'Webhook', href: '/docs/http/webhook' },
			{ title: 'Request', href: '/docs/http/request' }
		]
	},
	{
		title: 'Object Store',
		icon: '/icons/object-store.svg',
		items: [{ title: 'Object Store', href: '/docs/object-store/object-store' }]
	},
	{
		title: 'MSSQL',
		icon: '/icons/mssql.svg',
		items: [{ title: 'Query', href: '/docs/mssql/query' }]
	},
	{
		title: 'AI',
		icon: '/icons/ai.svg',
		items: [
			{ title: 'AI Completion', href: '/docs/ai/completion' },
			{ title: 'AI Gateway', href: '/docs/ai/gateway' },
			{ title: 'MCP Tools', href: '/docs/ai/mcp' }
		]
	},
	{
		title: 'Git',
		icon: '/icons/git.svg',
		items: [{ title: 'Git Sync', href: '/docs/git/sync' }]
	},
	{
		title: 'Core Tasks',
		icon: '/icons/core.svg',
		items: [
			{ title: 'Script (Rhai)', href: '/docs/core/script' },
			{ title: 'Convert', href: '/docs/core/convert' },
			{ title: 'Iterate', href: '/docs/core/iterate' },
			{ title: 'Buffer', href: '/docs/core/buffer' },
			{ title: 'Generate', href: '/docs/core/generate' },
			{ title: 'Log', href: '/docs/core/log' }
		]
	}
];
