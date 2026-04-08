export interface NavItem {
	title: string;
	href: string;
}

export interface NavSection {
	title: string;
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
			{ title: 'Caching', href: '/docs/concepts/caching' }
		]
	},
	{
		title: 'NATS JetStream',
		items: [
			{ title: 'Subscriber', href: '/docs/nats/subscriber' },
			{ title: 'Publisher', href: '/docs/nats/publisher' }
		]
	},
	{
		title: 'Salesforce',
		items: [
			{ title: 'PubSub API', href: '/docs/salesforce/pubsub' },
			{ title: 'REST API', href: '/docs/salesforce/rest' },
			{ title: 'Bulk API', href: '/docs/salesforce/bulk' },
			{ title: 'Tooling API', href: '/docs/salesforce/tooling' }
		]
	},
	{
		title: 'Google Cloud',
		items: [
			{ title: 'BigQuery Query', href: '/docs/gcp/bigquery-query' },
			{ title: 'BigQuery Storage', href: '/docs/gcp/bigquery-storage' },
			{ title: 'BigQuery Jobs', href: '/docs/gcp/bigquery-jobs' }
		]
	},
	{
		title: 'HTTP',
		items: [
			{ title: 'Webhook', href: '/docs/http/webhook' },
			{ title: 'Request', href: '/docs/http/request' }
		]
	},
	{
		title: 'Object Store',
		items: [
			{ title: 'Read', href: '/docs/object-store/read' },
			{ title: 'Write', href: '/docs/object-store/write' },
			{ title: 'List', href: '/docs/object-store/list' },
			{ title: 'Move', href: '/docs/object-store/move' }
		]
	},
	{
		title: 'MSSQL',
		items: [{ title: 'Query', href: '/docs/mssql/query' }]
	},
	{
		title: 'Core Tasks',
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
