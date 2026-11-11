import { listConnectors } from '$lib/api';
import { connectorIconUrl } from '$lib/urls';

export interface Connector {
	name: string;
	label: string;
	iconUrl: string;
}

function connector(name: string, label: string): Connector {
	return { name, label, iconUrl: connectorIconUrl(name) };
}

const CORE = connector('core', 'Core');

let connectors: Record<string, Connector> = {};
let taskConnectors: Record<string, string> = {};
let loading: Promise<void> | null = null;

export function loadConnectors(fetcher: typeof fetch): Promise<void> {
	loading ??= listConnectors({ fetch: fetcher })
		.then(({ data: found }) => {
			connectors = Object.fromEntries(found.map((c) => [c.name, connector(c.name, c.label)]));
			taskConnectors = Object.fromEntries(
				found.flatMap((c) => c.taskTypes.map((taskType) => [taskType, c.name]))
			);
		})
		.catch(() => {
			loading = null;
		});
	return loading;
}

export function connectorFor(taskType: string): Connector {
	const name = taskConnectors[taskType];
	return name ? connectorByName(name) : CORE;
}

export function connectorByName(name: string): Connector {
	return connectors[name] ?? connector(name, name);
}
