import { listConnectors, type CredentialsForm } from '$lib/api';
import { connectorIconUrl } from '$lib/urls';

export interface Connector {
	name: string;
	label: string;
	iconUrl: string;
	credentials: CredentialsForm[];
}

function connector(name: string, label: string, credentials: CredentialsForm[] = []): Connector {
	return { name, label, iconUrl: connectorIconUrl(name), credentials };
}

const CORE = connector('core', 'Core');

let connectors: Record<string, Connector> = {};
let taskConnectors: Record<string, string> = {};
let taskTypesByConnector: { connector: Connector; taskTypes: string[] }[] = [];
let loading: Promise<void> | null = null;

export function loadConnectors(fetcher: typeof fetch): Promise<void> {
	loading ??= listConnectors({ fetch: fetcher })
		.then(({ data: found }) => {
			connectors = Object.fromEntries(
				found.map((c) => [c.name, connector(c.name, c.label, c.credentials)])
			);
			taskConnectors = Object.fromEntries(
				found.flatMap((c) => c.taskTypes.map((taskType) => [taskType, c.name]))
			);
			taskTypesByConnector = found.map((c) => ({
				connector: connectors[c.name],
				taskTypes: c.taskTypes
			}));
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

/** Every connector with its task types, once `loadConnectors` is done. */
export function connectorTaskTypes(): { connector: Connector; taskTypes: string[] }[] {
	return taskTypesByConnector;
}

export function connectorByName(name: string): Connector {
	return connectors[name] ?? connector(name, name);
}
