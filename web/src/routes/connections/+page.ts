import { errorMessage, listChanges, listConnections } from '$lib/api';
import { credentialsEnabled } from '$lib/changes';
import { m } from '$lib/paraglide/messages.js';
import type { PageLoad } from './$types';

/** The committed change that last wrote a credentials file. */
export interface CredentialsChange {
	id: string;
	by: string;
	at: number;
}

/** The latest committed change of each credentials name, as `credentials/<name>.yaml`. */
async function credentialsChanges(fetcher: typeof fetch): Promise<Record<string, CredentialsChange>> {
	const changes = await listChanges({ fetch: fetcher }).then(
		({ data }) => data.changes,
		() => []
	);
	const decided = changes
		.filter((change) => change.status === 'committed')
		.map((change) => ({ change, at: change.decidedAt ?? change.createdAt }))
		.sort((a, b) => a.at - b.at);
	const latest: Record<string, CredentialsChange> = {};
	for (const { change, at } of decided) {
		for (const path of change.paths) {
			const name = /^credentials\/([a-z0-9-]+)\.yaml$/.exec(path)?.[1];
			if (name) latest[name] = { id: change.id, by: change.proposedBy, at };
		}
	}
	return latest;
}

// Returns the error instead of throwing so the page keeps refreshing.
export const load: PageLoad = async ({ fetch, depends }) => {
	depends('app:connections');
	const credentials = await credentialsEnabled(fetch);
	const changes = credentials ? await credentialsChanges(fetch) : {};
	try {
		return { connections: (await listConnections({ fetch })).data, error: null, credentials, changes };
	} catch (err) {
		return {
			connections: null,
			error: errorMessage(err, m.connections_load_failed()),
			credentials,
			changes
		};
	}
};
