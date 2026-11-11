import { listHubTemplates, listHubs as fetchHubs, type Hub, type HubTemplate } from '$lib/api';

export interface Catalog {
	hub: Hub;
	templates: HubTemplate[];
}

// The configured hubs; none when listing fails.
export async function listHubs(fetcher: typeof fetch = fetch): Promise<Hub[]> {
	return fetchHubs({ fetch: fetcher }).then(
		({ data }) => data.hubs ?? [],
		() => []
	);
}

async function catalogOf(hub: Hub, fetcher: typeof fetch): Promise<Catalog | null> {
	try {
		const { data } = await listHubTemplates({ path: { hub: hub.id }, fetch: fetcher });
		const templates = data.templates ?? [];
		return templates.length > 0 ? { hub, templates } : null;
	} catch {
		return null;
	}
}

// The hubs that answer with templates; one that fails or has none is left out.
export async function catalogsOf(hubs: Hub[], fetcher: typeof fetch = fetch): Promise<Catalog[]> {
	const answered = await Promise.all(hubs.map((hub) => catalogOf(hub, fetcher)));
	return answered.filter((catalog) => catalog !== null);
}
