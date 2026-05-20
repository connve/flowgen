export async function onRequest(context) {
	const host = context.env.POSTHOG_ASSETS_HOST || 'eu-assets.i.posthog.com';
	const url = new URL(context.request.url);
	const path = url.pathname.replace('/ingest/static', '') || '/';
	const target = `https://${host}/static${path}${url.search}`;

	const response = await fetch(target, {
		method: context.request.method,
		headers: context.request.headers,
		body: context.request.method !== 'GET' ? context.request.body : undefined,
	});

	return new Response(response.body, {
		status: response.status,
		headers: response.headers,
	});
}
