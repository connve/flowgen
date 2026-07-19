import type { RequestHandler } from './$types';

export const prerender = true;

const SITE_ORIGIN = 'https://connve.com';
const BASE_PATH = '/docs/flowgen';

const pages = import.meta.glob('../**/+page.md', { eager: true });

export const GET: RequestHandler = () => {
	const urls = new Set<string>();
	urls.add(`${SITE_ORIGIN}${BASE_PATH}/`);

	for (const file of Object.keys(pages)) {
		const route = file
			.replace(/^\.\.\//, '/')
			.replace(/\/\+page\.md$/, '');
		const normalized = route === '' ? '/' : route;
		urls.add(`${SITE_ORIGIN}${BASE_PATH}${normalized}`);
	}

	const today = new Date().toISOString().slice(0, 10);
	const body =
		`<?xml version="1.0" encoding="UTF-8"?>\n` +
		`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n` +
		[...urls]
			.sort()
			.map(
				(loc) =>
					`  <url>\n    <loc>${loc}</loc>\n    <lastmod>${today}</lastmod>\n    <changefreq>weekly</changefreq>\n  </url>`
			)
			.join('\n') +
		`\n</urlset>\n`;

	return new Response(body, {
		headers: { 'Content-Type': 'application/xml' }
	});
};
