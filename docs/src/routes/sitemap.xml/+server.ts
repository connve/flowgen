import type { RequestHandler } from './$types';

export const prerender = true;

const SITE_ORIGIN = 'https://connve.com';
const BASE_PATH = '/docs/flowgen';

const pages = import.meta.glob('../**/+page.md', { eager: true });

export const GET: RequestHandler = () => {
	const urls = new Set<string>();

	for (const file of Object.keys(pages)) {
		const route = file
			.replace(/^\.\.\//, '/')
			.replace(/\/\+page\.md$/, '');
		urls.add(`${SITE_ORIGIN}${BASE_PATH}${route}`);
	}

	const body =
		`<?xml version="1.0" encoding="UTF-8"?>\n` +
		`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n` +
		[...urls]
			.sort()
			.map((loc) => `  <url>\n    <loc>${loc}</loc>\n  </url>`)
			.join('\n') +
		`\n</urlset>\n`;

	return new Response(body, {
		headers: { 'Content-Type': 'application/xml' }
	});
};
