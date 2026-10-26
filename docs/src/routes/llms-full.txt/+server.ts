import type { RequestHandler } from './$types';
import { SITE, pages } from '$lib/llms';

export const prerender = true;

export const GET: RequestHandler = () => {
	const body = pages()
		.map((page) => `<!-- ${SITE}${page.route} -->\n\n${page.markdown}`)
		.join('\n\n---\n\n');
	return new Response(body + '\n', {
		headers: { 'Content-Type': 'text/plain; charset=utf-8' }
	});
};
