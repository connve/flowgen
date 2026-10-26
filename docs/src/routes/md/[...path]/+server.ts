import { error } from '@sveltejs/kit';
import type { EntryGenerator, RequestHandler } from './$types';
import { page, pages } from '$lib/llms';

export const prerender = true;

export const entries: EntryGenerator = () =>
	pages().map((page) => ({ path: `${page.route.slice(1)}.md` }));

export const GET: RequestHandler = ({ params }) => {
	const markdown = page(`/${params.path.replace(/\.md$/, '')}`);
	if (markdown === undefined) error(404, 'Not found');
	return new Response(markdown + '\n', {
		headers: { 'Content-Type': 'text/markdown; charset=utf-8' }
	});
};
