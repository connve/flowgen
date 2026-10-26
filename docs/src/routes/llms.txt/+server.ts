import type { RequestHandler } from './$types';
import { SITE, markdownUrl, navigation, page, summary } from '$lib/llms';

export const prerender = true;

export const GET: RequestHandler = () => {
	const sections = navigation.map((section) => {
		const items = [...section.items, ...(section.subsections ?? []).flatMap((sub) => sub.items)];
		const lines = items.flatMap((item) => {
			const markdown = page(item.href);
			if (markdown === undefined) return [];
			const about = summary(markdown);
			return [`- [${item.title}](${markdownUrl(item.href)})${about ? `: ${about}` : ''}`];
		});
		return `## ${section.title}\n\n${lines.join('\n')}`;
	});
	const body =
		`# Flowgen\n\n` +
		`> Flowgen runs flows defined in YAML: tasks that read, transform and send data between systems, call AI models and serve MCP tools, on one node or many.\n\n` +
		`Each link is the page as plain Markdown. [llms-full.txt](${SITE}/llms-full.txt) has every page in one file.\n\n` +
		sections.join('\n\n') +
		'\n';
	return new Response(body, { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
};
