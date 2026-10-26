// The docs as plain Markdown for coding agents, in the order of the
// navigation: an index (llms.txt), every page in one file (llms-full.txt) and
// each page on its own under /md/.
import { navigation, flattenNav } from '$lib/nav';

export const SITE = 'https://connve.com/docs/flowgen';

const sources = import.meta.glob('../routes/**/+page.md', {
	query: '?raw',
	import: 'default',
	eager: true
}) as Record<string, string>;

export interface Page {
	title: string;
	route: string;
	markdown: string;
}

function routeOf(file: string): string {
	return file.replace(/^\.\.\/routes/, '').replace(/\/\+page\.md$/, '');
}

// Svelte imports go, and Mermaid components become Mermaid code blocks.
function plain(source: string): string {
	return source
		.replace(/^<script>[\s\S]*?<\/script>\s*/, '')
		.replace(/<Mermaid chart=\{`([\s\S]*?)`\}\s*\/>/g, (_, chart: string) => '```mermaid\n' + chart.trim() + '\n```')
		.trim();
}

const byRoute = new Map(
	Object.entries(sources).map(([file, source]) => [routeOf(file), plain(source)])
);

/** Every page in the navigation, in its order. */
export function pages(): Page[] {
	return flattenNav(navigation).flatMap((item) => {
		const markdown = byRoute.get(item.href);
		return markdown === undefined ? [] : [{ title: item.title, route: item.href, markdown }];
	});
}

/** The page's Markdown, or undefined for a route that isn't a docs page. */
export function page(route: string): string | undefined {
	return byRoute.get(route);
}

/** The first sentence of the page's first paragraph. */
export function summary(markdown: string): string {
	const paragraph = markdown
		.split(/\n\s*\n/)
		.map((block) => block.trim())
		.find((block) => block && !/^(#|```|\||-|\*|>|<)/.test(block));
	if (!paragraph) return '';
	const text = paragraph.replace(/\s+/g, ' ');
	const end = text.search(/[.!?](?=\s+[A-Z`(]|$)/);
	return end === -1 ? text : text.slice(0, end + 1);
}

export function markdownUrl(route: string): string {
	return `${SITE}/md${route}.md`;
}

export { navigation };
