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

const FRONTMATTER = /^---\n([\s\S]*?)\n---\n/;

/** Longest description search results show in full. */
const DESCRIPTION_LIMIT = 160;

// The frontmatter and Svelte imports go, and Mermaid components become Mermaid code blocks.
function plain(source: string): string {
	return source
		.replace(FRONTMATTER, '')
		.replace(/^\s*<script>[\s\S]*?<\/script>\s*/, '')
		.replace(/<Mermaid chart=\{`([\s\S]*?)`\}\s*\/>/g, (_, chart: string) => '```mermaid\n' + chart.trim() + '\n```')
		.trim();
}

const byRoute = new Map(
	Object.entries(sources).map(([file, source]) => [routeOf(file), plain(source)])
);

/** Descriptions pages set in their frontmatter, as `description: …`. */
const ownDescriptions = new Map(
	Object.entries(sources).flatMap(([file, source]) => {
		const own = source.match(FRONTMATTER)?.[1].match(/^description:\s*(.+)$/m)?.[1].trim();
		return own ? [[routeOf(file), own.replace(/^(['"])(.*)\1$/, '$2')]] : [];
	})
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

/** The page's first paragraph of prose, on one line: one that ends a sentence,
 * so a lone "or" between two code blocks doesn't count. A paragraph that leads
 * into a list or table keeps its sentences before the colon, or ends with a
 * full stop instead. */
function firstParagraph(markdown: string): string {
	const paragraph = markdown
		.split(/\n\s*\n/)
		.map((block) => block.trim())
		.find((block) => /[.!?:]$/.test(block) && !/^(#|```|\||-|\*|>|<)/.test(block));
	if (!paragraph) return '';
	const text = paragraph.replace(/\s+/g, ' ');
	if (!text.endsWith(':')) return text;
	const end = text.search(/[.!?](?=\s[^.!?]*$)/);
	return end > 0 ? text.slice(0, end + 1) : `${text.slice(0, -1)}.`;
}

/** The page's own `description`, else the first sentence of its first paragraph. */
export function summary(route: string): string {
	const own = ownDescriptions.get(route);
	if (own) return own;
	const text = firstParagraph(byRoute.get(route) ?? '');
	const end = text.search(/[.!?](?=\s+[A-Z`(]|$)/);
	return end === -1 ? text : text.slice(0, end + 1);
}

/** What search results show for a page: its own `description`, else its first
 * paragraph without Markdown, cut after the last sentence that fits. */
export function description(route: string): string | undefined {
	const own = ownDescriptions.get(route);
	if (own) return own;
	const markdown = byRoute.get(route);
	if (markdown === undefined) return undefined;
	const text = firstParagraph(markdown)
		.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
		.replace(/`([^`]+)`/g, '$1')
		.replace(/\*\*([^*]+)\*\*/g, '$1');
	if (text.length <= DESCRIPTION_LIMIT) return text;
	const cut = text.slice(0, DESCRIPTION_LIMIT);
	const end = cut.search(/[.!?](?=\s[^.!?]*$)/);
	return end > 0 ? cut.slice(0, end + 1) : `${cut.slice(0, cut.lastIndexOf(' '))}…`;
}

/** The page's first `# ` heading. */
export function heading(markdown: string): string | undefined {
	return markdown.match(/^# (.+)$/m)?.[1].trim();
}

export function markdownUrl(route: string): string {
	return `${SITE}/md${route}.md`;
}

export { navigation };
