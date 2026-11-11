import GithubSlugger from 'github-slugger';
import { navigation, type NavSection } from '$lib/nav';
import { pages } from '$lib/llms';

/** One page, or one h2/h3 section of it, as the search palette finds it. */
export interface SearchEntry {
	/** The page title from the navigation. */
	page: string;
	/** The navigation section, e.g. `Salesforce`. */
	section: string;
	/** The section heading; absent for the text before the first heading. */
	heading?: string;
	/** The route with the heading's id, without the base path. */
	href: string;
	text: string;
}

function sectionOf(route: string): string {
	const found = navigation.find((section: NavSection) =>
		[...section.items, ...(section.subsections ?? []).flatMap((sub) => sub.items)].some(
			(item) => item.href === route
		)
	);
	return found?.title ?? '';
}

function inline(markdown: string): string {
	return markdown
		.replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1')
		.replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
		.replace(/<[^>]+>/g, ' ')
		.replace(/[`*]/g, '')
		.replace(/\\(.)/g, '$1');
}

function plain(lines: string[]): string {
	return inline(
		lines
			.filter((line) => !/^\s*(```|\|?\s*:?-{3,})/.test(line))
			.map((line) => line.replace(/^\s*(>|[-*+]|\d+\.)\s+/, '').replace(/\|/g, ' '))
			.join(' ')
	)
		.replace(/\s+/g, ' ')
		.trim();
}

/** Splits each page at its h2 and h3 headings; the ids follow rehype-slug, which slugs every heading in order. */
export function searchIndex(): SearchEntry[] {
	return pages().flatMap(({ title, route, markdown }) => {
		const slugger = new GithubSlugger();
		const section = sectionOf(route);
		const entries: SearchEntry[] = [];
		let current: { heading?: string; id?: string; lines: string[] } = { lines: [] };
		let fenced = false;

		const flush = () => {
			const text = plain(current.lines);
			if (text || current.heading) {
				entries.push({
					page: title,
					section,
					heading: current.heading,
					href: current.id ? `${route}#${current.id}` : route,
					text
				});
			}
		};

		for (const line of markdown.split('\n')) {
			if (/^\s*```/.test(line)) fenced = !fenced;
			const heading = fenced ? null : /^(#{1,6})\s+(.+?)\s*#*$/.exec(line);
			if (!heading) {
				current.lines.push(line);
				continue;
			}
			const text = inline(heading[2]).replace(/\s+/g, ' ').trim();
			const id = slugger.slug(text);
			if (heading[1].length === 2 || heading[1].length === 3) {
				flush();
				current = { heading: text, id, lines: [] };
			}
		}
		flush();
		return entries;
	});
}
