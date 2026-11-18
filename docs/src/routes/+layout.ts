import type { LayoutLoad } from './$types';
import { navigation, prevNext } from '$lib/nav';
import { description, heading, page } from '$lib/llms';

export const prerender = true;

const SITE_ORIGIN = 'https://connve.com';
const BASE_PATH = '/docs/flowgen';
const SITE_TITLE = 'Flowgen Docs';
const DEFAULT_DESCRIPTION =
	'Flowgen is an open-source composable data activation platform. Turn your data into the next best action!';

// Tab titles like the web UI's: most specific first, then the subsection and
// section, then the site, e.g. `REST API · Guides · Salesforce · Flowgen Docs`.
function titleOf(route: string, heading: string | undefined): string {
	for (const section of navigation) {
		const groups = [
			{ title: undefined, items: section.items },
			...(section.subsections ?? [])
		];
		for (const group of groups) {
			const item = group.items.find((candidate) => candidate.href === route);
			if (!item) continue;
			const page = item.title === 'Overview' ? [] : [item.title];
			const subsection = group.title ? [group.title] : [];
			return [...page, ...subsection, section.title, SITE_TITLE].join(' · ');
		}
	}
	return heading ? `${heading} · ${SITE_TITLE}` : SITE_TITLE;
}

export const load: LayoutLoad = ({ url }) => {
	const relativePath = url.pathname.startsWith(BASE_PATH)
		? url.pathname.slice(BASE_PATH.length) || '/'
		: url.pathname;
	const lookup = relativePath.replace(/\/$/, '') || '/';
	const markdown = page(lookup);

	const pageTitle = titleOf(lookup, markdown === undefined ? undefined : heading(markdown));
	const pageDescription = description(lookup) || DEFAULT_DESCRIPTION;
	const canonicalPath = lookup === '/' ? '/getting-started/why-flowgen' : lookup;
	const canonical = `${SITE_ORIGIN}${BASE_PATH}${canonicalPath}`;

	const { prev, next } = prevNext(navigation, lookup);

	return {
		seo: {
			title: pageTitle,
			description: pageDescription,
			canonical
		},
		nav: { prev, next }
	};
};
