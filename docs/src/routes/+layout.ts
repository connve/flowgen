import type { LayoutLoad } from './$types';
import { navigation, prevNext } from '$lib/nav';

export const prerender = true;

const SITE_ORIGIN = 'https://connve.com';
const BASE_PATH = '/docs/flowgen';
const SITE_TITLE = 'Flowgen Docs';
const DEFAULT_DESCRIPTION =
	'Flowgen is a real-time data activation platform. Author flows in YAML, run them on Kubernetes, and connect to your existing infrastructure.';

// Tab titles like the web UI's: most specific first, then the section, then
// the site, e.g. `KV Store · NATS JetStream · Flowgen Docs`.
function titleOf(route: string, heading: string | undefined): string {
	for (const section of navigation) {
		const items = [...section.items, ...(section.subsections ?? []).flatMap((sub) => sub.items)];
		const item = items.find((candidate) => candidate.href === route);
		if (item) {
			const page = item.title === 'Overview' ? [] : [item.title];
			return [...page, section.title, SITE_TITLE].join(' · ');
		}
	}
	return heading ? `${heading} · ${SITE_TITLE}` : SITE_TITLE;
}

const rawPages = import.meta.glob('./**/+page.md', { query: '?raw', import: 'default', eager: true }) as Record<string, string>;

interface PageMeta {
	title: string;
	description: string;
}

function extractMeta(raw: string): PageMeta {
	const lines = raw.split('\n');
	let title = '';
	let description = '';

	for (const line of lines) {
		const trimmed = line.trim();
		if (!title && trimmed.startsWith('# ')) {
			title = trimmed.slice(2).trim();
			continue;
		}
		if (title && !description && trimmed && !trimmed.startsWith('#') && !trimmed.startsWith('```')) {
			description = trimmed
				.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
				.replace(/`([^`]+)`/g, '$1')
				.replace(/\*\*([^*]+)\*\*/g, '$1')
				.replace(/\*([^*]+)\*/g, '$1');
			break;
		}
	}

	return { title, description };
}

const pageMetaByPath = new Map<string, PageMeta>();
for (const [file, raw] of Object.entries(rawPages)) {
	const route = file.replace(/^\.\//, '/').replace(/\/\+page\.md$/, '');
	const normalized = route === '' ? '/' : route;
	pageMetaByPath.set(normalized, extractMeta(raw));
}

export const load: LayoutLoad = ({ url }) => {
	const relativePath = url.pathname.startsWith(BASE_PATH)
		? url.pathname.slice(BASE_PATH.length) || '/'
		: url.pathname;
	const lookup = relativePath.replace(/\/$/, '') || '/';
	const meta = pageMetaByPath.get(lookup);

	const pageTitle = titleOf(lookup, meta?.title);
	const pageDescription = meta?.description || DEFAULT_DESCRIPTION;
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
