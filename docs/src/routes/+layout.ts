import type { LayoutLoad } from './$types';
import { navigation, prevNext } from '$lib/nav';

export const prerender = true;

const SITE_ORIGIN = 'https://connve.com';
const BASE_PATH = '/docs/flowgen';
const DEFAULT_TITLE = 'Flowgen documentation | CONNVE';
const DEFAULT_DESCRIPTION =
	'Flowgen is a real-time data activation platform. Author flows in YAML, run them on Kubernetes, and connect to your existing infrastructure.';

const SECTION_LABELS: Record<string, string> = {
	'getting-started': 'Flowgen getting started',
	concepts: 'Flowgen concepts',
	core: 'Flowgen core tasks',
	ai: 'Flowgen AI',
	braze: 'Flowgen Braze',
	git: 'Flowgen Git',
	gcp: 'Flowgen Google Cloud',
	http: 'Flowgen HTTP',
	mssql: 'Flowgen MSSQL',
	nats: 'Flowgen NATS JetStream',
	'object-store': 'Flowgen Object Store',
	oci: 'Flowgen OCI',
	salesforce: 'Flowgen Salesforce'
};

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

	const segments = lookup.split('/').filter(Boolean);
	const sectionLabel = segments.length >= 2 ? SECTION_LABELS[segments[0]] : undefined;

	let displayTitle = meta?.title ?? '';
	if (sectionLabel && displayTitle) {
		const lastWord = sectionLabel.split(' ').pop() ?? '';
		if (lastWord && displayTitle.toLowerCase().startsWith(lastWord.toLowerCase() + ' ')) {
			displayTitle = displayTitle.slice(lastWord.length + 1);
		}
	}

	const pageTitle = displayTitle
		? sectionLabel
			? `${sectionLabel} - ${displayTitle} | CONNVE`
			: `${displayTitle} | CONNVE`
		: DEFAULT_TITLE;
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
