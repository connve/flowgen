// Markdown from models, flows and workspace files, rendered so it can't run
// code or load anything on its own: raw HTML shows as text, images become
// links, and links keep only web, mail and relative addresses.

import { Marked } from 'marked';
import Prism from 'prismjs';
import { prismLanguage } from '$lib/highlight';

function escapeHtml(s: string): string {
	return s
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;')
		.replace(/"/g, '&quot;');
}

// Parsed the way the browser parses it, so tabs or newlines inside a scheme
// (`java\nscript:`) can't slip through.
function safeHref(href: string): string | null {
	try {
		const url = new URL(href, 'https://relative.invalid/');
		return ['http:', 'https:', 'mailto:'].includes(url.protocol) ? href : null;
	} catch {
		return null;
	}
}

function grammarOf(lang: string | undefined): string | null {
	const name = lang?.match(/^[\w+-]+/)?.[0];
	if (!name) return null;
	if (Object.hasOwn(Prism.languages, name)) return name;
	return prismLanguage(name);
}

const markdown = new Marked({
	async: false,
	renderer: {
		code({ text, lang }) {
			const grammar = grammarOf(lang);
			const body = grammar ? Prism.highlight(text, Prism.languages[grammar], grammar) : escapeHtml(text);
			return `<pre class="language-${grammar ?? 'none'}"><code>${body}</code></pre>`;
		},
		html({ text }) {
			return escapeHtml(text);
		},
		image({ href, text }) {
			const label = escapeHtml(text || href);
			const safe = safeHref(href);
			if (!safe) return label;
			return `<a href="${escapeHtml(safe)}" target="_blank" rel="noopener noreferrer">${label}</a>`;
		},
		link({ href, title, tokens }) {
			const text = this.parser.parseInline(tokens);
			const safe = safeHref(href);
			if (!safe) return text;
			const titleAttr = title ? ` title="${escapeHtml(title)}"` : '';
			return `<a href="${escapeHtml(safe)}"${titleAttr} target="_blank" rel="noopener noreferrer">${text}</a>`;
		}
	}
});

export function renderMarkdown(text: string): string {
	return markdown.parse(text) as string;
}
