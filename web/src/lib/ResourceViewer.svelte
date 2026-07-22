<script lang="ts">
	import { marked } from 'marked';
	import Prism from 'prismjs';
	import 'prismjs/components/prism-sql';
	import 'prismjs/components/prism-yaml';
	import 'prismjs/components/prism-json';
	import 'prismjs/components/prism-bash';
	import 'prismjs/components/prism-python';
	import 'prismjs/components/prism-typescript';
	import 'prismjs/components/prism-javascript';
	import 'prismjs/components/prism-markdown';

	interface Props {
		content: string;
		extension: string | null;
		// When set on a YAML view, `resource: "..."` values are turned into
		// clickable links that call this callback with the resource key. Lets
		// the flow inspector open a resource inline without leaving the modal.
		onResourceClick?: (key: string) => void;
		// When true, each `name: <value>` line gets an `id="task-<value>"`
		// anchor so external callers (the DAG) can scroll it into view.
		anchorTaskNames?: boolean;
	}

	let { content, extension, onResourceClick, anchorTaskNames }: Props = $props();

	// Map file extension → Prism language grammar. `.rhai` shares Rust-like
	// syntax; we fall back to a Rust-flavoured highlight since Prism has no
	// dedicated Rhai grammar and Rust matches the constructs users see (let,
	// if/else, ==, string literals).
	function extensionToPrismLang(ext: string | null): string | null {
		if (!ext) return null;
		const map: Record<string, string> = {
			sql: 'sql',
			yaml: 'yaml',
			yml: 'yaml',
			json: 'json',
			sh: 'bash',
			bash: 'bash',
			py: 'python',
			ts: 'typescript',
			js: 'javascript',
			mjs: 'javascript',
			md: 'markdown',
			rhai: 'javascript'
		};
		return map[ext.toLowerCase()] ?? null;
	}

	let isMarkdown = $derived(extension?.toLowerCase() === 'md');
	let renderedMarkdown = $derived(isMarkdown ? (marked.parse(content) as string) : '');

	let lang = $derived(extensionToPrismLang(extension));
	let rawHighlighted = $derived(
		!isMarkdown && lang && Prism.languages[lang]
			? Prism.highlight(content, Prism.languages[lang], lang)
			: null
	);

	// Post-process Prism output to wrap `resource: "path"` string values in a
	// clickable anchor. Only applies to YAML views that opted in via
	// `onResourceClick`. Regex targets the Prism markup Prism emits for the
	// key/value pair, not the raw text, so highlighting stays intact.
	function linkifyResources(html: string): string {
		return html.replace(
			/(<span class="token key atrule">resource<\/span><span class="token punctuation">:<\/span>\s*<span class="token string">)"([^"<>]+)"(<\/span>)/g,
			(_, open, key, close) =>
				`${open}"<a class="resource-link" data-resource="${escapeAttr(key)}" href="#">${escapeHtml(key)}</a>"${close}`
		);
	}

	function escapeAttr(s: string): string {
		return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;');
	}

	function escapeHtml(s: string): string {
		return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
	}

	// Finds the source lines that carry a task-level `name:` field — i.e.
	// `name:` sitting under a `tasks:` block. Ignores flow-level `name:` and
	// nested `name:` inside sub-configs, so DAG node clicks scroll to the
	// right task even when the flow name matches a task name.
	function findTaskNameLines(source: string): Map<number, string> {
		const lines = source.split('\n');
		const result = new Map<number, string>();
		let tasksIndent = -1;
		// Indent of `- <task_type>:` line for the current task item.
		let dashIndent = -1;
		// Indent of child keys under the current task item — YAML lets the
		// author put them anywhere deeper than the dash, so we lock it in
		// once we see the first child key rather than assuming `dash + 2`.
		let childIndent = -1;
		for (let i = 0; i < lines.length; i++) {
			const line = lines[i];
			if (line.trim() === '' || line.trim().startsWith('#')) continue;
			const indent = line.length - line.trimStart().length;
			const stripped = line.slice(indent);

			if (/^tasks\s*:/.test(stripped)) {
				tasksIndent = indent;
				dashIndent = -1;
				childIndent = -1;
				continue;
			}
			if (tasksIndent === -1) continue;
			// Left the tasks block: something at same or shallower indent than
			// `tasks:` itself.
			if (indent <= tasksIndent) {
				tasksIndent = -1;
				dashIndent = -1;
				childIndent = -1;
				continue;
			}
			// New task item.
			if (stripped.startsWith('- ')) {
				dashIndent = indent;
				childIndent = -1;
				continue;
			}
			if (dashIndent === -1) continue;
			// First deeper-than-dash key locks in the child indent level.
			if (childIndent === -1 && indent > dashIndent) {
				childIndent = indent;
			}
			if (indent === childIndent) {
				const m = stripped.match(/^name\s*:\s*(\S+)/);
				if (m) result.set(i, m[1]);
			}
		}
		return result;
	}

	// After Prism highlights, add id="task-<name>" anchors around the task
	// names on the pre-identified lines. Prism preserves newlines so the
	// mapping from source line → HTML line is 1:1.
	function anchorTaskLines(html: string, source: string): string {
		const taskLines = findTaskNameLines(source);
		if (taskLines.size === 0) return html;
		const htmlLines = html.split('\n');
		for (const [lineNo, name] of taskLines) {
			if (lineNo >= htmlLines.length) continue;
			// The name value in HTML looks like `name<..>: <token string?>name</token>`.
			// Replace the last occurrence of the raw name identifier on that
			// specific line only, so we don't accidentally rewrap prism tokens.
			const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
			htmlLines[lineNo] = htmlLines[lineNo].replace(
				new RegExp(`(name<\\/span><span class="token punctuation">:<\\/span>\\s*)(${escaped})`),
				(_, prefix, m) => `${prefix}<span id="task-${escapeAttr(name)}">${escapeHtml(m)}</span>`
			);
		}
		return htmlLines.join('\n');
	}

	let highlighted = $derived.by(() => {
		if (!rawHighlighted) return rawHighlighted;
		if (lang !== 'yaml') return rawHighlighted;
		let out = rawHighlighted;
		if (onResourceClick) out = linkifyResources(out);
		if (anchorTaskNames) out = anchorTaskLines(out, content);
		return out;
	});

	function onClick(e: MouseEvent) {
		if (!onResourceClick) return;
		const target = e.target as HTMLElement | null;
		const link = target?.closest?.('a.resource-link') as HTMLElement | null;
		if (!link) return;
		const key = link.dataset.resource;
		if (!key) return;
		e.preventDefault();
		onResourceClick(key);
	}
</script>

{#if isMarkdown}
	<article class="prose prose-sm max-w-none p-4">
		{@html renderedMarkdown}
	</article>
{:else if highlighted}
	<pre
		class="language-{lang} whitespace-pre-wrap break-words p-4 text-xs leading-relaxed"
		onclick={onClick}
		onkeydown={(e) => {
			if (e.key === 'Enter') onClick(e as unknown as MouseEvent);
		}}
		role={onResourceClick ? 'presentation' : undefined}><code
			class="language-{lang}">{@html highlighted}</code
		></pre>
{:else}
	<pre
		class="whitespace-pre-wrap break-words p-4 font-mono text-xs leading-relaxed"><code
			>{content}</code
		></pre>
{/if}

<style>
	/* Prism token colors + theme palette now live in app.css (shared with the
	   Agents chat). Only viewer-specific styles remain below. */
	:global(.resource-link) {
		color: inherit;
		text-decoration: underline;
		text-decoration-style: dotted;
		text-underline-offset: 3px;
		cursor: pointer;
	}
	:global(.resource-link:hover) {
		text-decoration-style: solid;
		color: #007cf3;
	}
	/* Persistent, visible scrollbars so users notice horizontal overflow —
	   YAML with long templates like `{{event.data.customer.email}}` is common. */
	pre {
		scrollbar-width: thin;
		scrollbar-color: rgba(100, 116, 130, 0.4) transparent;
	}
	pre::-webkit-scrollbar {
		height: 10px;
		width: 10px;
	}
	pre::-webkit-scrollbar-thumb {
		background: rgba(100, 116, 130, 0.4);
		border-radius: 6px;
	}
	pre::-webkit-scrollbar-thumb:hover {
		background: rgba(100, 116, 130, 0.65);
	}
	pre::-webkit-scrollbar-track {
		background: transparent;
	}
</style>
