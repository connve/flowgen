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
	}

	let { content, extension }: Props = $props();

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
	let highlighted = $derived(
		!isMarkdown && lang && Prism.languages[lang]
			? Prism.highlight(content, Prism.languages[lang], lang)
			: null
	);
</script>

{#if isMarkdown}
	<article class="prose prose-sm max-w-none p-4">
		{@html renderedMarkdown}
	</article>
{:else if highlighted}
	<pre class="language-{lang} overflow-auto p-4 text-xs leading-relaxed"><code
			class="language-{lang}">{@html highlighted}</code
		></pre>
{:else}
	<pre
		class="whitespace-pre-wrap break-words p-4 font-mono text-xs leading-relaxed"><code
			>{content}</code
		></pre>
{/if}

<style>
	/* Prism token colors — minimal, matches the primary green accent for keywords. */
	:global(.token.comment),
	:global(.token.prolog),
	:global(.token.doctype),
	:global(.token.cdata) {
		color: #6a8a7a;
		font-style: italic;
	}
	:global(.token.punctuation) {
		color: #556b60;
	}
	:global(.token.property),
	:global(.token.tag),
	:global(.token.constant),
	:global(.token.symbol),
	:global(.token.deleted) {
		color: #007600;
	}
	:global(.token.boolean),
	:global(.token.number) {
		color: #cf3450;
	}
	:global(.token.selector),
	:global(.token.attr-name),
	:global(.token.string),
	:global(.token.char),
	:global(.token.builtin),
	:global(.token.inserted) {
		color: #006b55;
	}
	:global(.token.operator),
	:global(.token.entity),
	:global(.token.url),
	:global(.language-css .token.string),
	:global(.style .token.string) {
		color: #007cf3;
	}
	:global(.token.atrule),
	:global(.token.attr-value),
	:global(.token.keyword) {
		color: #006b55;
		font-weight: 600;
	}
	:global(.token.function),
	:global(.token.class-name) {
		color: #007600;
	}
	:global(.token.regex),
	:global(.token.important),
	:global(.token.variable) {
		color: #ff9804;
	}
</style>
