import Prism from 'prismjs';
import 'prismjs/components/prism-sql';
import 'prismjs/components/prism-yaml';
import 'prismjs/components/prism-json';
import 'prismjs/components/prism-bash';
import 'prismjs/components/prism-python';
import 'prismjs/components/prism-typescript';
import 'prismjs/components/prism-javascript';
import 'prismjs/components/prism-markdown';

// Prism grammar per file extension. Prism has no Rhai grammar; JavaScript
// covers what Rhai scripts use (let, if/else, operators, string literals).
const LANGUAGES: Record<string, string> = {
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

export function prismLanguage(extension: string | null): string | null {
	if (!extension) return null;
	const lang = LANGUAGES[extension.toLowerCase()];
	return lang && Prism.languages[lang] ? lang : null;
}

// Highlighted HTML for `content`, or `null` when the extension has no grammar.
export function highlight(content: string, extension: string | null): string | null {
	const lang = prismLanguage(extension);
	return lang ? Prism.highlight(content, Prism.languages[lang], lang) : null;
}
