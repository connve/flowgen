// Formats workspace files with Prettier in the browser, loaded on first use.

type Formatter = {
	parser: string;
	plugins: () => Promise<unknown[]>;
};

const FORMATTERS: Record<string, Formatter> = {
	yaml: { parser: 'yaml', plugins: async () => [await import('prettier/plugins/yaml')] },
	yml: { parser: 'yaml', plugins: async () => [await import('prettier/plugins/yaml')] },
	json: {
		parser: 'json',
		plugins: async () => [
			await import('prettier/plugins/babel'),
			await import('prettier/plugins/estree')
		]
	},
	md: { parser: 'markdown', plugins: async () => [await import('prettier/plugins/markdown')] }
};

// The line width the workspace files of the repo are formatted at, so saving
// one changes only what was edited.
const PRINT_WIDTH = 120;

// `source` formatted for the file type of `path`: two-space indents, one blank
// line at most, comments kept. As it is when the type has no formatter or the
// source does not parse, so validation reports the problem.
export async function formatSource(source: string, path: string): Promise<string> {
	const formatter = FORMATTERS[path.split('.').pop()?.toLowerCase() ?? ''];
	if (!formatter) return source;
	try {
		const [{ format }, plugins] = await Promise.all([
			import('prettier/standalone'),
			formatter.plugins()
		]);
		return await format(source, {
			parser: formatter.parser,
			plugins: plugins as never[],
			tabWidth: 2,
			useTabs: false,
			printWidth: PRINT_WIDTH
		});
	} catch {
		return source;
	}
}
