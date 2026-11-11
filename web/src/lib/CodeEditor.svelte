<script lang="ts">
	import { onMount } from 'svelte';
	import { EditorState, type Extension } from '@codemirror/state';
	import { EditorView, drawSelection, highlightActiveLine, keymap } from '@codemirror/view';
	import { defaultKeymap, history, historyKeymap, indentWithTab } from '@codemirror/commands';
	import {
		HighlightStyle,
		bracketMatching,
		indentOnInput,
		indentUnit,
		syntaxHighlighting
	} from '@codemirror/language';
	import { highlightSelectionMatches, searchKeymap } from '@codemirror/search';
	import { tags } from '@lezer/highlight';
	import { yaml } from '@codemirror/lang-yaml';
	import { json } from '@codemirror/lang-json';
	import { sql } from '@codemirror/lang-sql';
	import { markdown } from '@codemirror/lang-markdown';
	import { javascript } from '@codemirror/lang-javascript';

	interface Props {
		value: string;
		extension: string | null;
		label: string;
	}

	let { value = $bindable(), extension, label }: Props = $props();

	let host: HTMLDivElement;
	let view: EditorView | undefined;

	// Language per file extension. Rhai has no grammar; JavaScript covers what
	// Rhai scripts use (let, if/else, operators, string literals).
	function language(extension: string | null): Extension {
		switch (extension?.toLowerCase()) {
			case 'yaml':
			case 'yml':
				return yaml();
			case 'json':
				return json();
			case 'sql':
				return sql();
			case 'md':
				return markdown();
			case 'ts':
				return javascript({ typescript: true });
			case 'js':
			case 'mjs':
			case 'rhai':
				return javascript();
			default:
				return [];
		}
	}

	// The colors of the read-only views, from the --prism-* variables in app.css.
	const colors = HighlightStyle.define([
		{ tag: tags.comment, color: 'var(--prism-comment)', fontStyle: 'italic' },
		{
			tag: [tags.propertyName, tags.definition(tags.propertyName), tags.attributeName],
			color: 'var(--prism-property)',
			fontWeight: '600'
		},
		{ tag: [tags.string, tags.special(tags.string)], color: 'var(--prism-string)' },
		{ tag: [tags.number, tags.bool, tags.null], color: 'var(--prism-number)' },
		{
			tag: [tags.keyword, tags.operatorKeyword, tags.modifier, tags.heading],
			color: 'var(--prism-keyword)',
			fontWeight: '600'
		},
		{ tag: [tags.function(tags.variableName), tags.typeName, tags.className], color: 'var(--prism-function)' },
		{ tag: [tags.operator, tags.url, tags.link], color: 'var(--prism-operator)' },
		{ tag: [tags.punctuation, tags.separator, tags.bracket], color: 'var(--prism-punctuation)' },
		{ tag: [tags.regexp, tags.special(tags.variableName)], color: 'var(--prism-variable)' }
	]);

	const theme = EditorView.theme({
		'&': { height: '100%', backgroundColor: 'transparent', fontSize: '0.75rem' },
		'&.cm-focused': { outline: 'none' },
		'.cm-scroller': { fontFamily: 'var(--font-mono)', lineHeight: '1.625' },
		'.cm-content': { padding: '1rem 0', caretColor: 'var(--color-base-content)' },
		'.cm-line': { padding: '0 1rem' },
		'.cm-cursor': { borderLeftColor: 'var(--color-base-content)' },
		'.cm-activeLine': { backgroundColor: 'color-mix(in oklab, var(--color-base-content) 4%, transparent)' },
		'&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection': {
			backgroundColor: 'color-mix(in oklab, var(--color-primary) 25%, transparent)'
		},
		'.cm-matchingBracket': { backgroundColor: 'color-mix(in oklab, var(--color-primary) 20%, transparent)' },
		'.cm-panels': { backgroundColor: 'var(--color-base-200)', color: 'var(--color-base-content)' }
	});

	onMount(() => {
		view = new EditorView({
			parent: host,
			state: EditorState.create({
				doc: value,
				extensions: [
					history(),
					drawSelection(),
					highlightActiveLine(),
					highlightSelectionMatches(),
					indentOnInput(),
					bracketMatching(),
					indentUnit.of('  '),
					EditorState.tabSize.of(2),
					EditorView.lineWrapping,
					keymap.of([indentWithTab, ...defaultKeymap, ...historyKeymap, ...searchKeymap]),
					syntaxHighlighting(colors),
					theme,
					language(extension),
					EditorView.contentAttributes.of({ 'aria-label': label, spellcheck: 'false' }),
					EditorView.updateListener.of((update) => {
						if (update.docChanged) value = update.state.doc.toString();
					})
				]
			})
		});
		return () => view?.destroy();
	});

	// A value set from outside, such as a formatted save or a reload, replaces the text.
	$effect(() => {
		const next = value;
		if (view && next !== view.state.doc.toString()) {
			view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: next } });
		}
	});
</script>

<div class="min-h-0 flex-1 overflow-auto bg-base-200" bind:this={host}></div>
