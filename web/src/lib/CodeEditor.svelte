<script lang="ts">
	import { highlight, prismLanguage } from '$lib/highlight';

	interface Props {
		value: string;
		extension: string | null;
		label: string;
	}

	let { value = $bindable(), extension, label }: Props = $props();

	let lang = $derived(prismLanguage(extension));
	let langClass = $derived(lang ? `language-${lang}` : '');
	let highlighted = $derived(highlight(value, extension));
</script>

<!-- A transparent textarea over the highlighted text: both share one grid
     cell and the same font metrics, so the caret lines up with the colors. -->
<div class="min-h-0 flex-1 overflow-auto bg-base-200">
	<div class="grid min-h-full">
		<pre
			aria-hidden="true"
			class="{langClass} pointer-events-none m-0 whitespace-pre-wrap break-words p-4 font-mono text-xs leading-relaxed [grid-area:1/1]"><code
				class={langClass}
				>{#if highlighted}{@html highlighted}{:else}{value}{/if}{'\n'}</code
			></pre>
		<textarea
			class="resize-none overflow-hidden whitespace-pre-wrap break-words bg-transparent p-4 font-mono text-xs leading-relaxed text-transparent caret-base-content outline-none [grid-area:1/1]"
			aria-label={label}
			spellcheck="false"
			bind:value
		></textarea>
	</div>
</div>
