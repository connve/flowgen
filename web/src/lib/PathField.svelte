<script lang="ts">
	// A workspace path as folders and a name, the way a code host names a new
	// file: typing `/` turns the name into a folder, Backspace on an empty
	// name steps back out of the last folder. `root` (`flows/`, `resources/`)
	// stays fixed, and so does a flow's extension.
	interface Props {
		/** The whole path, e.g. `flows/orders/sync.yaml`. */
		path: string;
		/** Names the field for assistive tech. */
		label: string;
		/** Shown in the empty name, e.g. `sync`. */
		placeholder: string;
		/** Called when the person changes the name or folders. */
		onedit?: () => void;
	}

	let { path = $bindable(), label, placeholder, onedit }: Props = $props();

	const FLOWS = 'flows/';
	const FLOW_EXTENSION = /\.(yaml|yml|json)$/;

	let root = $derived(path.startsWith('resources/') ? 'resources/' : FLOWS);
	let isFlow = $derived(root === FLOWS);
	let relative = $derived(path.slice(root.length));
	let extension = $derived(isFlow ? (relative.match(FLOW_EXTENSION)?.[0] ?? '.yaml') : '');
	let parts = $derived((isFlow ? relative.replace(FLOW_EXTENSION, '') : relative).split('/'));
	let folders = $derived(parts.slice(0, -1).filter((part) => part !== ''));
	let name = $derived(parts[parts.length - 1] ?? '');

	let input = $state<HTMLInputElement | null>(null);

	function write(nextFolders: string[], nextName: string) {
		const joined = [...nextFolders, nextName].join('/');
		path = `${root}${joined}${isFlow && nextName !== '' ? extension : ''}`;
		onedit?.();
	}

	// Typing `/` adds a folder; a pasted path from `root` replaces the folders;
	// a flow's extension typed or pasted is the fixed one.
	function oninput(event: Event & { currentTarget: HTMLInputElement }) {
		const typed = event.currentTarget.value;
		const whole = typed.startsWith(root);
		const pieces = (whole ? typed.slice(root.length) : typed).split('/');
		const added = pieces.slice(0, -1).filter((piece) => piece !== '');
		const last = pieces[pieces.length - 1];
		const next = isFlow ? last.replace(FLOW_EXTENSION, '') : last;
		write(whole ? added : [...folders, ...added], next);
		if (next !== last && input) input.value = next;
	}

	function onkeydown(event: KeyboardEvent & { currentTarget: HTMLInputElement }) {
		if (event.key !== 'Backspace' || event.currentTarget.value !== '' || folders.length === 0) {
			return;
		}
		event.preventDefault();
		write(folders.slice(0, -1), folders[folders.length - 1]);
	}
</script>

<div class="flex min-w-0 flex-wrap items-center gap-x-1 font-mono text-sm" role="group" aria-label={label}>
	<span class="text-base-content/40">{root.slice(0, -1)}</span>
	<span class="text-base-content/30">/</span>
	{#each folders as folder, i (i)}
		<span class="px-0.5 text-base-content/70">{folder}</span>
		<span class="text-base-content/30">/</span>
	{/each}
	<input
		bind:this={input}
		type="text"
		class="min-w-[1ch] border-b border-transparent bg-transparent px-0.5 outline-none placeholder:text-base-content/30 focus:border-primary"
		style="field-sizing: content; max-width: 40ch"
		aria-label={label}
		spellcheck="false"
		{placeholder}
		value={name}
		{oninput}
		{onkeydown}
	/>
	{#if isFlow}
		<span class="text-base-content/40">{extension}</span>
	{/if}
</div>
