<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import { encodePath, errorMessage, type AuthoringTarget } from '$lib/api';
	import { proposeFile, targetOf } from '$lib/changes';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		/** The file, by its path in the workspace. */
		file: string;
		targets: AuthoringTarget[];
		/** Proposes the deletion; by default as a change of its own or in the user's pending one. */
		ondelete?: () => Promise<void>;
		disabled?: boolean;
	}

	let { file, targets, ondelete, disabled = false }: Props = $props();

	let canDelete = $derived(targetOf(targets, file) !== undefined);
	let name = $derived(file.split('/').pop() ?? file);
	let confirming = $state(false);
	let deleting = $state(false);
	let failure = $state<string | null>(null);
	let cancelButton = $state<HTMLButtonElement | null>(null);

	$effect(() => {
		if (confirming) cancelButton?.focus();
	});

	async function remove() {
		deleting = true;
		failure = null;
		try {
			if (ondelete) {
				await ondelete();
			} else {
				const change = await proposeFile({ path: file, content: null }, targets);
				await goto(`${base}/changes/${encodeURIComponent(change.id)}`);
			}
			confirming = false;
		} catch (err) {
			failure = errorMessage(err, m.file_delete_failed({ name }));
		} finally {
			deleting = false;
		}
	}
</script>

<div class="dropdown dropdown-end">
	<button
		type="button"
		tabindex="0"
		class="flex h-8 w-8 items-center justify-center rounded-md text-base-content/60 transition-colors hover:bg-base-200 hover:text-base-content"
		aria-label={m.editor_more_actions()}
	>
		<Icon icon="tabler:dots" class="h-5 w-5" />
	</button>
	<ul
		tabindex="-1"
		class="menu dropdown-content z-10 mt-1 w-56 rounded-md border border-base-200 bg-base-100 p-1 text-sm shadow-lg"
	>
		<li>
			<a href="{base}/edit?new={file.startsWith('flows/') ? 'flow' : 'resource'}&from={encodePath(file)}">
				<Icon icon="tabler:copy" class="h-4 w-4" />
				{m.editor_duplicate()}
			</a>
		</li>
		<li class={canDelete ? '' : 'menu-disabled'}>
			<button
				type="button"
				class="text-error"
				disabled={disabled || !canDelete}
				onclick={() => (confirming = true)}
			>
				<Icon icon="tabler:trash" class="h-4 w-4" />
				{m.common_delete()}
			</button>
			{#if !canDelete}
				<span class="text-xs text-base-content/50">{m.editor_delete_read_only()}</span>
			{/if}
		</li>
	</ul>
</div>

{#if confirming}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-4"
		role="dialog"
		aria-modal="true"
		aria-label={m.common_delete()}
		tabindex="-1"
		onclick={(e) => {
			if (e.target === e.currentTarget && !deleting) confirming = false;
		}}
		onkeydown={(e) => {
			if (e.key !== 'Escape' || deleting) return;
			e.stopPropagation();
			confirming = false;
		}}
	>
		<div class="w-full max-w-sm space-y-4 rounded-xl bg-base-100 p-5 shadow-2xl">
			<h2 class="text-[0.9375rem] font-semibold">{m.file_delete_title({ name })}</h2>
			{#if failure}
				<p class="text-sm text-error">{failure}</p>
			{/if}
			<div class="flex justify-end gap-2">
				<button
					type="button"
					class="btn btn-ghost btn-sm"
					bind:this={cancelButton}
					disabled={deleting}
					onclick={() => (confirming = false)}
				>
					{m.common_cancel()}
				</button>
				<button type="button" class="btn btn-error btn-sm" disabled={deleting} onclick={remove}>
					{m.common_delete()}
				</button>
			</div>
		</div>
	</div>
{/if}
