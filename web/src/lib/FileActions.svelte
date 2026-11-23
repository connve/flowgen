<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import {
		encodePath,
		errorMessage,
		listInstalledTemplates,
		type AuthoringTarget,
		type InstalledTemplate
	} from '$lib/api';
	import { proposeFile, proposeFiles, targetOf } from '$lib/changes';
	import { m } from '$lib/paraglide/messages.js';
	import { onMount } from 'svelte';

	interface Props {
		/** The file, by its path in the workspace. */
		file: string;
		targets: AuthoringTarget[];
		/** Proposes the deletion; by default in the user's pending change, or a new one. */
		ondelete?: () => Promise<void>;
		disabled?: boolean;
	}

	let { file, targets, ondelete, disabled = false }: Props = $props();

	let canDelete = $derived(targetOf(targets, file) !== undefined);
	let name = $derived(file.split('/').pop() ?? file);
	let installed = $state<InstalledTemplate[]>([]);
	let template = $derived(installed.find((template) => template.files.includes(file)));
	let confirming = $state(false);
	let busy = $state(false);
	let failure = $state<string | null>(null);
	let cancelButton = $state<HTMLButtonElement | null>(null);

	$effect(() => {
		if (confirming) cancelButton?.focus();
	});

	onMount(async () => {
		try {
			const { data } = await listInstalledTemplates();
			installed = data.templates;
		} catch {
			installed = [];
		}
	});

	async function remove() {
		busy = true;
		failure = null;
		try {
			if (template) {
				const change = await proposeFiles(
					template.files.map((path) => ({ path, content: null })),
					targets,
					{
						title: m.hub_template_change_title_delete({ name: template.slug }),
						template: { ...template, removed: true }
					}
				);
				await goto(`${base}/changes/${encodeURIComponent(change.id)}`);
			} else if (ondelete) {
				await ondelete();
			} else {
				const change = await proposeFile({ path: file, content: null }, targets);
				await goto(`${base}/changes/${encodeURIComponent(change.id)}`);
			}
			confirming = false;
		} catch (err) {
			failure = errorMessage(err, m.file_delete_failed({ name: template?.slug ?? name }));
		} finally {
			busy = false;
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
			if (e.target === e.currentTarget && !busy) confirming = false;
		}}
		onkeydown={(e) => {
			if (e.key !== 'Escape' || busy) return;
			e.stopPropagation();
			confirming = false;
		}}
	>
		<div class="w-full max-w-sm space-y-4 rounded-xl bg-base-100 p-5 shadow-2xl">
			{#if template}
				<h2 class="text-[0.9375rem] font-semibold">
					{m.file_delete_title({ name: template.slug })}
				</h2>
				<p class="text-sm text-base-content/70">
					{m.file_delete_template({ file: name, template: template.slug })}
				</p>
				<ul class="space-y-1 font-mono text-xs text-base-content/70">
					{#each template.files as path (path)}
						<li class="[overflow-wrap:anywhere]">{path}</li>
					{/each}
				</ul>
			{:else}
				<h2 class="text-[0.9375rem] font-semibold">{m.file_delete_title({ name })}</h2>
			{/if}
			{#if failure}
				<p class="text-sm text-error">{failure}</p>
			{/if}
			<div class="flex justify-end gap-2">
				<button
					type="button"
					class="btn btn-ghost btn-sm"
					bind:this={cancelButton}
					disabled={busy}
					onclick={() => (confirming = false)}
				>
					{m.common_cancel()}
				</button>
				<button type="button" class="btn btn-error btn-sm" disabled={busy} onclick={remove}>
					{m.common_delete()}
				</button>
			</div>
		</div>
	</div>
{/if}
