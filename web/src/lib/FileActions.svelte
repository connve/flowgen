<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import Icon from '@iconify/svelte';
	import {
		encodePath,
		errorMessage,
		listInstalledTemplates,
		proposeChange,
		type AuthoringTarget,
		type InstalledTemplate
	} from '$lib/api';
	import { proposeFile, targetOf } from '$lib/changes';
	import { m } from '$lib/paraglide/messages.js';
	import { onMount } from 'svelte';

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
	let installed = $state<InstalledTemplate[]>([]);
	let template = $derived(installed.find((template) => template.files.includes(file)));
	let confirming = $state<'delete' | 'uninstall' | null>(null);
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

	async function run(action: () => Promise<void>, fallback: string) {
		busy = true;
		failure = null;
		try {
			await action();
			confirming = null;
		} catch (err) {
			failure = errorMessage(err, fallback);
		} finally {
			busy = false;
		}
	}

	function remove() {
		return run(async () => {
			if (ondelete) {
				await ondelete();
			} else {
				const change = await proposeFile({ path: file, content: null }, targets);
				await goto(`${base}/changes/${encodeURIComponent(change.id)}`);
			}
		}, m.file_delete_failed({ name }));
	}

	// Deletes the files flowgen.lock holds for the template and drops it from flowgen.lock.
	function uninstall(template: InstalledTemplate) {
		return run(async () => {
			const { data: change } = await proposeChange({
				body: {
					title: m.hub_template_change_title_uninstall({ name: template.slug }),
					files: template.files.map((path) => ({ path, content: null })),
					template: { hub: template.hub, slug: template.slug, digest: template.digest, removed: true }
				}
			});
			await goto(`${base}/changes/${encodeURIComponent(change.id)}`);
		}, m.hub_template_uninstall_failed({ name: template.slug }));
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
		{#if template && canDelete}
			<li>
				<button
					type="button"
					class="text-error"
					{disabled}
					onclick={() => (confirming = 'uninstall')}
				>
					<Icon icon="tabler:package-off" class="h-4 w-4" />
					{m.hub_template_uninstall()}
				</button>
			</li>
		{/if}
		<li class={canDelete ? '' : 'menu-disabled'}>
			<button
				type="button"
				class="text-error"
				disabled={disabled || !canDelete}
				onclick={() => (confirming = 'delete')}
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
		aria-label={confirming === 'uninstall' ? m.hub_template_uninstall() : m.common_delete()}
		tabindex="-1"
		onclick={(e) => {
			if (e.target === e.currentTarget && !busy) confirming = null;
		}}
		onkeydown={(e) => {
			if (e.key !== 'Escape' || busy) return;
			e.stopPropagation();
			confirming = null;
		}}
	>
		<div class="w-full max-w-sm space-y-4 rounded-xl bg-base-100 p-5 shadow-2xl">
			{#if confirming === 'uninstall' && template}
				<h2 class="text-[0.9375rem] font-semibold">
					{m.hub_template_uninstall_title({ name: template.slug })}
				</h2>
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
					onclick={() => (confirming = null)}
				>
					{m.common_cancel()}
				</button>
				{#if confirming === 'uninstall' && template}
					{@const installedTemplate = template}
					<button
						type="button"
						class="btn btn-error btn-sm"
						disabled={busy}
						onclick={() => uninstall(installedTemplate)}
					>
						{m.hub_template_uninstall()}
					</button>
				{:else}
					<button type="button" class="btn btn-error btn-sm" disabled={busy} onclick={remove}>
						{m.common_delete()}
					</button>
				{/if}
			</div>
		</div>
	</div>
{/if}
