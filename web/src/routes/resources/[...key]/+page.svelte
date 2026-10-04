<script lang="ts">
	import { base } from '$app/paths';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import CopyButton from '$lib/CopyButton.svelte';
	import EditButton from '$lib/EditButton.svelte';
	import PendingChanges from '$lib/PendingChanges.svelte';
	import { editUrl, targetOf } from '$lib/changes';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let keySegments = $derived(data.key.split('/'));
	let folderSegments = $derived(keySegments.slice(0, -1));
	let leafName = $derived(keySegments[keySegments.length - 1] ?? '');
	let editHref = $derived(
		targetOf(data.targets, `resources/${data.key}`) ? editUrl(base, `resources/${data.key}`) : null
	);
</script>

<svelte:head>
	<title>{data.key} | Flowgen</title>
</svelte:head>

<section class="p-6">
	<div class="mb-4">
		<div class="flex min-h-8 items-center justify-between gap-2">
			<div class="flex items-center gap-1.5 text-sm">
				<a href="{base}/resources" class="text-primary hover:underline">Resources</a>
				{#each folderSegments as segment, i (i)}
					<span class="opacity-40">/</span>
					<span class="font-mono opacity-70">{segment}</span>
				{/each}
				<span class="opacity-40">/</span>
				<span class="font-mono">{leafName}</span>
			</div>
			<div class="flex items-center gap-1">
				{#if data.targets.length > 0}
					<EditButton href={editHref} />
				{/if}
			</div>
		</div>
	</div>

	<PendingChanges changes={data.pending} />

	<div
		class="flex h-[calc(100vh-10rem)] flex-col overflow-hidden rounded-lg border border-base-200 bg-base-100"
	>
		<div class="flex h-10 shrink-0 items-center justify-between border-b border-base-200 bg-base-100 px-4">
			<span class="text-xs font-medium uppercase opacity-70">
				{data.resource.extension ?? 'File'}
			</span>
			<CopyButton text={data.resource.content} label="Copy" />
		</div>
		<div class="min-h-0 flex-1 overflow-auto bg-base-200">
			<ResourceViewer content={data.resource.content} extension={data.resource.extension} />
		</div>
	</div>
</section>
