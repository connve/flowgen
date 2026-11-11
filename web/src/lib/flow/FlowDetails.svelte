<script lang="ts">
	import { base } from '$app/paths';
	import { CopyButton, IconButton, Inspector, Loading, StateMessage } from '@connve/ui';
	import ResourceViewer from '$lib/ResourceViewer.svelte';
	import ActivityDetails, {
		activityJson,
		activitySummary,
		type Activity
	} from '$lib/flow/ActivityDetails.svelte';
	import { encodePath, errorMessage, getResource, type ResourceContent } from '$lib/api';
	import { m } from '$lib/paraglide/messages.js';

	interface Props {
		/** The resource a flow's config points at, shown with its content. */
		resource?: string | null;
		/** The activity record shown when no resource is. */
		activity?: Activity | null;
	}

	let { resource = $bindable(null), activity = $bindable(null) }: Props = $props();

	let content = $state<ResourceContent | null>(null);
	let loading = $state(false);
	let error = $state<string | null>(null);

	$effect(() => {
		const key = resource;
		content = null;
		error = null;
		if (key === null) return;
		let current = true;
		loading = true;
		getResource({ path: { key } })
			.then(({ data }) => {
				if (current) content = data;
			})
			.catch((err) => {
				if (current) error = errorMessage(err, m.flow_resource_load_failed({ key }));
			})
			.finally(() => {
				if (current) loading = false;
			});
		return () => {
			current = false;
		};
	});

	function close() {
		resource = null;
		activity = null;
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key !== 'Escape' || event.defaultPrevented || !(resource || activity)) return;
		const target = event.target instanceof HTMLElement ? event.target : null;
		if (target?.isContentEditable || target?.closest('input, textarea, select, [role="dialog"]')) return;
		close();
	}
</script>

<svelte:window onkeydown={onKeydown} />

{#if resource || activity}
	<Inspector
		title={resource ?? activity?.task ?? m.activity_flow_task()}
		label={resource ? m.flow_resource_preview_label() : m.activity_detail_label()}
		onclose={close}
	>
		{#snippet subtitle()}
			{#if resource}
				<span class="truncate">{resource}</span>
			{:else if activity}
				{@render activitySummary(activity)}
			{/if}
		{/snippet}
		{#snippet actions()}
			{#if resource}
				<CopyButton text={content?.content} label={m.common_copy()} tooltip="bottom" />
				<IconButton
					icon="tabler:arrow-up-right"
					label={m.resource_open_full_page()}
					href="{base}/resources/{encodePath(resource)}"
				/>
			{:else if activity}
				<CopyButton text={activityJson(activity)} label={m.activity_copy_log()} tooltip="bottom" />
			{/if}
		{/snippet}
		{#if resource}
			{#if loading}
				<Loading size="md" />
			{:else if error}
				<StateMessage tone="oops" title={m.flow_resource_load_failed_title()} message={error} />
			{:else if content}
				<div class="-mx-6 overflow-auto bg-base-200 text-xs">
					<ResourceViewer content={content.content} extension={content.extension} />
				</div>
			{/if}
		{:else if activity}
			<ActivityDetails {activity} />
		{/if}
	</Inspector>
{/if}
