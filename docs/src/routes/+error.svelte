<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { StateMessage } from '@connve/ui';
</script>

<div class="flex min-h-[calc(100dvh-10rem)] flex-col">
	{#if page.status === 404}
		<StateMessage
			title="Page not found"
			message="This page doesn't exist or has moved. Pick a page from the navigation or start from the beginning."
		>
			<a href="{base}/getting-started/why-flowgen" class="btn btn-primary btn-sm">Go to the docs</a>
		</StateMessage>
	{:else}
		<StateMessage tone="oops" message={page.error?.message ?? `Error ${page.status}`}>
			<a href="{base}/getting-started/why-flowgen" class="btn btn-primary btn-sm">Go to the docs</a>
		</StateMessage>
	{/if}
</div>
