<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import Badge from '$lib/Badge.svelte';
	import { cluster, pollClusterStatus, unreachableCount } from '$lib/clusterStatus.svelte';

	let { children } = $props();
	let currentPath = $derived(page.url.pathname);
	let missing = $derived(cluster.status ? unreachableCount(cluster.status) : 0);

	onMount(() => pollClusterStatus());
</script>

<section class="flex h-[calc(100vh-4rem)] min-w-0 flex-col overflow-hidden">
	<nav class="tabs tabs-border shrink-0 border-b border-base-200 px-3" aria-label="Monitor">
		{#each [{ href: '/monitor/logs', label: 'Logs' }, { href: '/monitor/pods', label: 'Pods' }] as tab (tab.href)}
			{@const active = currentPath.startsWith(base + tab.href)}
			<a
				href="{base}{tab.href}"
				class="tab gap-2 {active ? 'tab-active' : ''}"
				aria-current={active ? 'page' : undefined}
			>
				{tab.label}
				{#if tab.href === '/monitor/pods' && cluster.status && missing > 0}
					<Badge variant="warning">
						{cluster.status.pods.length - missing}/{cluster.status.pods.length}
					</Badge>
				{/if}
			</a>
		{/each}
	</nav>
	<div class="flex min-h-0 flex-1 flex-col">
		{@render children()}
	</div>
</section>
