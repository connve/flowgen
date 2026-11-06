<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import Icon from '@iconify/svelte';
	import { onMount } from 'svelte';
	import SidePane from '$lib/SidePane.svelte';
	import { cluster, pollClusterStatus, unreachableCount } from '$lib/clusterStatus.svelte';
	import { isMobile } from '$lib/mobile';
	import { m } from '$lib/paraglide/messages.js';

	let { children } = $props();
	let currentPath = $derived(page.url.pathname);
	let missing = $derived(cluster.status ? unreachableCount(cluster.status) : 0);

	let paneOpen = $state(true);

	const VIEWS = [
		{ href: '/monitor/logs', icon: 'tabler:list-details', label: m.monitor_tab_logs },
		{ href: '/monitor/pods', icon: 'tabler:server-2', label: m.monitor_tab_pods }
	];
	let current = $derived(VIEWS.find((view) => currentPath.startsWith(base + view.href)));

	onMount(() => {
		const saved = localStorage.getItem('flowgen-monitor-pane');
		if (saved !== null) paneOpen = saved === '1';
		if (isMobile()) paneOpen = false;
		return pollClusterStatus();
	});

	function closePaneOnMobile() {
		if (isMobile()) paneOpen = false;
	}

	function togglePane() {
		paneOpen = !paneOpen;
		localStorage.setItem('flowgen-monitor-pane', paneOpen ? '1' : '0');
	}
</script>

<section class="relative flex h-[calc(100vh-4rem)] min-w-0">
	<SidePane
		open={paneOpen}
		title={m.nav_monitor()}
		collapseLabel={m.monitor_collapse()}
		expandLabel={m.monitor_expand()}
		ontoggle={togglePane}
		onclose={closePaneOnMobile}
	>
		<nav class="min-h-0 flex-1 overflow-y-auto px-3 pb-3" aria-label={m.monitor_tabs_label()}>
			<ul class="space-y-px">
				{#each VIEWS as view (view.href)}
					{@const active = view === current}
					<li>
						<a
							href="{base}{view.href}"
							class="flex h-9 w-full min-w-0 items-center gap-2 rounded-md pr-2 text-sm transition-colors {active
								? 'bg-base-200 font-medium'
								: 'hover:bg-base-200/60'}"
							style="padding-left: 1.75rem"
							aria-current={active ? 'page' : undefined}
							onclick={closePaneOnMobile}
						>
							<Icon icon={view.icon} class="h-4 w-4 shrink-0 opacity-60" />
							<span class="min-w-0 flex-1 truncate">{view.label()}</span>
							{#if view.href === '/monitor/pods' && cluster.status && missing > 0}
								<span
									class="tooltip tooltip-right shrink-0 text-xs tabular-nums text-attention"
									data-tip={m.monitor_pods_reachable({
										reachable: cluster.status.pods.length - missing,
										total: cluster.status.pods.length
									})}
								>
									{cluster.status.pods.length - missing}/{cluster.status.pods.length}
								</span>
							{/if}
						</a>
					</li>
				{/each}
			</ul>
		</nav>
		{#snippet rail()}
			{#if cluster.status && missing > 0}
				<a
					href="{base}/monitor/pods"
					class="tooltip tooltip-right flex h-8 w-8 items-center justify-center rounded-md text-attention transition-colors hover:bg-base-200"
					data-tip={m.monitor_pods_reachable({
						reachable: cluster.status.pods.length - missing,
						total: cluster.status.pods.length
					})}
					aria-label={m.monitor_pods_reachable({
						reachable: cluster.status.pods.length - missing,
						total: cluster.status.pods.length
					})}
				>
					<Icon icon="tabler:alert-triangle" class="h-5 w-5" />
				</a>
			{/if}
		{/snippet}
	</SidePane>

	<div class="flex min-w-0 flex-1 flex-col">
		<div class="flex min-h-0 flex-1 flex-col">
			{@render children()}
		</div>
	</div>
</section>
