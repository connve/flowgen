<script lang="ts">
	import Badge from '$lib/Badge.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { cluster } from '$lib/clusterStatus.svelte';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';
</script>

<svelte:head>
	<title>{pageTitle(m.monitor_tab_pods(), m.nav_monitor())}</title>
</svelte:head>

<div class="flex min-h-0 flex-1 flex-col overflow-y-auto p-6">
	{#if !cluster.loaded}
		<div class="flex justify-center py-12">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if !cluster.status}
		<StateMessage
			tone="oops"
			title={m.pods_load_failed_title()}
			message={cluster.error ?? m.pods_load_failed_message()}
		/>
	{:else}
		<div class="shrink-0 overflow-x-auto rounded-lg border border-base-300 bg-base-100">
			<table class="table table-sm w-full bg-base-100">
				<thead class="bg-base-100 text-xs uppercase tracking-wide">
					<tr>
						<th>{m.pods_column_pod()}</th>
						<th>{m.pods_column_status()}</th>
						<th>
							<span
								class="tooltip tooltip-bottom before:normal-case before:tracking-normal"
								data-tip={m.pods_column_flows_tooltip()}
							>
								{m.pods_column_flows()}
							</span>
						</th>
					</tr>
				</thead>
				<tbody>
					{#each cluster.status.pods as pod (pod.identity)}
						<tr>
							<td class="whitespace-nowrap font-mono text-xs">{pod.identity}</td>
							<td class="text-sm">
								{#if pod.unreachable_reason}
									<span class="flex items-start gap-2">
										<Badge variant="warning">{m.pods_status_unreachable()}</Badge>
										<span class="opacity-70">{pod.unreachable_reason}</span>
									</span>
								{:else}
									<Badge variant="success">{m.pods_status_ok()}</Badge>
								{/if}
							</td>
							<td class="text-sm tabular-nums">
								{#if pod.flows === undefined}
									<span class="opacity-50">—</span>
								{:else}
									{pod.flows}
								{/if}
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
