<script lang="ts">
	import Badge from '$lib/Badge.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { cluster } from '$lib/clusterStatus.svelte';
</script>

<svelte:head>
	<title>Pods | Flowgen</title>
</svelte:head>

<div class="min-h-0 flex-1 overflow-y-auto p-6">
	{#if !cluster.loaded}
		<div class="flex justify-center py-12">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if !cluster.status}
		<StateMessage tone="oops" title="Failed to load pods" message="GET /api/cluster did not answer." />
	{:else}
		<div class="overflow-x-auto rounded-lg border border-base-300 bg-base-100">
			<table class="table table-sm w-full bg-base-100">
				<thead class="bg-base-100 text-xs uppercase tracking-wide opacity-60">
					<tr>
						<th>Pod</th>
						<th>Status</th>
						<th>
							<span
								class="tooltip tooltip-bottom before:normal-case before:tracking-normal"
								data-tip="Flows without leader election, plus the leader-elected flows this pod leads"
							>
								Flows
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
										<Badge variant="warning">unreachable</Badge>
										<span class="opacity-70">{pod.unreachable_reason}</span>
									</span>
								{:else}
									<Badge variant="success">ok</Badge>
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
