<script lang="ts">
	import Inspector from '$lib/Inspector.svelte';
	import StatusDot from '$lib/StatusDot.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { cluster } from '$lib/clusterStatus.svelte';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';

	let selectedId = $state<string | null>(null);
	let selected = $derived(cluster.status?.pods.find((pod) => pod.identity === selectedId) ?? null);
</script>

<svelte:head>
	<title>{pageTitle(m.monitor_tab_pods(), m.nav_monitor())}</title>
</svelte:head>

<div class="flex min-h-0 min-w-0 flex-1">
<div class="flex min-h-0 min-w-0 flex-1 flex-col">

<div class="shrink-0 bg-base-100 px-6 pb-4 pt-6">
	<div class="flex min-h-8 items-center">
		<div class="flex items-baseline gap-1.5 text-[0.9375rem] font-semibold">
			<span>{m.pods_all()}</span>
			{#if cluster.status}
				<span class="text-xs font-normal opacity-50">· {m.pods_count({ count: cluster.status.pods.length })}</span>
			{/if}
		</div>
	</div>
</div>

<div class="flex min-h-0 flex-1 flex-col overflow-y-auto px-6 pb-6">
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
		<div class="-mx-6 shrink-0 overflow-x-auto px-6">
			<table class="table table-sm w-full bg-base-100">
				<thead class="bg-base-100 text-xs font-medium text-base-content/50">
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
						<tr
							class="cursor-pointer transition-colors hover:bg-base-200 {selectedId === pod.identity
								? 'bg-base-200'
								: ''}"
							onclick={() => (selectedId = pod.identity)}
							onkeydown={(e) => {
								if (e.key === 'Enter' || e.key === ' ') {
									e.preventDefault();
									selectedId = pod.identity;
								}
							}}
							tabindex="0"
							role="button"
						>
							<td class="w-full max-w-0"><div class="truncate font-mono">{pod.identity}</div></td>
							<td class="text-sm">
								{#if pod.unreachable_reason}
									<span class="flex items-start gap-2">
										<StatusDot variant="warning" label={m.pods_status_unreachable()} />
										<span class="opacity-70">{pod.unreachable_reason}</span>
									</span>
								{:else}
									<StatusDot variant="success" label={m.pods_status_ok()} />
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
</div>

{#if selected}
	<Inspector title={selected.identity} label={m.pods_details()} onclose={() => (selectedId = null)}>
		{#snippet subtitle()}
			{#if selected.unreachable_reason}
				<StatusDot variant="warning" label={m.pods_status_unreachable()} />
			{:else}
				<StatusDot variant="success" label={m.pods_status_ok()} />
			{/if}
		{/snippet}
		<dl class="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1">
			{#if selected.address}
				<dt class="text-base-content/50">{m.pods_field_address()}</dt>
				<dd class="[overflow-wrap:anywhere]">{selected.address}</dd>
			{/if}
			{#if selected.flows !== undefined}
				<dt class="text-base-content/50">{m.pods_column_flows()}</dt>
				<dd class="tabular-nums">{selected.flows}</dd>
			{/if}
			{#if selected.unreachable_reason}
				<dt class="text-base-content/50">{m.pods_field_reason()}</dt>
				<dd class="whitespace-pre-wrap [overflow-wrap:anywhere]">{selected.unreachable_reason}</dd>
			{/if}
		</dl>
	</Inspector>
{/if}
</div>
