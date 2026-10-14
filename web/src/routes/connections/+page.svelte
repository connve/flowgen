<script lang="ts">
	import { base } from '$app/paths';
	import { goto, invalidate } from '$app/navigation';
	import type { Connection, FlowStatus } from '$lib/api';
	import Badge from '$lib/Badge.svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { connectorByName } from '$lib/connectors';
	import { m } from '$lib/paraglide/messages.js';
	import type { PageProps } from './$types';

	const REFRESH_MS = 30_000;

	let { data }: PageProps = $props();

	$effect(() => {
		const timer = setInterval(() => invalidate('app:connections'), REFRESH_MS);
		return () => clearInterval(timer);
	});

	const STATUS: Record<
		FlowStatus,
		{ label: () => string; variant: 'neutral' | 'success' | 'warning' | 'error' }
	> = {
		ok: { label: m.flows_status_ok, variant: 'success' },
		warn: { label: m.flows_status_warn, variant: 'warning' },
		error: { label: m.flows_status_error, variant: 'error' },
		idle: { label: m.flows_status_idle, variant: 'neutral' }
	};

	function subtitle(connection: Connection): string {
		const parts = connection.account ? [connection.account, ...connection.targets] : [...connection.targets];
		if (connection.perEvent) parts.push(m.connections_per_event());
		if (connection.onBehalfOfUser) parts.push(m.connections_on_behalf_of_user());
		return parts.join(' · ');
	}
</script>

<svelte:head>
	<title>{m.connections_page_title()}</title>
</svelte:head>

<div class="flex min-h-0 flex-1 flex-col overflow-y-auto p-6">
	{#if data.connections === null}
		<StateMessage tone="oops" title={m.connections_load_failed()} message={data.error ?? ''} />
	{:else if data.connections.length === 0}
		<StateMessage title={m.connections_empty_title()} message={m.connections_empty_message()} />
	{:else}
		<div class="shrink-0 overflow-x-auto rounded-lg border border-base-300 bg-base-100">
			<table class="table table-sm w-full bg-base-100">
				<thead class="bg-base-100 text-xs uppercase tracking-wide">
					<tr>
						<th>{m.connections_column_connection()}</th>
						<th>{m.connections_column_status()}</th>
						<th>{m.connections_column_flows()}</th>
					</tr>
				</thead>
				<tbody>
					{#each data.connections as connection (connection.id)}
						{@const connector = connectorByName(connection.connector)}
						{@const status = STATUS[connection.status]}
						{@const detail = subtitle(connection)}
						{@const flowsUrl = `${base}/?connection=${encodeURIComponent(connection.id)}`}
						<tr
							class="cursor-pointer transition-colors hover:bg-base-200"
							onclick={(e) => {
								if (!(e.target as Element).closest('a')) goto(flowsUrl);
							}}
							onkeydown={(e) => {
								if (e.target !== e.currentTarget) return;
								if (e.key === 'Enter' || e.key === ' ') {
									e.preventDefault();
									goto(flowsUrl);
								}
							}}
							tabindex="0"
							role="link"
						>
							<td>
								<div class="flex min-w-0 items-center gap-2">
									<img src={connector.iconUrl} alt="" class="h-4 w-4 shrink-0" />
									<span class="shrink-0 text-sm font-medium">{connector.label}</span>
									{#if detail}
										<span
											class="tooltip min-w-0 before:max-w-md before:whitespace-normal before:break-words"
											data-tip={detail}
										>
											<span class="block truncate text-xs opacity-60">{detail}</span>
										</span>
									{/if}
								</div>
							</td>
							<td><Badge variant={status.variant}>{status.label()}</Badge></td>
							<td class="whitespace-nowrap text-sm">
								<a href={flowsUrl} class="text-primary hover:underline"
									>{m.flows_count({ count: connection.flows.length })}</a
								>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
