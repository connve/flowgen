<script lang="ts">
	import { base } from '$app/paths';
	import { goto, invalidate } from '$app/navigation';
	import { encodePath, type Connection, type FlowStatus } from '$lib/api';
	import Icon from '@iconify/svelte';
	import { Badge, IconButton, Inspector, InspectorSection, StateMessage, StatusDot } from '@connve/ui';
	import Actor from '$lib/Actor.svelte';
	import CredentialsForm from '$lib/CredentialsForm.svelte';
	import { connectorByName } from '$lib/connectors';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';
	import { formatAbsolute } from '$lib/time';
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
		return parts.length > 0 ? parts.join(' · ') : (connection.credentialsName ?? '');
	}

	/** The connection's name: its credentials, or where it connects without any. */
	function nameOf(connection: Connection, label: string): string {
		return connection.credentialsName ?? connection.targets[0] ?? label;
	}

	let selectedId = $state<string | null>(null);
	let selected = $derived(data.connections?.find((c) => c.id === selectedId) ?? null);

	function flowsUrl(connection: Connection): string {
		return `${base}/?connection=${encodeURIComponent(connection.id)}`;
	}
</script>

<svelte:head>
	<title>{pageTitle(m.nav_connections())}</title>
</svelte:head>

<section class="flex h-[calc(100vh-4rem)] min-w-0">
<div class="flex min-w-0 flex-1 flex-col overflow-hidden">
<div class="shrink-0 bg-base-100 px-6 pb-4 pt-6">
	<div class="flex min-h-8 items-center">
		<div class="flex items-baseline gap-1.5 text-[0.9375rem] font-semibold">
			<span>{m.connections_all()}</span>
			{#if data.connections}
				<span class="text-xs font-normal opacity-50">· {m.connections_count({ count: data.connections.length })}</span>
			{/if}
		</div>
	</div>
</div>

<div class="flex min-h-0 flex-1 flex-col overflow-y-auto px-6 pb-6">
	{#if data.connections === null}
		<StateMessage tone="oops" title={m.connections_load_failed()} message={data.error ?? ''} />
	{:else if data.connections.length === 0}
		<StateMessage title={m.connections_empty_title()} message={m.connections_empty_message()} />
	{:else}
		<div class="-mx-6 shrink-0 overflow-x-auto px-6">
			<table class="table table-sm w-full bg-base-100">
				<thead class="bg-base-100 text-xs font-medium text-base-content/50">
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
						<tr
							class="cursor-pointer transition-colors hover:bg-base-200 {selectedId === connection.id
								? 'bg-base-200'
								: ''}"
							onclick={(e) => {
								if (!(e.target as Element).closest('a')) selectedId = connection.id;
							}}
							ondblclick={() => goto(flowsUrl(connection))}
							onkeydown={(e) => {
								if (e.target !== e.currentTarget) return;
								if (e.key === 'Enter' || e.key === ' ') {
									e.preventDefault();
									selectedId = connection.id;
								}
							}}
							tabindex="0"
							role="button"
						>
							<td class="w-full max-w-0">
								<div class="flex min-w-0 items-center gap-2">
									<img src={connector.iconUrl} alt="" class="h-4 w-4 shrink-0" />
									<span class="shrink-0 text-sm font-medium">{connector.label}</span>
									{#if !connection.credentialsFound}
										<Badge variant="warning">{m.credentials_missing()}</Badge>
									{/if}
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
							<td><StatusDot variant={status.variant} label={status.label()} /></td>
							<td class="whitespace-nowrap text-sm">
								<a href={flowsUrl(connection)} class="text-primary hover:underline"
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
</div>

{#if selected}
	{@const connector = connectorByName(selected.connector)}
	{@const status = STATUS[selected.status]}
	{@const name = nameOf(selected, connector.label)}
	{@const addable = data.credentials ? selected.credentialsName : undefined}
	{@const changed = addable && selected.credentialsFound ? data.changes[addable] : undefined}
	<Inspector title={name} label={m.connections_details()} onclose={() => (selectedId = null)}>
		{#snippet subtitle()}
			<img src={connector.iconUrl} alt="" class="h-3.5 w-3.5 shrink-0" />
			{#if name !== connector.label}
				<span>{connector.label}</span>
				<span aria-hidden="true">·</span>
			{/if}
			{#if selected.credentialsFound}
				<StatusDot variant={status.variant} label={status.label()} />
			{:else}
				<StatusDot variant="warning" label={m.credentials_missing()} />
			{/if}
		{/snippet}
		{#snippet actions()}
			<IconButton icon="tabler:arrow-up-right" label={m.connections_show_flows()} href={flowsUrl(selected)} />
		{/snippet}
		{#if addable}
			{#key selected.id}
				<CredentialsForm name={addable} {connector} found={selected.credentialsFound} />
			{/key}
		{/if}
		{#if changed || selected.account || selected.targets.length > 0 || selected.perEvent || selected.onBehalfOfUser}
			<dl class="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1">
				{#if changed}
					<dt class="text-base-content/50">{m.connections_field_updated()}</dt>
					<dd>
						<a href="{base}/changes/{encodeURIComponent(changed.id)}" class="hover:underline">
							<Actor label={changed.by} /> · {formatAbsolute(changed.at)}
						</a>
					</dd>
				{/if}
				{#if selected.account}
					<dt class="text-base-content/50">{m.connections_field_account()}</dt>
					<dd class="[overflow-wrap:anywhere]">{selected.account}</dd>
				{/if}
				{#if selected.targets.length > 0}
					<dt class="text-base-content/50">
						{selected.targets.length > 1 ? m.connections_field_targets() : m.connections_field_target()}
					</dt>
					<dd class="space-y-1 [overflow-wrap:anywhere]">
						{#each selected.targets as target (target)}
							<div>{target}</div>
						{/each}
					</dd>
				{/if}
				{#if selected.perEvent || selected.onBehalfOfUser}
					<dt class="text-base-content/50">{m.connections_field_signs_in()}</dt>
					<dd>
						{selected.perEvent ? m.connections_signs_in_per_event() : m.connections_signs_in_as_user()}
					</dd>
				{/if}
			</dl>
		{/if}
		<InspectorSection title={m.connections_column_flows()}>
			<ul class="space-y-px">
				{#each selected.flows as flow (flow)}
					{@const slash = flow.lastIndexOf('/')}
					<li>
						<a
							href="{base}/flows/{encodePath(flow)}"
							class="-mx-2 flex h-9 items-center gap-2 rounded-md px-2 transition-colors hover:bg-base-200/60"
						>
							<Icon icon="tabler:binary-tree" class="h-4 w-4 shrink-0 text-base-content/40" />
							<span class="min-w-0 truncate font-medium">{flow.slice(slash + 1)}</span>
							{#if slash > 0}
								<span class="min-w-0 flex-1 truncate text-xs text-base-content/50">{flow.slice(0, slash)}</span>
							{:else}
								<span class="flex-1"></span>
							{/if}
							<Icon icon="tabler:chevron-right" class="h-4 w-4 shrink-0 text-base-content/30" />
						</a>
					</li>
				{/each}
			</ul>
		</InspectorSection>
	</Inspector>
{/if}
</section>
