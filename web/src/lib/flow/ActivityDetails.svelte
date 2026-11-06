<script lang="ts" module>
	import StatusDot from '$lib/StatusDot.svelte';
	import { levelLabel, levelVariant, type ActivityLevel } from '$lib/logRecord';
	import { formatDuration } from '$lib/toolRuns.svelte';

	export { activitySummary };

	export interface Activity {
		flow: string;
		task: string | null;
		task_type: string | null;
		level: ActivityLevel;
		ts_ms: number;
		message: string;
		duration_ms?: number;
		event_id?: string;
		extra?: Array<[string, string]>;
	}

	/** The full record as JSON, for the clipboard. */
	export function activityJson(a: Activity): string {
		const payload: Record<string, unknown> = {
			timestamp: new Date(a.ts_ms).toISOString(),
			level: a.level,
			flow: a.flow,
			task: a.task,
			processor: a.task_type,
			message: a.message,
		};
		if (a.duration_ms !== undefined) payload.duration_ms = a.duration_ms;
		if (a.event_id) payload.event_id = a.event_id;
		if (a.extra && a.extra.length > 0) payload.attributes = Object.fromEntries(a.extra);
		return JSON.stringify(payload, null, 2);
	}
</script>

<script lang="ts">
	import InspectorSection from '$lib/InspectorSection.svelte';
	import { formatAbsolute, formatRelative } from '$lib/time';
	import { m } from '$lib/paraglide/messages.js';

	let { activity }: { activity: Activity } = $props();
</script>

<!-- The record's level, processor and duration, for the inspector's subtitle. -->
{#snippet activitySummary(activity: Activity)}
	<StatusDot variant={levelVariant(activity.level)} label={levelLabel(activity.level)} />
	{#if activity.task_type}
		<span class="truncate">· {activity.task_type}</span>
	{/if}
	{#if activity.duration_ms !== undefined}
		<span class="tabular-nums">· {formatDuration(activity.duration_ms)}</span>
	{/if}
{/snippet}

<div class="space-y-3">
	<dl class="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1">
		<dt class="text-base-content/50">{m.activity_field_timestamp()}</dt>
		<dd class="tabular-nums">{formatAbsolute(activity.ts_ms)}</dd>
		<dt class="text-base-content/50">{m.activity_field_when()}</dt>
		<dd>{formatRelative(activity.ts_ms)}</dd>
		{#if activity.event_id}
			<dt class="text-base-content/50">{m.activity_field_event()}</dt>
			<dd class="[overflow-wrap:anywhere]">{activity.event_id}</dd>
		{/if}
	</dl>
</div>

<InspectorSection title={m.activity_field_message()}>
	<p class="whitespace-pre-wrap break-words {activity.message ? '' : 'text-base-content/40'}">
		{activity.message || '—'}
	</p>
</InspectorSection>

{#if activity.extra && activity.extra.length > 0}
	<InspectorSection title={m.activity_field_attributes()}>
		<dl class="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-4 gap-y-1">
			{#each activity.extra as [key, value] (key)}
				<dt class="text-base-content/50">{key}</dt>
				<dd class="max-h-64 overflow-auto whitespace-pre-wrap [overflow-wrap:anywhere]">{value}</dd>
			{/each}
		</dl>
	</InspectorSection>
{/if}
