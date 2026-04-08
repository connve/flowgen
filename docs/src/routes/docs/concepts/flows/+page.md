# Flows

A flow is a YAML file that defines a sequence of tasks. Flows can be event-driven, scheduled, or streaming. Each flow runs as an independent unit within a flowgen worker.

## Structure

```yaml
flow:
  name: my_flow
  require_leader_election: true
  parallel_instances: 1
  tasks:
    - nats_jetstream_subscriber:
        name: source
        # ...

    - script:
        name: transform
        # ...

    - nats_jetstream_publisher:
        name: sink
        # ...
```

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Unique identifier for the flow. Used in logging, metrics, and cache key namespacing. |
| `require_leader_election` | bool | `false` | When `true`, only the leader pod runs this flow. Other replicas wait in standby. |
| `parallel_instances` | int | `1` | Number of concurrent instances of this flow to run on the active pod. |
| `tasks` | list | required | Ordered list of tasks that form the flow. |

## Leader election

Flowgen uses NATS JetStream key-value store for distributed leader election. When `require_leader_election` is enabled:

- Only one pod runs the flow at a time (active/passive).
- If the leader pod fails, another pod acquires the lease and takes over.
- Lease renewal happens automatically with configurable TTL.

This is useful for flows that should not run in parallel across pods — for example, a cron-triggered export that should only execute once.

## Parallel instances

The `parallel_instances` field controls how many copies of the flow run concurrently on the active pod. This is useful for flows that can safely process messages in parallel, such as NATS subscribers with `max_ack_pending > 1`.

## Task wiring

Tasks are connected sequentially via internal event channels. Each task receives events from the previous task and sends events to the next:

```
[subscriber] → channel → [script] → channel → [publisher]
```

- The first task is typically a subscriber or generator (no input channel).
- The last task is typically a publisher or sink (no output channel).
- Middle tasks receive, process, and forward events.

## Flow discovery

Flowgen discovers flows from the configured path at startup:

```yaml
flows:
  path: /etc/flowgen/flows/    # directory: loads all *.yaml files
  # or
  path: /etc/flowgen/flows/*.yaml  # glob pattern
```

Each YAML file should contain a single flow definition.
