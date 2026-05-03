# Flows

A flow is a YAML file that defines a sequence of tasks. Flows can be event-driven, scheduled, or streaming. Each flow runs as an independent unit within a flowgen worker.

## Structure

```yaml
flow:
  name: my_flow
  require_leader_election: true
  parallel_instances: 1
  tasks:
    - http_webhook:
        name: source
        # ...

    - script:
        name: transform
        # ...

    - log:
        name: sink
        # ...
```

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Unique identifier for the flow. Used in logging, metrics, and cache key namespacing. |
| `require_leader_election` | bool | `false` | When `true`, only the leader pod runs this flow. Other replicas wait in standby. |
| `parallel_instances` | int | `1` | Number of concurrent instances of this flow to run on the active pod. |
| `tasks` | list | required | List of tasks that form the flow. |

## Linear flows

By default, tasks run as a linear chain — each task receives events from the previous task and forwards to the next:

```yaml
flow:
  name: linear_example
  tasks:
    - http_webhook:
        name: ingest
        endpoint: /events
        method: POST

    - script:
        name: transform
        code: |
          let body = event.data;
          #{
            id: body.id,
            received_at: timestamp_now()
          }

    - log:
        name: sink
```

Internally:

```
[ingest] → channel → [transform] → channel → [sink]
```

The first task is a source (no input channel). The last task is a leaf (no output channel). Middle tasks receive, process, and forward.

## DAG flows

Tasks can form a directed acyclic graph using the `depends_on` field. A task with `depends_on` receives events from the named upstream tasks instead of the previous task in the list. Flowgen supports both fan-out (one parent → many children) and fan-in (many parents → one child).

### Fan-out

One source feeding multiple branches that run in parallel:

```yaml
flow:
  name: fan_out_example
  tasks:
    - http_webhook:
        name: ingest
        endpoint: /events
        method: POST

    - http_request:
        name: forward_to_a
        endpoint: https://service-a.internal/events
        method: POST
        payload:
          from_event: true
        depends_on: [ingest]

    - http_request:
        name: forward_to_b
        endpoint: https://service-b.internal/events
        method: POST
        payload:
          from_event: true
        depends_on: [ingest]
```

Every event from `ingest` is delivered to both `forward_to_a` and `forward_to_b`. The two branches run concurrently and the webhook responds only after both return.

### Fan-in

Multiple producers feeding the same downstream task:

```yaml
flow:
  name: fan_in_example
  tasks:
    - generate:
        name: tick_fast
        interval: "1m"

    - generate:
        name: tick_slow
        interval: "5m"

    - log:
        name: combined
        depends_on: [tick_fast, tick_slow]
```

`combined` receives events from both `tick_fast` and `tick_slow`, in arrival order. Each event is processed independently — there is no waiting or merging across branches.

### Mixed shapes

`depends_on` and the implicit "previous task" wiring can coexist. Tasks without `depends_on` fall back to depending on the previous task in the list, which keeps simple flows compact:

```yaml
flow:
  name: mixed
  tasks:
    - http_webhook:
        name: ingest
        endpoint: /events
        method: POST

    - script:
        name: validate
        code: |
          if event.data.id == () { throw "missing id"; }
          event.data

    - log:
        name: audit
        depends_on: [validate]

    - http_request:
        name: forward
        endpoint: https://service.internal/events
        method: POST
        payload:
          from_event: true
        depends_on: [validate]
```

`validate` implicitly depends on `ingest`. `audit` and `forward` both fan out from `validate`.

### Validation

The flow loader rejects:

- Cycles. A task may only depend on tasks that appear earlier in the `tasks` list.
- Unknown names in `depends_on`.
- Duplicate task names within a flow.

## End-to-end acknowledgement

Source tasks acknowledge the upstream message only after the entire downstream flow has completed successfully. This applies to every replayable source — message brokers, HTTP webhooks, MCP tool requests, scheduled generators.

### How it works

When a source produces an event, it attaches a per-event completion channel sized to the number of leaf tasks reachable through the directed acyclic graph. Every leaf signals completion when it finishes processing. The source is notified once every leaf has signalled — only then is the upstream message acked.

For the fan-out example above, `ingest` waits for **both** `forward_to_a` and `forward_to_b` to commit before responding. If either branch fails, the webhook returns an error and the caller can retry.

The same model applies to other sources:

- **Message broker subscribers** — broker redelivers when ack does not arrive within `ack_wait`, up to `max_deliver` attempts.
- **HTTP webhooks** — success returns 200, partial failure returns 500.
- **MCP tool requests** — the response is delivered only after every downstream leaf finishes; the LLM client sees a single result.
- **Scheduled `generate`** — cache state advances only on full success, so the next tick retries from the same point.

### Failure handling

A task that fails (after retries exhausted) emits an error event downstream rather than calling completion directly. Downstream tasks can inspect `event.error` and route it explicitly — to a dead-letter publisher, an audit log, or an alerting endpoint:

```yaml
- script:
    name: dlq_router
    code: |
      if event.error != () {
          // route to a different downstream branch via meta
          event.meta.lane = "dlq";
      }
      event.data
```

If no leaf processes the error event to completion, the source's acknowledgement timeout fires, the message is not acked, and redelivery handles the rest. Partial failures never silently ack: either every leaf reports success, or the source learns the flow did not complete.

## Leader election

When `require_leader_election` is enabled:

- Only one pod runs the flow at a time (active/passive).
- If the leader pod fails, another pod acquires the lease and takes over.
- Lease renewal happens automatically with configurable TTL.

This is useful for flows that must not run in parallel across pods — for example, a cron-triggered export that should only execute once.

## Parallel instances

The `parallel_instances` field controls how many copies of the flow run concurrently on the active pod. Useful for flows that can safely process messages in parallel.

## Flow discovery

Flowgen discovers flows from the configured path at startup:

```yaml
flows:
  path: /etc/flowgen/flows/    # directory: loads all *.yaml files
  # or
  path: /etc/flowgen/flows/*.yaml  # glob pattern
```

Each YAML file should contain a single flow definition.
