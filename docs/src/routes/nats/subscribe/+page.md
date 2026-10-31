# NATS JetStream Subscribe

Consumes messages from a NATS JetStream stream. Source task — typically first in a flow.

## Configuration

```yaml
- nats_jetstream_subscribe:
    name: order_events
    credentials_path: /etc/nats/credentials.json
    url: "{{env.NATS_URL}}"
    subject: "orders.>"
    durable_name: order_processor
    stream:
      name: ORDERS
      subjects:
        - "orders.>"
      create_or_update: true
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | optional | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `subject` | string | required | Subject to subscribe to (supports wildcards). |
| `durable_name` | string | | Durable consumer name for persistent subscriptions. |
| `stream` | object | | Stream configuration (see below). |
| `max_in_flight` | int | `64` | Messages in the flow at once. `1` keeps them in order. |
| `max_messages_per_batch` | int | `200` | Most messages one pull request asks for. The subscriber asks only for as many as it has free `max_in_flight` slots. |
| `max_ack_pending` | int | | Max unacknowledged messages across every pod running the flow. |
| `max_deliver` | int | | Max delivery attempts before discarding. |
| `batch_expires` | duration | `30s` | How long each batch request waits on the server before returning empty. Prevents empty fetch loops. |
| `delay` | duration | | Deprecated and ignored. Use `throttle` to rate-limit individual messages. |
| `throttle` | duration | | Delay between individual messages. |
| `ack_timeout` | duration | wait indefinitely | Max time a message may stay in the flow, from receipt. Also sets the consumer `ack_wait`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Stream options

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Stream name. |
| `subjects` | list | required | Subject patterns (supports wildcards). |
| `create_or_update` | bool | false | Create or update stream if it does not match. |
| `max_age` | duration | | Max message age. |
| `max_messages` | int | | Max messages in stream. |
| `max_bytes` | int | | Max stream size in bytes. |
| `retention` | string | `limits` | Retention policy: `limits`, `interest`, `work_queue`. |
| `discard` | string | `old` | Discard policy: `old`, `new`. |
| `discard_new_per_subject` | bool | false | When `true`, rejects new messages if `max_messages_per_subject` limit is reached for that subject. Requires `discard: new`. |
| `duplicate_window` | duration | | Deduplication window. |

## Delivery

A message is acked when every last task of the flow has finished with it. From receipt, also while it waits for a busy next task, the subscriber tells NATS the message is still in progress, so NATS doesn't redeliver it. A message the flow holds for more than 5 minutes is logged as a warning every 5 minutes. A flow without a next task acks the message once it is read. A failed or timed-out flow asks NATS to redeliver the message after the [redelivery delay](/docs/flowgen/concepts/retry#redelivery). Delivery is at least once: a message can still arrive twice, for example after a pod stops mid-flow, so tasks with side effects should check an idempotency key.

When the flow stops or reloads, the messages it holds go back to NATS for immediate redelivery, and progress acks stop. If the subscriber ever stops reading while the flow runs, the flow fails and restarts.

## Durable consumer

The subscriber creates the durable when it is missing. An existing durable gets the flow's `ack_timeout`, `max_deliver`, `max_ack_pending` and `max_waiting` on start, and loses a `backoff` that older versions set. When NATS refuses the update, the subscriber logs a warning with the live and the flow's values and keeps the durable as it is. A durable whose filter subject differs from `subject` fails the task; delete it or use another `durable_name`.

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) / [Avro](https://docs.rs/apache-avro/latest/apache_avro/) / [Arrow](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) | [async-nats](https://docs.rs/async-nats/latest/async_nats/) | Message payload from JetStream. Format depends on how the message was published. |

| Event field | Value |
|---|---|
| `event.subject` | Subject the message was published to. |
| `event.id` | The `Nats-Msg-Id` header, when the message has one. |
| `event.meta.headers` | Message headers. A header set more than once keeps its first value. |

## Example

```yaml
flow:
  tasks:
    - nats_jetstream_subscribe:
        name: orders
        credentials_path: /etc/nats/credentials.json
        subject: "orders.created"
        durable_name: order_processor
        max_ack_pending: 100
        stream:
          name: ORDERS
          subjects:
            - "orders.>"
          create_or_update: true
          retention: work_queue

    - script:
        name: transform
        code: |
          let order = event.data;
          #{id: order.id, total: order.amount}

    - log:
        name: output
```
