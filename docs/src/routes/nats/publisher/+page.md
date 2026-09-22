# NATS JetStream Publisher

Publishes events to a NATS JetStream subject.

## Configuration

```yaml
- nats_jetstream_publisher:
    name: publish_results
    credentials_path: /etc/nats/credentials.json
    url: "{{env.NATS_URL}}"
    subject: "results.processed"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | optional | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `subject` | string | required | Subject to publish to. Supports templating. |
| `msg_id` | string | | NATS message ID for server-side deduplication. Can be a static string or templated from event data (e.g. `"{{event.data.record_id}}"`, `"fixed-key"`). Overrides `event.id` as the `Nats-Msg-Id` header. Requires `duplicate_window` on the stream to take effect. |
| `stream` | object | | Optional stream configuration (same as subscriber). |
| `stream.name` | string | required | Stream name. |
| `stream.subjects` | list | `[]` | Subject patterns for the stream. If empty when creating, defaults to `">"` (all subjects). |
| `stream.create_or_update` | bool | `false` | Create the stream if it does not exist, or update it to merge subjects and inherit limits. |
| `stream.retention` | string | | `limits`, `interest`, or `workqueue`. |
| `stream.discard` | string | | `old` or `new`. What to do when a stream limit is reached. |
| `stream.max_messages` | int | | Maximum number of messages in the **entire stream** across all subjects. If you want a cap per subject, use `max_messages_per_subject` instead. |
| `stream.discard_new_per_subject` | bool | | When `true` and `discard: new`, rejects new messages for a subject that has reached `max_messages_per_subject`. Without it, `discard: new` evicts old messages instead. |
| `stream.max_messages_per_subject` | int | | Maximum number of messages to keep **per subject**. Unlike `max_messages`, this limit is applied independently to each subject in the stream. |
| `stream.duplicate_window` | duration | | Window for server-side deduplication via `msg_id`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `stream` | string | JetStream stream name. |
| `sequence` | int | Message sequence number. |
| `duplicate` | bool | Whether the message was deduplicated. |

## Stream limits and shared streams

Stream limits such as `max_messages`, `max_messages_per_subject`, `discard`, and `discard_new_per_subject` are **stream-level settings** in NATS JetStream. They apply to every subject in the stream, not just the subject used by one task.

### `max_messages` vs `max_messages_per_subject`

Do not confuse these two limits:

- `max_messages` caps the **total number of messages in the stream** across all subjects. With `discard: new`, every publish after the cap is rejected, regardless of which subject it targets.
- `max_messages_per_subject` caps the number of messages **for each subject individually**. With `discard: new` and `discard_new_per_subject: true`, a second publish to the same subject is rejected, but a publish to a different subject still succeeds.

Using `max_messages: 1` on a stream with multiple subjects means the entire stream holds only one message, no matter how many subjects there are.

### `max_messages_per_subject` without `discard_new_per_subject`

`max_messages_per_subject` caps how many messages each subject keeps. With `discard: new` but **without** `discard_new_per_subject: true`, NATS still accepts every publish and evicts the oldest messages on that subject. The publisher receives a normal ack, but the stream no longer contains the older messages.

If two tasks share a stream and one task sets `max_messages_per_subject`, the other task's subjects inherit the same cap through `create_or_update`:

```yaml
- nats_jetstream_publisher:
    name: publish_created
    credentials_path: /etc/nats/credentials.json
    subject: "orders.created"
    stream:
      name: ORDERS
      subjects:
        - "orders.created"
      create_or_update: true
      discard: new
      max_messages_per_subject: 1

- nats_jetstream_publisher:
    name: publish_updated
    credentials_path: /etc/nats/credentials.json
    subject: "orders.updated"
    stream:
      name: ORDERS
      subjects:
        - "orders.created"
        - "orders.updated"
      create_or_update: true
```

Because `create_or_update` merges the existing stream config, `publish_updated` inherits `max_messages_per_subject: 1`. Every new `orders.updated` message evicts the previous one, so only the latest message per subject remains in the stream.

### Isolating subjects with `discard_new_per_subject`

To enforce the cap per subject and get an explicit error when a subject is full, set `discard_new_per_subject: true`:

```yaml
stream:
  name: ORDERS
  subjects:
    - "orders.>"
  create_or_update: true
  discard: new
  discard_new_per_subject: true
  max_messages_per_subject: 1
```

With this flag, a second publish to the same subject is rejected and the task emits an error event. A publish to a different subject on the same stream still succeeds.

### Recommended: separate streams per task

The safest way to avoid accidental cross-task interference is to use a separate stream for each task:

```yaml
- nats_jetstream_publisher:
    name: publish_created
    subject: "orders.created"
    stream:
      name: ORDERS_CREATED
      subjects:
        - "orders.created"
      create_or_update: true
      discard: new
      discard_new_per_subject: true
      max_messages_per_subject: 1
```

## Example

```yaml
- nats_jetstream_publisher:
    name: publish_orders
    credentials_path: /etc/nats/credentials.json
    subject: "orders.processed.{{event.data.region}}"
    stream:
      name: PROCESSED_ORDERS
      subjects:
        - "orders.processed.>"
      create_or_update: true
```
