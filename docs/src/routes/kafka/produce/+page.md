# Kafka Produce

Publishes the incoming event to a Kafka topic and emits the delivery result (topic, partition, offset) downstream.

## Configuration

```yaml
- kafka_produce:
    name: publish_customer
    credentials_path: /etc/kafka/credentials.json
    brokers: "{{env.KAFKA_BROKERS}}"
    topic: customers
    message_key: "customer-{{event.data.name}}"
    create_or_update: true
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | | Path to Kafka credentials file. Omit to connect without authentication. See [Credentials](/docs/flowgen/kafka#credentials). |
| `brokers` | string | `localhost:9092` | Comma-separated bootstrap broker addresses. |
| `topic` | string | required | Topic to publish to. Supports templating. |
| `message_key` | string | | Message key template (e.g. `key-{{event.id}}`). See [Templating](/docs/flowgen/concepts/templating). |
| `create_or_update` | bool | `false` | When `true`, the topic is created from `topic_options` if it does not exist. When `false`, an error is returned if the topic is absent from the cluster. |
| `topic_options` | object | | Settings for a topic created by `create_or_update`. See [Topic creation](#topic-creation). |
| `ack_timeout` | duration | `30s` | How long to wait for the broker to acknowledge a message before the attempt fails. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `topic` | string | Topic the message was written to. |
| `partition` | int | Partition the message was written to. |
| `offset` | int | Offset of the written message. |

## Examples

**Publish the incoming event as the message payload:**

```yaml
- kafka_produce:
    name: publish_customer
    credentials_path: /etc/kafka/credentials.json
    topic: customers
    create_or_update: true
```

**Keyed messages with an id fallback:**

Incoming events do not always carry an `id` (e.g. NATS messages without a `Nats-Msg-Id` header, generate tasks). The producer patches a UUID v7 fallback into the render context, so `{{event.id}}` in `message_key` always resolves to a value:

```yaml
- kafka_produce:
    name: publish_orders
    credentials_path: /etc/kafka/credentials.json
    topic: orders
    message_key: "{{event.id}}"
```

### Topic creation

With `create_or_update: true`, a topic that does not exist is created from `topic_options`:

```yaml
- kafka_produce:
    name: publish_orders
    topic: orders
    create_or_update: true
    topic_options:
      partitions: 6
      replication_factor: 3
      retention: 7d
      config:
        cleanup.policy: delete
```

| Field | Type | Default | Description |
|---|---|---|---|
| `partitions` | int | `1` | Number of partitions. |
| `replication_factor` | int | `1` | Replication factor. Cannot exceed the number of brokers. |
| `retention` | duration | | How long the topic retains a message. Sets `retention.ms`. |
| `config` | map | | Any other topic-level setting, passed to the broker verbatim. A key here overrides the equivalent typed field. |

The defaults suit a local broker only — a single partition caps throughput at one consumer, and no replication loses the topic with its broker. Set both explicitly for anything else.

`topic_options` applies only when the topic is created. An existing topic is left as it is, so changing these values does not reshape a live topic.

On a cluster with `auto.create.topics.enable` (the broker default), a topic is otherwise created on first use with the broker's own defaults. The task checks for the topic over the admin protocol, which does not trigger that, so `topic_options` is what the topic is actually created with.

## Behaviour

The message payload is the incoming event's data — JSON is serialized as-is, `bytes`/Avro payloads are sent raw, and Arrow record batches are serialized as an Arrow IPC stream. Delivery is acknowledged before the result event is emitted downstream; if the broker does not acknowledge within `ack_timeout`, the task fails and retries per the [retry configuration](/docs/flowgen/concepts/retry).

Errors that cannot succeed on a second attempt — a missing topic with `create_or_update: false`, a template that fails to render, a payload that cannot be serialized — fail immediately instead of consuming the retry budget.
