# NATS JetStream Publisher

Publishes events to a NATS JetStream subject.

## Configuration

```yaml
- nats_jetstream_publisher:
    name: publish_results
    credentials_path: /etc/nats/credentials.json
    subject: "results.processed"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `subject` | string | required | Subject to publish to. Supports templating. |
| `stream` | object | | Optional stream configuration (same as subscriber). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

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
