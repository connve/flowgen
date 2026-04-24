# Events

Events are the data records that flow between tasks. Each task receives events, processes them, and emits new events to the next task.

## Structure

| Field | Type | Description |
|---|---|---|
| `data` | EventData | Payload — JSON, Arrow RecordBatch, or Avro. |
| `subject` | string | Task name that produced the event. |
| `id` | string | Optional identifier set by the source (e.g., Salesforce record ID, NATS message ID). |
| `timestamp` | int | Creation time in microseconds since Unix epoch. |
| `task_id` | int | Task index in the flow. |
| `task_type` | string | Task type (e.g., `script`, `http_request`). |
| `meta` | map | Key-value metadata that travels with the event. |
| `error` | string | Error message if a task failed after retries. |

## Data formats

- **JSON** — default for REST APIs, webhooks, scripts. Accessed as `event.data.field` in templates and scripts.
- **Arrow RecordBatch** — columnar format for BigQuery, Parquet, CSV. Stays in Arrow through the flow without serialization overhead.
- **Avro** — binary format for Salesforce Pub/Sub and gRPC streams.

## Metadata

`event.meta` is a key-value map that travels through the entire event chain. Each task preserves meta from the upstream event automatically.

Use meta for:
- Passing context between tasks (e.g., source file path, request headers).
- Correlation — `event.meta.correlation_id` is auto-generated (UUIDv7) for every event chain.
- User context — `event.meta.auth` contains resolved user identity for authenticated webhooks.

Set meta in scripts:

```rhai
event.meta.source = "salesforce";
event.meta.batch_id = uuid();
event
```

Access meta in templates:

```yaml
endpoint: "https://api.example.com/{{event.meta.region}}/orders"
```

## Correlation ID

Every event chain gets a `correlation_id` in meta, generated automatically when the first event is created. It stays the same through all downstream tasks. Use it to trace a single request through the entire flow in logs.

Webhook, AI gateway, and MCP tasks set their own `correlation_id` for response streaming. These override the auto-generated one.

## Event chain

Tasks are connected by channels. Each task receives from the previous task and sends to the next:

```
[source] → channel → [transform] → channel → [sink]
```

If a task returns no data (e.g., a filter script returns `()`), the event chain stops — no downstream event is emitted.
