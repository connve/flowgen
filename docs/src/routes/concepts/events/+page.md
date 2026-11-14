# Events

Events are the data records that flow between tasks. Each task receives events, processes them, and emits new events to the next task.

## Structure

| Field | Type | Description |
|---|---|---|
| `data` | EventData | Payload — JSON, Arrow RecordBatch, Avro, or raw bytes. |
| `subject` | string | Set by the task that produced the event: the task name for most tasks, the source's own subject for a source (e.g., a NATS subject, a Kafka topic or a table). |
| `id` | string | Optional identifier set by the source (e.g., a record ID or a NATS message ID). |
| `timestamp` | int | Creation time in microseconds since Unix epoch; a source can set the time of the record it read (e.g., Kafka record time). |
| `task_id` | int | Task index in the flow. |
| `task_type` | string | Task type (e.g., `script`, `http_request`). |
| `meta` | map | Key-value metadata that travels with the event. |
| `error` | string | Error message if a task failed after retries. |

## Data formats

- **JSON** — default for REST APIs, webhooks, scripts. Accessed as `event.data.field` in templates and scripts.
- **Arrow RecordBatch** — columnar format for query results, Parquet, CSV. Stays in Arrow through the flow without serialization overhead. Where a task, script or template reads it as JSON, it is an array of objects, one per row, with every column as a key, `null` for a null value, and binary values in base64.
- **Avro** — binary format of gRPC and pub/sub streams.
- **Bytes** — opaque binary payload for content that cannot be safely coerced to UTF-8 (archive contents, binary OCI layers, `http_request` responses with `response_type: bytes`). Handlebars templates against `event.data` see a base64-encoded string so they do not crash on binary; downstream tasks that need the raw bytes pattern-match `EventData::Bytes` directly. Only tasks that opt in accept this variant — text- or JSON-shaped processors reject it with an `UnsupportedEventData` error.

## Metadata

`event.meta` is a key-value map that travels through the entire event chain. Each task preserves meta from the upstream event automatically.

Use meta for:
- Passing context between tasks (e.g., source file path, request headers).
- Correlation — `event.meta.correlation_id` is auto-generated (UUIDv7) for every event chain.
- User context — `event.meta.auth` contains resolved user identity for authenticated webhooks.

Set meta in scripts:

```rhai
event.meta.source = "crm";
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
