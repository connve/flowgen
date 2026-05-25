# Buffer

Accumulates events into batches. Flushes when the batch reaches the configured size or the timeout expires.

## Configuration

```yaml
- buffer:
    name: batch
    size: 100
    timeout: "30s"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `size` | int | required | Number of events per batch. |
| `timeout` | duration | `30s` | Flush timeout — sends the batch even if not full. |
| `partition_key` | string | | Template for partitioned buffering. Events with the same key are batched together. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `batch` | array | Accumulated events. |
| `batch_size` | int | Number of events in the batch. |
| `flush_reason` | string | `size`, `timeout`, or `shutdown`. |

## Example: Partitioned buffering

```yaml
- buffer:
    name: batch_by_region
    size: 50
    timeout: "10s"
    partition_key: "{{event.data.region}}"
```

Events are grouped by region. Each partition flushes independently when it reaches 50 events or 10 seconds.
