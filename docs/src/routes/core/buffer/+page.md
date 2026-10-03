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
| `timeout` | duration | `30s` | Flush timeout — sends the batch even if not full, measured from the first event in the batch. |
| `partition_key` | string | | Template for partitioned buffering. Events with the same key are batched together. |
| `flush_on_completion` | bool | `false` | Flush when the source marks its last event, e.g. the last file of a [Git Sync](/docs/flowgen/git/sync) or [OCI Sync](/docs/flowgen/oci/sync) pull. A batch cut short by `timeout` reports `flush_reason: timeout`, so a flow that needs the whole pull can skip it. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `batch` | array | Accumulated events. |
| `batch_size` | int | Number of events in the batch. |
| `flush_reason` | string | `size`, `timeout`, `completion`, or `shutdown`. |

## Example: Partitioned buffering

```yaml
- buffer:
    name: batch_by_region
    size: 50
    timeout: "10s"
    partition_key: "{{event.data.region}}"
```

Events are grouped by region. Each partition flushes independently when it reaches 50 events or 10 seconds.
