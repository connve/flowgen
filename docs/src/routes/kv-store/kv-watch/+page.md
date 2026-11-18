# KV Watch

Start a flow with the value of a key, or of each key under a prefix, whenever it changes in a bucket of a key-value store. The `url` picks the store as in [KV Store](/docs/flowgen/kv-store/kv-store): `redb://<path>` or a NATS JetStream server.

At start, the task sends the current value of each watched key the flow has not finished; after that, each value put. A value the flow finished is not sent again, even after a restart. Values put while a value is in the flow are sent one after another. Deletes send nothing.

## Configuration

```yaml
- kv_watch:
    name: on_release
    url: "redb://.flowgen/state.redb"
    bucket: flowgen_system
    key: releases.latest
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `url` | string | required | `redb://<path>`, or the URL of a NATS server. Supports `{{env.VAR_NAME}}` templates. |
| `credentials_path` | string | | NATS credentials file. A redb file takes none. |
| `bucket` | string | required | Bucket name, created when missing. |
| `key` | string | | The key to watch, under a folder such as `releases.`. |
| `key_prefix` | string | | Watch every key under this prefix, which ends in `.` or `/` and is under a folder, such as `flows.`. Set either `key` or `key_prefix`. |
| `ack_timeout` | duration | | How long the flow may take to finish a value. Waits indefinitely when omitted. |
| `max_deliver` | int | | How many times a value is sent before it is skipped. Sent until the flow finishes it when omitted. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry) for connecting and for sending a failed value again. |

## Output

| Field | Description |
|---|---|
| `event.subject` | The key. |
| `event.data` | The value: parsed JSON, else text, else bytes. |

The digest of the last value the flow finished for each key is kept in the flow's cache.
