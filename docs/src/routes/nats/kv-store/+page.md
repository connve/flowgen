# NATS KV Store

Read, write, list, and delete keys in a NATS JetStream Key-Value bucket.

## Operations

| Operation | Description |
|---|---|
| `get` | Read a value by key. |
| `put` | Write a value. Uses `event.data.content` if present, otherwise the full event data. |
| `list` | List keys matching a prefix. |
| `delete` | Delete a key. |

## Configuration

```yaml
- nats_kv_store:
    name: write_flow
    operation: put
    credentials_path: /etc/nats/credentials.json
    url: nats://localhost:4222
    bucket: flowgen_system
    key: "flows.{{event.data.path}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | One of `get`, `put`, `list`, `delete`. |
| `credentials_path` | string | required | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `bucket` | string | required | KV bucket name. |
| `key` | string | | Key for get, put, and delete. Supports templating. |
| `key_prefix` | string | | Key prefix for list operations. Supports templating. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

### `get`

| Field | Type | Description |
|---|---|---|
| `key` | string | Requested key. |
| `content` | string / null | Stored value, or null if not found. |
| `found` | bool | Whether the key exists. |

### `put`

| Field | Type | Description |
|---|---|---|
| `key` | string | Written key. |
| `revision` | int | KV store revision number. |

### `delete`

| Field | Type | Description |
|---|---|---|
| `key` | string | Deleted key. |

### `list`

| Field | Type | Description |
|---|---|---|
| `keys` | array | Matching key names. |
| `count` | int | Number of keys returned. |
| `prefix` | string | Prefix that was searched. |

## Examples

### Write to KV

```yaml
- nats_kv_store:
    name: save_config
    operation: put
    bucket: flowgen_system
    key: "config.{{event.data.name}}"
    credentials_path: /etc/nats/credentials.json
```

### Read from KV

```yaml
- nats_kv_store:
    name: load_config
    operation: get
    bucket: flowgen_system
    key: "config.my_setting"
    credentials_path: /etc/nats/credentials.json
```

Returns `{key, content, found}`. `content` is null if the key does not exist.

### List keys

```yaml
- nats_kv_store:
    name: list_flows
    operation: list
    bucket: flowgen_system
    key_prefix: "flows."
    credentials_path: /etc/nats/credentials.json
```

Returns `{prefix, keys, count}`.

### Delete a key

```yaml
- nats_kv_store:
    name: remove_config
    operation: delete
    bucket: flowgen_system
    key: "config.old_setting"
    credentials_path: /etc/nats/credentials.json
```
