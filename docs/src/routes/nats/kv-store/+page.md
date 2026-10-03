# NATS KV Store

Read, write, list, and delete keys in a NATS JetStream Key-Value bucket.

## Operations

| Operation | Description |
|---|---|
| `get` | Read a value by key. |
| `put` | Write `event.data.content` to `key`; without `key`, write every entry of `event.data.entries` under `key_prefix`. |
| `list` | List keys matching a prefix. |
| `delete` | Delete a key. |

## Configuration

```yaml
- nats_kv_store:
    name: write_flow
    operation: put
    credentials_path: /etc/nats/credentials.json
    url: "{{env.NATS_URL}}"
    bucket: flowgen_system
    key: "flows.{{event.data.path}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | One of `get`, `put`, `list`, `delete`. |
| `credentials_path` | string | optional | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `bucket` | string | required | KV bucket name. |
| `key` | string | | Key for get, put, and delete. Supports templating. |
| `key_prefix` | string | | Key prefix for `list`, and for the keys of `put` entries. Supports templating. |
| `prune` | bool | `false` | For a `put` of entries: delete the keys under `key_prefix` that no entry names. |
| `allow_empty` | bool | `false` | With `prune`: let an empty `entries` list delete every key under `key_prefix`. Otherwise it fails and deletes nothing. |
| `include_values` | bool | `false` | For `list`: also return each key's value under `values`. |
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

A `put` of entries returns instead:

| Field | Type | Description |
|---|---|---|
| `put` | array | Keys written because they were new or changed. |
| `deleted` | array | Keys removed by `prune`. |
| `unchanged` | int | Entries that already held their value. |

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
    url: "{{env.NATS_URL}}"
```

### Mirror a set of keys

The incoming event carries `{"entries": [{"key": "orders/sync", "value": "..."}]}`. Keys under `key_prefix` end up exactly as listed: new and changed values are written, identical ones are left alone, and keys that are no longer listed are deleted.

```yaml
- nats_kv_store:
    name: apply_flows
    operation: put
    bucket: flowgen_system
    key_prefix: "flows.platform/"
    prune: true
    credentials_path: /etc/nats/credentials.json
    url: "{{env.NATS_URL}}"
```

### Read from KV

```yaml
- nats_kv_store:
    name: load_config
    operation: get
    bucket: flowgen_system
    key: "config.my_setting"
    credentials_path: /etc/nats/credentials.json
    url: "{{env.NATS_URL}}"
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
    url: "{{env.NATS_URL}}"
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
    url: "{{env.NATS_URL}}"
```
