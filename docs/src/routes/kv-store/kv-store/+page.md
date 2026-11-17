# KV Store

Read, write, list, and delete keys in a bucket of a key-value store. The `url` picks the store:

| URL | Store |
|---|---|
| `redb://<path>` | A redb file, relative to the working directory unless absolute. Created when missing. |
| `nats://`, `tls://`, `ws://`, `wss://` or `<host>:<port>` | A NATS JetStream server. |

Any other scheme fails when the flow loads.

## Operations

| Operation | Description |
|---|---|
| `get` | Read a value by key. |
| `put` | Write `event.data.content` to `key`; without `key`, write every entry of `event.data.entries` under `key_prefix`. |
| `list` | List keys under `key_prefix`. |
| `delete` | Delete a key. |

## Configuration

```yaml
- kv_store:
    name: write_flow
    url: "redb://.flowgen/state.redb"
    bucket: flowgen_system
    operation: put
    key: "flows.{{event.data.path}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `url` | string | required | `redb://<path>`, or the URL of a NATS server. Supports `{{env.VAR_NAME}}` templates. |
| `credentials_path` | string | | NATS credentials file. A redb file takes none. |
| `bucket` | string | required | Bucket name, created when missing. |
| `operation` | string | required | One of `get`, `put`, `list`, `delete`. |
| `key` | string | | Key for get, put, and delete; required for get and delete. Letters, digits and `-/_=.`, not starting or ending with `.`. Supports templating. |
| `key_prefix` | string | | Key prefix for `list`, and for the keys of `put` entries; a `put` needs `key` or `key_prefix`. Supports templating. |
| `prune` | bool | `false` | For a `put` of entries: delete the keys under `key_prefix` that no entry names. Requires a `key_prefix` ending in `.` or `/`. |
| `allow_empty` | bool | `false` | With `prune`: let an empty `entries` list delete every key under `key_prefix`. Otherwise it fails and deletes nothing. |
| `include_values` | bool | `false` | For `list`: also return each key's value under `values`. |
| `ttl` | duration | | For `put`: how long the value lives, e.g. `30d`. On NATS it needs a bucket with per-key TTL (NATS 2.11+), as this task creates; on another bucket the task fails to start. |
| `nats` | object | | Options of a NATS server `url`: `history`, the values the bucket keeps per key, 1 to 64 (default `1`), set when this task creates the bucket. Set on a `redb://` URL, the task fails to load. |
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

### `put` and `delete`

| Field | Type | Description |
|---|---|---|
| `key` | string | Written or deleted key. |

A `put` of entries returns instead:

| Field | Type | Description |
|---|---|---|
| `put` | array | Keys written because they were new or changed. |
| `deleted` | array | Keys removed by `prune`. |
| `unchanged` | int | Entries that already held their value. |

### `list`

| Field | Type | Description |
|---|---|---|
| `keys` | array | Keys under the prefix. |
| `count` | int | Number of keys returned. |
| `prefix` | string | Prefix that was listed. |
| `values` | object | Each key's value, with `include_values`. |

## Examples

### Mirror a set of keys

The incoming event carries `{"entries": [{"key": "orders/sync", "value": "..."}]}`. Keys under `key_prefix` end up exactly as listed: new and changed values are written, identical ones are left alone, and keys that are no longer listed are deleted.

```yaml
- kv_store:
    name: apply_flows
    url: "nats://{{env.NATS_HOST}}:4222"
    credentials_path: "{{env.FLOWGEN_CREDENTIALS}}/nats.json"
    bucket: flowgen_system
    operation: put
    key_prefix: "flows."
    prune: true
```

### Read a key

```yaml
- kv_store:
    name: load_setting
    url: "redb://.flowgen/state.redb"
    bucket: settings
    operation: get
    key: "my_setting"
```

Returns `{key, content, found}`. `content` is null if the key does not exist.
