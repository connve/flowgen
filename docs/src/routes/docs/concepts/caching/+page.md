# Caching

Flowgen provides a cache for state management, deduplication, and leader election.

## Backends

| Backend | Description |
|---|---|
| **NATS JetStream KV** | Distributed key-value store. Shared across all replicas. |
| **In-memory** | Local HashMap. Single-node deployments. Lost on restart. |

If NATS cache is enabled but fails to connect, flowgen falls back to in-memory automatically. If cache is disabled or not configured, in-memory is used.

## Configuration

```yaml
cache:
  enabled: true
  type: nats
  credentials_path: /etc/nats/credentials.json
  url: nats://localhost:4222
  db_name: flowgen_cache
  history: 10
  tombstone_ttl: "1h"
```

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Enable distributed cache. |
| `type` | string | required | Cache backend: `nats`. |
| `credentials_path` | string | required | Path to NATS credentials. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `db_name` | string | `flowgen_cache` | KV bucket name. |
| `history` | int | 10 | Historical entries retained per key. |
| `tombstone_ttl` | duration | `1h` | TTL for delete markers. Enables per-key TTL on entries. |

## Operations

The cache is exposed to Rhai scripts via `ctx.cache`. Keys are namespaced by the flow name, so two flows can use the same logical key without colliding.

| Operation | Returns | Description |
|---|---|---|
| `ctx.cache.get(key)` | `()` or string | Read a value. Returns `()` if not found. |
| `ctx.cache.put(key, value)` | bool | Store a value (string or integer) with no expiration. |
| `ctx.cache.put(key, value, ttl_secs)` | bool | Store with a time-to-live in seconds. |
| `ctx.cache.delete(key)` | bool | Delete a key. |
| `ctx.cache.list_keys(prefix)` | array of strings | List keys matching a prefix (with the flow-name namespace stripped from results). |


## Access in scripts

```rhai
// Write without expiration.
ctx.cache.put("order." + event.data.id, event.data.status);

// Write with TTL (seconds).
ctx.cache.put("seen." + event.data.id, "1", 3600);

// Read.
let status = ctx.cache.get("order." + event.data.id);

// Delete.
ctx.cache.delete("order." + event.data.id);

// List.
let keys = ctx.cache.list_keys("order.");
for key in keys {
    print(key);
}
```

## Use cases

**Deduplication:**

```rhai
let key = "seen." + event.data.id;
if ctx.cache.get(key) != () {
  return ();
}
ctx.cache.put(key, "1", 86400);
event
```

**State tracking:**

```rhai
let key = "last_sync." + event.data.source;
let last = ctx.cache.get(key);
ctx.cache.put(key, timestamp_to_iso(timestamp_now()));
event.data.last_sync = last;
event
```

## Leader election

The cache is also used for distributed leader election. When `require_leader_election: true` is set on a flow, flowgen uses the KV store to coordinate which replica runs the flow. Only one replica holds the lease at a time.
