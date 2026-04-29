# Configuration

Flowgen's worker reads a single YAML file (passed via `--config`). It has five top-level sections:

```yaml
flows:       # Flow discovery (required).
cache:       # Distributed cache backend (optional).
resources:   # External resource loading (optional).
worker:      # HTTP/MCP servers, retry defaults, channel sizing (optional).
telemetry:   # OpenTelemetry export (optional).
```

Only `flows` is required. Every other section has working defaults.

## Minimal config

```yaml
flows:
  path: /etc/flowgen/flows/
```

Discovers every `.yaml`/`.yml`/`.json` file under the path and runs them with an in-memory cache, no resources directory, no HTTP server, and no telemetry.

## Full reference

```yaml
flows:
  path: /etc/flowgen/flows/

cache:
  enabled: true
  type: nats
  credentials_path: /etc/nats/credentials.json
  url: nats://localhost:4222
  db_name: flowgen_cache
  history: 10
  tombstone_ttl: "1h"

resources:
  path: /etc/flowgen/resources/

worker:
  http_server:
    enabled: true
    port: 3000
    path: /api/flowgen/workers
    credentials_path: /etc/flowgen/credentials/http.json
    auth:
      type: jwt
      secret: "shared-secret"

  mcp_server:
    enabled: true
    port: 3001
    path: /mcp
    credentials_path: /etc/flowgen/credentials/mcp.json

  retry:
    max_attempts: 10
    initial_backoff: "1s"

  event_buffer_size: 10000000

telemetry:
  enabled: true
  otlp_endpoint: http://localhost:4317
  service_name: flowgen
  metrics_export_interval: "60s"
```

## `flows`

Flow discovery. Filesystem and distributed cache can be used independently or together; the worker merges flows from both sources at startup.

| Field | Type | Default | Description |
|---|---|---|---|
| `path` | string | | Directory or glob pattern. Filesystem-mode loads every `.yaml`/`.yml`/`.json` file recursively. Omit to skip filesystem loading. |
| `cache` | object | | Cache-mode flow loading. Loads flows from the metadata cache in addition to the filesystem. |
| `cache.enabled` | bool | required | Enable cache-mode flow loading. |
| `cache.prefix` | string | `flowgen.flows` | Cache key prefix for flow entries. |
| `cache.db_name` | string | `flowgen_system` | Cache bucket holding the flow definitions. |

Both sources can be active at the same time. Use this for gradual migration: a small set of bootstrap flows mounted from disk (typically a sync flow that pulls user flows from Git into the cache), with the bulk of user flows loaded from the cache. On a name collision the filesystem entry wins, so a locally mounted bootstrap flow cannot be silently overridden by a stale cache entry.

Cache-mode flow loading is useful when flows are provisioned dynamically — for example, a control plane writes flow YAML into NATS KV and every flowgen replica picks them up.

## `cache`

Distributed cache backend. When omitted, flowgen uses an in-memory cache (single-node only).

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Set `false` to fall back to in-memory cache. |
| `type` | string | required | Backend type. Currently `nats`. |
| `credentials_path` | string | required | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `db_name` | string | `flowgen_cache` | KV bucket name. |
| `history` | int | `10` | Historical entries retained per key. Only applies when the bucket is created. |
| `tombstone_ttl` | duration | `1h` | TTL for delete/purge tombstones. Required for per-key TTLs on cache entries to work. |

If NATS is configured but unreachable, flowgen falls back to in-memory automatically and logs a warning. See [Caching](/docs/concepts/caching).

## `resources`

External resource loading for SQL queries, prompts, scripts, schemas. See [Resources](/docs/concepts/resources).

| Field | Type | Default | Description |
|---|---|---|---|
| `path` | string | | Filesystem base directory for resources. Resource keys resolve relative to this path. |
| `cache` | object | | Cache-backed resources. When `cache.enabled` is true, resources load from the cache. |
| `cache.enabled` | bool | required | Enable cache-backed resources. |
| `cache.prefix` | string | `flowgen.resources` | Cache key prefix for resource entries. |
| `cache.db_name` | string | `flowgen_system` | Cache bucket holding resource entries. |

When `resources` is omitted, only inline content is supported — any task that uses `{ resource: ... }` will fail at startup with a clear error.

## `worker`

Worker-process configuration: HTTP server, MCP server, retry defaults, channel sizing.

| Field | Type | Default | Description |
|---|---|---|---|
| `http_server` | object | | HTTP server for `http_webhook` and `ai_gateway` tasks. |
| `mcp_server` | object | | MCP server for `mcp_tool` tasks. |
| `retry` | object | `{max_attempts: 10, initial_backoff: "1s"}` | Default retry config for every task. See [Retry](/docs/concepts/retry). |
| `event_buffer_size` | int | `10000000` | Capacity of inter-task event channels. Each slot is roughly 128 bytes, so the default reserves ~1.2 GB across the worker. |

### `worker.http_server`

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for `http_webhook` and `ai_gateway` tasks to start. |
| `port` | int | `3000` | Listening port. |
| `path` | string | | Optional path prefix applied to every registered route. |
| `credentials_path` | string | | Worker-level shared bearer/basic credentials. Tasks override per-route. See [Credentials](/docs/concepts/credentials). |
| `auth` | object | | User-level authentication provider (JWT, OIDC, session). See [Authentication](/docs/concepts/auth). |

### `worker.mcp_server`

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for `mcp_tool` tasks to register. |
| `port` | int | `3001` | Listening port. |
| `path` | string | `/mcp` | MCP endpoint path. |
| `credentials_path` | string | | Worker-level shared API key credentials. Individual tools can override. |

### `worker.retry`

Sets the default retry policy for every task on this worker. Individual tasks can override via their own `retry` field.

| Field | Type | Default | Description |
|---|---|---|---|
| `max_attempts` | int / null | `10` | Maximum attempts before giving up. `null` for infinite retries. |
| `initial_backoff` | duration | `1s` | Delay before first retry. Each subsequent retry doubles, with jitter. |

See [Retry](/docs/concepts/retry) for the two retry patterns (circuit breaker for processors, infinite reconnect for subscribers).

### `worker.event_buffer_size`

Each pair of connected tasks shares a bounded mpsc channel. `event_buffer_size` sets the channel capacity (in events). When the channel fills, the upstream task blocks until the downstream task drains — this is how backpressure propagates through the flow.

Tune it down for memory-constrained workers. Tune it up for high-throughput flows where producer/consumer rates fluctuate. The default (10M) is generous for most deployments.

## `telemetry`

OpenTelemetry export over OTLP/gRPC. See [Telemetry](/docs/concepts/telemetry).

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Set `true` to start the OTLP exporter. |
| `otlp_endpoint` | string | `http://localhost:4317` | OTLP/gRPC endpoint. |
| `service_name` | string | `flowgen` | `service.name` resource attribute. |
| `metrics_export_interval` | duration | `60s` | How often metric snapshots are pushed. |

When `telemetry` is omitted entirely, no OTLP exporter starts but tracing logs still go to stderr.

## Running

```bash
flowgen --config /etc/flowgen/config.yaml
```

The worker validates the config at startup. Any field with a typo, missing required value, or invalid type produces an error before any flow runs.
