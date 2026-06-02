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
    prefix: flowgen.flows
    db_name: flowgen_system

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

  ai_gateway:
    enabled: true
    port: 3002
    path: /v1
    credentials_path: /etc/flowgen/credentials/ai.json

  retry:
    max_attempts: 10
    initial_backoff: "1s"

  event_buffer_size: 10000

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

If NATS is configured but unreachable, flowgen falls back to in-memory automatically and logs a warning. See [Caching](/docs/flowgen/concepts/caching).

## `resources`

External resource loading for SQL queries, prompts, scripts, schemas. See [Resources](/docs/flowgen/concepts/resources).

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
| `http_server` | object | | HTTP server for `http_webhook` tasks. |
| `mcp_server` | object | | MCP server for `mcp_tool` tasks. |
| `ai_gateway` | object | | AI gateway server for any AI task that requires exposing a server endpoint. |
| `retry` | object | `{max_attempts: 10, initial_backoff: "1s"}` | Default retry config for every task. See [Retry](/docs/flowgen/concepts/retry). |
| `event_buffer_size` | int | `10000` | Capacity of each inter-task event channel (in events). When full the upstream task blocks until the downstream task drains a slot. |

### `worker.http_server`

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for `http_webhook` tasks to start. |
| `port` | int | `3000` | Listening port. |
| `path` | string | | Optional path prefix applied to every registered route. |
| `credentials_path` | string | | Worker-level shared bearer/basic credentials. Tasks override per-route. See [Credentials](/docs/flowgen/concepts/credentials). |
| `auth` | object | | User-level authentication provider (JWT, OIDC, session). See [Authentication](/docs/flowgen/concepts/auth). |

### `worker.ai_gateway`

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for `llm_proxy` tasks to be registered. |
| `port` | int | `3002` | Listening port, independent of the webhook HTTP server. |
| `path` | string | `/v1` | Path prefix for AI gateway routes. The chat completions endpoint is served at `<path>/chat/completions`. |
| `credentials_path` | string | | Path to global credentials file. Individual `llm_proxy` tasks can override. |
| `auth` | object | | User-level authentication provider (JWT, OIDC, session). See [Authentication](/docs/flowgen/concepts/auth). |

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

See [Retry](/docs/flowgen/concepts/retry) for the two retry patterns (circuit breaker for processors, infinite reconnect for subscribers).

### `worker.event_buffer_size`

Each pair of connected tasks shares a bounded mpsc channel. `event_buffer_size` sets the channel capacity (in events). When the channel fills, the upstream task blocks until the downstream task drains a slot — this is how backpressure propagates through the flow. No events are dropped.

The default (10,000) is sufficient for most workloads. The buffer only needs to absorb the gap between producer and consumer processing rates; downstream throughput is determined by task processing speed, not channel depth. Increase it if you observe producer stalls in flows with very bursty fan-out patterns and fast consumers.

## `telemetry`

OpenTelemetry export over OTLP/gRPC. See [Telemetry](/docs/flowgen/concepts/telemetry).

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
