# Configuration

Flowgen reads a single YAML file (passed via `--config`). Top-level sections:

```yaml
flows:             # Flow discovery (required).
cache:             # Distributed cache backend (optional).
resources:         # External resource loading (optional).
http_server:       # Webhook / metrics HTTP server (optional).
mcp_server:        # MCP server for tools/resources/prompts (optional).
ai_gateway:        # OpenAI-compatible LLM gateway (optional).
web:               # Admin web UI (optional).
health:            # k8s liveness/readiness listener (defaults on).
retry:             # Default retry policy for every task (optional).
event_buffer_size: # Per-edge channel capacity (optional).
telemetry:         # OpenTelemetry export (optional).
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
  url: "{{env.NATS_URL}}"
  db_name: flowgen_cache
  history: 10
  tombstone_ttl: "1h"

resources:
  path: /etc/flowgen/resources/

http_server:
  enabled: true
  port: 3000
  path: /api/flowgen/workers/v1
  credentials_path: /etc/flowgen/credentials/http.json
  auth:
    type: jwt
    secret: "shared-secret"

mcp_server:
  enabled: true
  port: 3001
  path: /mcp/v1
  credentials_path: /etc/flowgen/credentials/mcp.json

ai_gateway:
  enabled: true
  port: 3002
  path: /v1
  credentials_path: /etc/flowgen/credentials/ai.json

web:
  enabled: true
  port: 8080
  path: /flowgen

health:
  enabled: true
  port: 8081

retry:
  max_attempts: 10
  initial_backoff: "1s"

event_buffer_size: 10000

telemetry:
  enabled: true
  backend:
    type: remote
    endpoint: http://otel-collector:4317
  service_name: flowgen
  metrics_export_interval: "60s"
```

## `flows`

Flow discovery. Filesystem and distributed cache can be used independently or together; flowgen merges flows from both sources at startup.

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
| `credentials_path` | string | optional | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `db_name` | string | `flowgen_cache` | KV bucket name. |
| `history` | int | `64` | Historical entries retained per key. Only applies when the bucket is created. Server caps at 64. |
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

## `http_server`

HTTP server that hosts every `http_endpoint` route plus health/metrics endpoints.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for `http_endpoint` tasks to start. |
| `port` | int | `3000` | Listening port. |
| `path` | string | | Optional path prefix applied to every registered route. |
| `credentials_path` | string | | Shared bearer/basic credentials. Tasks override per-route. See [Credentials](/docs/flowgen/concepts/credentials). |
| `auth` | object | | User-level authentication provider (JWT, OIDC, session). See [Authentication](/docs/flowgen/concepts/auth). |

## `mcp_server`

MCP server for exposing `mcp_tool`, `mcp_resource`, and `mcp_prompt` tasks to LLM clients.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for any MCP task to register. |
| `port` | int | `3001` | Listening port. |
| `path` | string | `/mcp/v1` | MCP endpoint path. |
| `credentials_path` | string | | Shared API key credentials. Individual tools can override. |
| `auth` | object | | User-level authentication provider (JWT, OIDC, session). See [Authentication](/docs/flowgen/concepts/auth). |
| `resource_uri_scheme` | string | `flowgen` | Scheme used when auto-generating `mcp_resource` URIs of the form `<scheme>://<flow_name>/<name>`. White-label deployments override this. |

## `ai_gateway`

OpenAI-compatible LLM gateway that serves every registered `llm_proxy` flow.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for `llm_proxy` tasks to be registered. |
| `port` | int | `3002` | Listening port, independent of the webhook HTTP server. |
| `path` | string | `/v1` | Path prefix for AI gateway routes. The chat completions endpoint is served at `<path>/chat/completions`. |
| `credentials_path` | string | | Path to global credentials file. Individual `llm_proxy` tasks can override. |
| `auth` | object | | User-level authentication provider (JWT, OIDC, session). See [Authentication](/docs/flowgen/concepts/auth). |

## `web`

Embedded admin dashboard for inspecting loaded flows and resources at runtime.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Set `true` to start the admin server. |
| `port` | int | `8080` | Listening port. |
| `path` | string | `/` | Path prefix. Use a subpath (e.g. `/flowgen`) when co-hosting under a tenant subdomain like `tenant.example.com/flowgen`. |

The UI lists loaded flows with description, tags, status, and last-run timestamp; click a row to preview the raw source YAML with syntax highlighting. A Resources tab browses everything under the configured `resources.path` and renders `.md` files as HTML plus code files (SQL, YAML, JSON, Rhai, JS/TS, Bash, Python) with Prism.

## `retry`

Sets the default retry policy for every task. Individual tasks can override via their own `retry` field.

| Field | Type | Default | Description |
|---|---|---|---|
| `max_attempts` | int / null | `10` | Maximum attempts before giving up. `null` for infinite retries. |
| `initial_backoff` | duration | `1s` | Delay before first retry. Each subsequent retry doubles, with jitter. |

See [Retry](/docs/flowgen/concepts/retry) for the two retry patterns (circuit breaker for processors, infinite reconnect for subscribers).

## `event_buffer_size`

Each pair of connected tasks shares a bounded mpsc channel. `event_buffer_size` sets the channel capacity (in events). When the channel fills, the upstream task blocks until the downstream task drains a slot — this is how backpressure propagates through the flow. No events are dropped.

The default (10,000) is sufficient for most workloads. The buffer only needs to absorb the gap between producer and consumer processing rates; downstream throughput is determined by task processing speed, not channel depth. Increase it if you observe producer stalls in flows with very bursty fan-out patterns and fast consumers.

## `telemetry`

OpenTelemetry providers for metrics, traces, and logs. See [Telemetry](/docs/flowgen/concepts/telemetry).

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Set `true` to initialize the provider. |
| `backend` | object | in-memory | Backend selection. Omit for the in-memory backend. |
| `backend.type` | string | — | Either `memory` or `remote`. |
| `backend.endpoint` | string | — | Required for `remote`. gRPC endpoint of the collector. |
| `backend.logs_per_flow` | usize | `1000` | Memory backend only. Log records retained per flow. |
| `backend.metrics_per_flow` | usize | `1000` | Memory backend only. Metric samples retained per flow. |
| `service_name` | string | `flowgen` | `service.name` resource attribute. |
| `metrics_export_interval` | duration | `60s` | How often metric snapshots are pushed. Ignored by the memory backend. |

When `telemetry` is omitted entirely, no telemetry stack is started.

## Running

```bash
flowgen --config /etc/flowgen/config.yaml
```

Flowgen validates the config at startup. Any field with a typo, missing required value, or invalid type produces an error before any flow runs.
