# Configuration

Flowgen reads a single YAML file (passed via `--config`). Top-level sections:

```yaml
flows:             # Flow discovery (required).
cache:             # Distributed cache backend (optional).
resources:         # External resource loading (optional).
http_server:       # Webhook / metrics HTTP server (optional).
mcp_server:        # MCP server for tools/resources/prompts (optional).
ai_gateway:        # OpenAI-compatible LLM gateway (optional).
web:               # Web UI (optional).
health:            # k8s liveness/readiness listener (defaults on).
retry:             # Default retry policy for every task (optional).
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
    prefix: flows
    db_name: flowgen_system

cache:
  enabled: true
  type: nats
  credentials_path: /etc/nats/credentials.json
  url: "{{env.NATS_URL}}"
  runtime:
    db_name: flowgen_cache
  system:
    db_name: flowgen_system
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
  headers:
    X-Flowgen-Client: flowgen-ui

health:
  enabled: true
  port: 8081

retry:
  max_attempts: 10
  initial_backoff: "1s"

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
| `cache.prefix` | string | `flows` | Cache key prefix for flow entries. |
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
| `runtime.db_name` | string | `flowgen_cache` | KV bucket for flow state, reachable from scripts via `ctx.cache`. |
| `system.db_name` | string | `flowgen_system` | KV bucket for leader-election leases and peer registration. Kept apart from `runtime` so scripts cannot reach coordination state. Created whenever the cache is enabled. |
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
| `cache.prefix` | string | `resources` | Cache key prefix for resource entries. |
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

Embedded web dashboard and JSON API.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Set `true` to start the server. |
| `port` | int | `8080` | Listening port. |
| `path` | string | `/` | Path prefix for both the UI and the API. |
| `headers` | map of string to string | `{}` | HTTP headers sent with every request the web server makes to the AI gateway for the Agents tab. Set this so `llm_proxy`/`mcp_tool` tasks scoped with a matching `headers` field can identify and allow the web server as a caller — see [AI Gateway](/docs/flowgen/ai/gateway) and [MCP](/docs/flowgen/ai/mcp). |
| `agents.enabled` | bool | required | Shows the Agents tab and enables `/api/agents`. Omit `agents` to hide the tab. |
| `agents.conversation_history_ttl` | duration | `30d` | How long a conversation is kept after its last message; `0` keeps it indefinitely. |
| `agents.cache_prefix` | string | `agents` | Cache key prefix for conversations, read state and folders: segments of letters, digits, `-` and `_` joined by `.`. A flow opens a conversation under `<cache_prefix>.conversations.`. |
| `agents.reply_save_interval` | duration | `3s` | How often a reply in progress is saved; above 0. A pending reply not saved for ten times this long is shown as interrupted. |
| `agents.reply_idle_timeout` | duration | `10m` | How long a reply may wait for the AI gateway's next output before it fails; above 0. |
| `agents.on_behalf_of_user` | bool | `false` | Calls the AI gateway as the signed-in user, with their login token as the bearer. The gateway's `auth` must be `oidc` with the issuer of `web.auth` and its `client_id` as the audience. |
| `ai_gateway_url` | string | same-process `ai_gateway` | Base URL of the AI gateway the Agents tab replies through, e.g. `http://127.0.0.1:3002/v1`. Set when the AI gateway runs in another process or pod. |
| `api_credentials_path` | string | | Machine keys the API accepts as `Authorization: Bearer` next to the login session. See [Authoring](/docs/flowgen/concepts/authoring#machine-keys). |
| `frame_ancestors` | list of string | `["'self'"]` | Who may show the web UI in a frame, as Content-Security-Policy `frame-ancestors` sources, e.g. `https://console.example.com`. |
| `authoring.enabled` | bool | required | Enables change proposals. See [Authoring](/docs/flowgen/concepts/authoring). |
| `authoring.targets` | list | one `workspace` target over `flows/` and `resources/` | Where changes can be proposed. See [Targets](/docs/flowgen/concepts/authoring#targets). |
| `authoring.targets[].name` | string | required | Name shown on the target's changes. |
| `authoring.targets[].paths` | list of string | required | Workspace folders the target covers, e.g. `flows/user/`. |
| `authoring.targets[].publish_flow` | string | `system/publish_workspace` | Identity of the flow, starting with an [`inproc_endpoint`](/docs/flowgen/core/inproc), that publishes the target's approved changes. |
| `authoring.targets[].approver_groups` | list of string | `[]` | Identity provider groups whose members may approve and reject the target's changes. Empty allows every signed-in user. |
| `authoring.targets[].track_deployment` | bool | `true` | Reports when a committed change runs on this instance. Turn off when the target is not deployed here automatically. |
| `authoring.groups_claim` | string | `groups` | Claim of the signed-in user that lists their groups. |

The API contract is defined in `openapi.yaml` and served at `<path>/api/openapi.yaml`.

| Endpoint | Description |
|---|---|
| `GET <path>/api/flows` | List loaded flows with live counters. |
| `GET <path>/api/flows/{name}` | Full YAML source of one flow. |
| `GET <path>/api/flows/stream` | Server-Sent Events of live per-flow metrics: one `snapshot` frame with every flow's counters on connect, then a `snapshot` frame per change. Event/log history and live tail come from `/api/logs` and `/api/logs/stream` instead. |
| `GET <path>/api/logs` | Retained log records — framework, lifecycle, and per-task activity. Accepts `?limit=` (default 500, max 10000) and `?flow=` to scope to one flow's records. |
| `GET <path>/api/logs/stream` | Server-Sent Events live tail of log records as they arrive, same `?flow=` scoping as `/api/logs`. |
| `GET <path>/api/resources` | List discoverable resources. |
| `GET <path>/api/resources/{key}` | Fetch one resource's content. |
| `GET <path>/api/version` | Running build version. |
| `GET <path>/api/config` | Non-secret config info shown in the web UI (e.g. whether the Agents chat is configured). |
| `GET <path>/api/agents/models` | Proxies `GET /models` on the AI gateway for the built-in Agents chat's model picker. |
| `GET <path>/api/agents/tools` | Each `mcp_tool` with its flow and the connectors that flow reaches. |
| `GET <path>/api/agents/conversations/stream` | Server-Sent Events of the caller's conversations: the list on connect, then each change and removal. |
| `PATCH <path>/api/agents/conversations/{id}` | Rename a conversation, or move it to one of the caller's folders. |
| `PUT`, `DELETE <path>/api/agents/conversations/{id}/read` | Mark a conversation read or unread for the caller. |
| `POST <path>/api/agents/conversations/{id}/reply` | Add the user's message and have the agent reply. The reply runs on the server and is saved into the conversation as it arrives. `409` while another reply to the conversation is in progress. |
| `GET`, `DELETE <path>/api/agents/conversations/{id}/reply` | Follow the reply in progress as Server-Sent Events from the pod writing it, or stop it from any pod. |
| `POST <path>/api/workspace/validate` | Validates flow and resource files without running them. |
| `GET`, `POST <path>/api/changes` | List and propose workspace changes. |
| `GET <path>/api/changes/{id}` | One change with its diff. |
| `POST <path>/api/changes/{id}/approve`, `/reject` | Decide a pending change; a signed-in user only. |
| `GET <path>/api/openapi.yaml` | The spec itself. |

### Conversations from flows

With `web.auth`, each signed-in user sees their own conversations and the shared ones; without it, every conversation is shared.

A flow opens a conversation in the Agents tab by writing it to the system bucket (`cache.system.db_name`) with [`nats_kv_store`](/docs/flowgen/nats/kv-store). Under `agents.conversations.<id>` it is shared. Under `agents.conversations.<user>.<id>`, where `<user>` is the hex SHA-256 of the user id, only that user sees it; a flow started by an `http_endpoint` or `mcp_tool` with `auth` has the id in `event.meta.auth.user_id`, so a script gets `<user>` with `sha256(event.meta.auth.user_id)`. The id uses only letters, digits, `-` and `_`. `event.data.content` is a `Conversation` from `openapi.yaml` as JSON, with `origin: agent` and `unread: true`, and optionally `folder`, e.g. `Preflight/Klaviyo`. It is unread for each user until they open it, and again when it changes. Marking a conversation read or unread, or moving it to another folder, affects only the user who does it; without `web.auth` it affects everyone.

```yaml
- nats_kv_store:
    name: open_conversation
    url: "{{env.NATS_URL}}"
    bucket: flowgen_system
    operation: put
    key: "agents.conversations.{{event.meta.correlation_id}}"
    ttl: 30d
```

## `health`

Kubernetes liveness/readiness listener.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `true` | Enable the listener. |
| `port` | int | `8081` | Listening port. |

| Endpoint | Response |
|---|---|
| `GET /livez` | `200 OK`. |
| `GET /readyz` | `200 OK` once startup has finished, `503` before and after a shutdown signal. A worker with no flows is ready. |
| `GET /healthz` | Alias of `/livez`. |

## `retry`

Sets the default retry policy for every task. Individual tasks can override via their own `retry` field.

| Field | Type | Default | Description |
|---|---|---|---|
| `max_attempts` | int / null | `10` | Maximum attempts before giving up. `null` for infinite retries. |
| `initial_backoff` | duration | `1s` | Delay before first retry. Each subsequent retry doubles, with jitter. |

See [Retry](/docs/flowgen/concepts/retry) for how events, task startup, subscribers and flow restarts retry.

## `telemetry`

OpenTelemetry providers for metrics, traces, and logs. See [Telemetry](/docs/flowgen/concepts/telemetry).

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Set `true` to initialize the provider. |
| `backend` | object | in-memory | Backend selection. Omit for the in-memory backend. |
| `backend.type` | string | — | `memory` or `remote`. |
| `backend.endpoint` | string | — | Required for `remote`. gRPC endpoint of the collector. |
| `backend.logs_per_flow` | usize | `1000` | Memory backend only. Log records retained per flow and level on each pod. |
| `backend.metrics_per_flow` | usize | `1000` | Memory backend only. Metric samples retained per flow. |
| `backend.port` | int | `8082` | Memory backend only. Port of the internal endpoint the other pods read logs and flow counters from in cluster mode. |
| `service_name` | string | `flowgen` | `service.name` resource attribute. |
| `metrics_export_interval` | duration | `60s` | How often metric snapshots are pushed. Ignored by the memory backend. |

When `telemetry` is omitted entirely, no telemetry stack is started.

## Running

```bash
flowgen --config /etc/flowgen/config.yaml
```

Flowgen validates the config at startup. Any field with a typo, missing required value, or invalid type produces an error before any flow runs.
