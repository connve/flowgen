# Configuration

Flowgen reads a single YAML file (passed via `--config`). Top-level sections:

```yaml
flows:             # Flow discovery (required).
cache:             # Cache backend (optional, a local redb file by default).
resources:         # External resource loading (optional).
http_server:       # Webhook / metrics HTTP server (optional).
mcp_server:        # MCP server for tools/resources/prompts (optional).
ai_gateway:        # OpenAI-compatible LLM gateway (optional).
web:               # Web UI (optional).
health:            # k8s liveness/readiness listener (defaults on).
retry:             # Default retry policy for every task (optional).
telemetry:         # OpenTelemetry export (optional).
shutdown_timeout:  # How long a stopping flow lets its events finish (optional).
host:              # The address every server listens on (optional).
ai_budget:         # Token and amount limits of every ai_completion (optional).
credentials:       # Folder and age key of encrypted credentials (optional).
```

Only `flows` is required. Every other section has working defaults. A key no setting knows fails the start with an error naming it.

An `APP_` environment variable sets a key that takes a single value, with `__` between levels: `APP_AUTH__CLIENT_SECRET` sets `auth.client_secret`. Lists are set in the file. A variable without `__` that matches no top-level setting is ignored, since the platform may set `APP_` variables of its own; a key inside a section must match a setting.

## Minimal config

```yaml
flows:
  path: /etc/flowgen/flows/
```

Discovers every `.yaml`/`.yml`/`.json` file under the path and runs them with the cache in a local redb file at `.flowgen/state.redb`, no resources directory, no HTTP server, and no telemetry.

## Full reference

```yaml
flows:
  path: /etc/flowgen/flows/
  cache:
    enabled: true

cache:
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

auth:
  issuer_url: https://auth.example.com
  client_id: my-client-id
  credentials_path: /etc/flowgen/credentials/web.json
  redirect_uri: https://example.com/flowgen/auth/callback

health:
  enabled: true
  port: 8081

retry:
  max_attempts: 10
  initial_backoff: "1s"

shutdown_timeout: "20s"

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
| `cache` | object | | Cache-mode flow loading. Loads flows from the system bucket, under `flows.`, in addition to the filesystem. |
| `cache.enabled` | bool | required | Enable cache-mode flow loading. |
| `system` | bool | `false` | Runs this version's system flows under `system/`: the [lifecycle flows](/docs/flowgen/concepts/authoring#lifecycle-flows), Flowgen Agent and its tools, and the hubs. A flow at `path` with the same identity takes their place. A workspace started by `flowgen` without `--config` holds them in its repository instead. |
| `system_exclude` | list | | System flows left out, by identity, such as `system/release_workspace` when the repository's CI pushes the releases and flowgen only deploys them. An identity that is not a system flow fails the config. |

Both sources can be active at the same time. Use this for gradual migration: a small set of bootstrap flows mounted from disk (typically a sync flow that pulls user flows from Git into the cache), with the bulk of user flows loaded from the cache. On a name collision the filesystem entry wins, so a locally mounted bootstrap flow cannot be silently overridden by a stale cache entry.

Cache-mode flow loading is useful when flows are provisioned dynamically — for example, a control plane writes flow YAML into NATS KV and every flowgen replica picks them up.

## `cache`

Cache backend. When omitted, flowgen keeps the cache in a local redb file at `.flowgen/state.redb`.

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | string | `redb` | `nats` for a NATS server shared by every replica, `redb` for a local file used by a single process. |
| `path` | string | `.flowgen/state.redb` | The file of a `redb` cache. Created with its folder when missing. |
| `credentials_path` | string | optional | Path to NATS credentials file. |
| `url` | string | `localhost:4222` | NATS server URL. |
| `runtime.db_name` | string | `flowgen_cache` | KV bucket for flow state, reachable from scripts via `ctx.cache`. |
| `system.db_name` | string | `flowgen_system` | KV bucket for leader-election leases and peer registration. Kept apart from `runtime` so scripts cannot reach coordination state. |
| `history` | int | `64` | Historical entries retained per key. For NATS, only applies when the bucket is created; the server caps it at 64. |
| `tombstone_ttl` | duration | `1h` | TTL for delete/purge tombstones. Required for per-key TTLs on cache entries to work. |

If NATS is configured but flowgen cannot connect, it does not start. See [Caching](/docs/flowgen/concepts/caching).

A `redb` cache keeps flow state, leases and per-key TTLs across restarts without a NATS server. The `runtime` and `system` buckets are separate tables in the one file. Only one process can open the file, so use NATS when more than one replica runs.

```yaml
cache:
  path: /var/lib/flowgen/state.redb
```

## `resources`

External resource loading for SQL queries, prompts, scripts, schemas. See [Resources](/docs/flowgen/concepts/resources).

| Field | Type | Default | Description |
|---|---|---|---|
| `path` | string | | Filesystem base directory for resources. Resource keys resolve relative to this path. |
| `cache` | object | | Cache-backed resources. When `cache.enabled` is true, resources load from the system bucket, under `resources.`. |
| `cache.enabled` | bool | required | Enable cache-backed resources. |

When `resources` is omitted, only inline content is supported — any task that uses `{ resource: ... }` will fail at startup with a clear error.

## `http_server`

HTTP server that hosts every `http_endpoint` route plus health/metrics endpoints.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for `http_endpoint` tasks to start. |
| `port` | int | `3000` | Listening port. |
| `path` | string | | Optional path prefix applied to every registered route. |
| `credentials_path` | string | | Shared bearer/basic credentials. Tasks override per-route. See [Credentials](/docs/flowgen/concepts/credentials). |
| `auth` | object | top-level `auth` | User-level authentication provider (JWT, OIDC, session). See [Authentication](/docs/flowgen/concepts/auth). |

## `mcp_server`

MCP server for exposing `mcp_tool`, `mcp_resource`, and `mcp_prompt` tasks to LLM clients.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for any MCP task to register. |
| `port` | int | `3001` | Listening port. |
| `path` | string | `/mcp/v1` | MCP endpoint path. |
| `credentials_path` | string | | Shared API key credentials. Individual tools can override. |
| `auth` | object | top-level `auth` | User-level authentication provider (JWT, OIDC, session). With `oidc`, needs an `audience` and requires a key or token on every request. See [Sign-in](/docs/flowgen/ai/mcp#sign-in). |
| `resource_uri_scheme` | string | `flowgen` | Scheme used when auto-generating `mcp_resource` URIs of the form `<scheme>://<flow_name>/<name>`. White-label deployments override this. |
| `public_url` | string | the address the client used | The address MCP clients reach the server at through a proxy, e.g. `https://mcp.example.com`. |

## `ai_gateway`

OpenAI-compatible LLM gateway that serves every registered `llm_proxy` flow.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Required for `llm_proxy` tasks to be registered. |
| `port` | int | `3002` | Listening port, independent of the webhook HTTP server. |
| `path` | string | `/v1` | Path prefix for AI gateway routes. The chat completions endpoint is served at `<path>/chat/completions`. |
| `credentials_path` | string | | Path to global credentials file. Individual `llm_proxy` tasks can override. |
| `auth` | object | top-level `auth` | User-level authentication provider (JWT, OIDC, session). See [Authentication](/docs/flowgen/concepts/auth). |

## `web`

Embedded web dashboard and JSON API.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Set `true` to start the server. |
| `port` | int | `8080` | Listening port. |
| `path` | string | `/flowgen` | Path prefix for both the UI and the API. |
| `headers` | map of string to string | `{}` | HTTP headers sent with every request the web server makes to the AI gateway for the Agents tab, such as a key the gateway requires. |
| `agents.enabled` | bool | required | Shows the Agents tab and enables `/api/agents`. Omit `agents` to hide the tab. |
| `agents.session_history_ttl` | duration | `30d` | How long a session is kept after its last message; `0` keeps it indefinitely. |
| `agents.cache_prefix` | string | `agents` | Cache key prefix for sessions, read state and folders: segments of letters, digits, `-` and `_` joined by `.`. A flow opens a session under `<cache_prefix>.sessions.`. |
| `agents.reply_save_interval` | duration | `3s` | How often a reply in progress is saved; above 0. A pending reply not saved for ten times this long is shown as interrupted. |
| `agents.reply_idle_timeout` | duration | `10m` | How long a reply may wait for the AI gateway's next output before it fails; above 0. |
| `agents.on_behalf_of_user` | bool | `false` | Calls the AI gateway with the signed-in user's access token. Needs `auth`. |
| `ai_gateway_url` | string | same-process `ai_gateway` | Base URL of the AI gateway the Agents tab replies through, e.g. `http://127.0.0.1:3002/v1`. Set when the AI gateway runs in another process or pod. |
| `api_credentials_path` | string | | Machine keys the API accepts as `Authorization: Bearer` next to the login session. See [Authoring](/docs/flowgen/concepts/authoring#machine-keys). |
| `frame_ancestors` | list of string | `["'self'"]` | Who may show the web UI in a frame, as Content-Security-Policy `frame-ancestors` sources, e.g. `https://console.example.com`. |
| `update_check.enabled` | bool | `true` | Checks for a newer flowgen release and shows it next to the version in the web UI. |
| `update_check.url` | string | `https://api.github.com/repos/connve/flowgen/releases/latest` | Where the newest release is read from, as a GitHub releases API `latest` response. Point it at a mirror when the server has no internet access. |
| `update_check.interval` | duration | `1h` | How often to check. |
| `authoring.enabled` | bool | required | Enables change proposals. See [Authoring](/docs/flowgen/concepts/authoring). |
| `authoring.targets` | list | one `workspace` target over `flows/` and `resources/` | Where changes can be proposed. See [Targets](/docs/flowgen/concepts/authoring#targets). |
| `authoring.targets[].name` | string | required | Name shown on the target's changes. |
| `authoring.targets[].paths` | list of string | required | Workspace folders the target covers, e.g. `flows/user/`. |
| `authoring.targets[].commit_flow` | string | `system/commit_workspace` | Identity of the flow, starting with an [`inproc_endpoint`](/docs/flowgen/core/inproc), that commits the target's approved changes. |
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
| `GET <path>/api/agents/sessions/stream` | Server-Sent Events of the caller's sessions: the list on connect, then each change and removal. |
| `PATCH <path>/api/agents/sessions/{id}` | Rename a session, or move it to one of the caller's folders. |
| `PUT`, `DELETE <path>/api/agents/sessions/{id}/read` | Mark a session read or unread for the caller. |
| `POST <path>/api/agents/sessions/{id}/reply` | Add the user's message and have the agent reply. The reply runs on the server and is saved into the session as it arrives. `409` while another reply to the session is in progress. |
| `GET`, `DELETE <path>/api/agents/sessions/{id}/reply` | Follow the reply in progress as Server-Sent Events from the pod writing it, or stop it from any pod. |
| `POST <path>/api/workspace/validate` | Validates flow and resource files without running them. |
| `GET`, `POST <path>/api/changes` | List and propose workspace changes. |
| `GET <path>/api/changes/{id}` | One change with its diff. |
| `POST <path>/api/changes/{id}/approve`, `/reject` | Decide a pending change; a signed-in user only. |
| `GET <path>/api/openapi.yaml` | The spec itself. |

### Sessions from flows

With `auth`, each signed-in user sees their own sessions and the shared ones; without it, every session is shared.

A flow opens a session in the Agents tab by writing it to the system bucket (`cache.system.db_name`) with [`kv_store`](/docs/flowgen/kv-store/kv-store). Under `agents.sessions.<id>` it is shared. Under `agents.sessions.<user>.<id>`, where `<user>` is the hex SHA-256 of the user id, only that user sees it; a flow started by an `http_endpoint` or `mcp_tool` with `auth` has the id in `event.meta.auth.user_id`, so a script gets `<user>` with `sha256(event.meta.auth.user_id)`. The id uses only letters, digits, `-` and `_`. `event.data.content` is a `Session` from `openapi.yaml` as JSON, with `origin: agent` and `unread: true`, and optionally `folder`, e.g. `Reports/Weekly`. It is unread for each user until they open it, and again when it changes. Marking a session read or unread, or moving it to another folder, affects only the user who does it; without `auth` it affects everyone.

```yaml
- kv_store:
    name: open_session
    url: "{{env.NATS_URL}}"
    bucket: flowgen_system
    operation: put
    key: "agents.sessions.{{event.meta.correlation_id}}"
    ttl: 30d
```

## `auth`

The OpenID Connect provider people sign in with, used by every server without its own `auth`. See [Authentication](/docs/flowgen/concepts/auth).

| Field | Type | Default | Description |
|---|---|---|---|
| `issuer_url` | string | required | The provider's issuer; discovery is read from `{issuer_url}/.well-known/openid-configuration`. |
| `client_id` | string | required | The client registered with the provider. |
| `client_secret` | string | | Required unless `credentials_path` holds it. |
| `credentials_path` | string | | JSON file holding `client_secret` and, optionally, `cookie_secret`. |
| `redirect_uri` | string | required | Must match a redirect URI registered with the provider and include `web.path`, e.g. `https://example.com/flowgen/auth/callback`. |
| `extra_scopes` | list of string | `[]` | Requested next to `openid`, `profile` and `email`. |
| `signout_redirect_url` | string | | The provider's logout URL; flowgen appends `id_token_hint`. |
| `audience` | string | `client_id` | The `aud` the servers accept, e.g. `api://example`. |

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

## `shutdown_timeout`

How long a stopping flow lets its events in flight finish, `20s` by default; a flow can set its own. After it, the tasks get 5 more seconds to hand back what they hold. See [Stopping a flow](/docs/flowgen/concepts/flows#stopping-a-flow).

A pod stops its flows on shutdown, so Kubernetes' `terminationGracePeriodSeconds` must exceed the longest `shutdown_timeout` by at least 15 seconds. Flows called through `inproc_endpoint` stop after the others, so with such flows it must exceed twice the longest `shutdown_timeout` by 25 seconds. The Helm chart sets it to 65, which covers the default `shutdown_timeout` of 20 seconds either way.

## `host`

The address the web UI, HTTP server, MCP server, AI gateway, health server and cluster listener listen on, `0.0.0.0` by default. With `127.0.0.1` only the same machine reaches them, and the web UI, HTTP server, MCP server and AI gateway answer `403` to a request whose `Host` or `Origin` names another machine, which blocks DNS rebinding and cross-site requests. On Kubernetes keep the default: Services and probes connect to the pod's address.

## `ai_budget`

Limits on what every `ai_completion` of the workspace uses together, across all models, per calendar day, week or month in UTC:

```yaml
ai_budget:
  limits:
    - scope: workspace
      period: month
      max_amount: 500
    - scope: user
      period: day
      max_tokens: 200000
  prices:
    kimi-k2.7-code-highspeed: { input: 0.60, output: 2.50 }
    mistral-medium: { input: 0.40, output: 2.00 }
```

| Field | Type | Description |
|---|---|---|
| `limits[].scope` | string | `workspace` counts every flow together; `user` counts each signed-in user across flows. |
| `limits[].period` | string | `day`, `week` (ISO) or `month`, after which the count starts over. |
| `limits[].max_tokens` | int | Prompt and completion tokens per period. |
| `limits[].max_amount` | number | Amount per period, at `prices`, in their currency. |
| `prices` | map | Price per million prompt (`input`) and completion (`output`) tokens, by the model name a completion calls. |

Each limit sets exactly one of `max_tokens` and `max_amount`, and no two limits share a scope, a period and a kind. Under an amount limit, a completion of a model without a price fails. Each model call adds what the provider reports to the counts in the system cache, which flow scripts cannot change, also when the run it belongs to fails. Once a count reaches its limit, a completion fails before it calls the model, without a retry, and an AI gateway caller gets the error; a run with tools stops before its next model call. A completion without a signed-in user counts toward the workspace only.

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

Without `--config`, flowgen runs on a workspace folder: `flowgen <folder>`, or `FLOWGEN_HOME`, or `~/flowgen`. A new folder becomes a git repository with one commit holding the system flows in `flows/system/`, `.system-flows` and `.sops.yaml`; a folder inside a git repository becomes a workspace of that repository, so one repository can hold a folder per environment. Each run starts the [lifecycle flows](/docs/flowgen/concepts/authoring#lifecycle-flows) from the last commit, and every other flow loads from the cache in `.flowgen/state.redb`. The config is `host: 127.0.0.1` with the web UI, HTTP server, MCP server, AI gateway and telemetry on. `APP_` environment variables change it.

A run on a workspace whose system flows differ from the running version says so. `flowgen bootstrap [<folder>]` commits the system flows of the running version, and their resources in `resources/system/`, that are missing or differ, lists them in `.system-flows`, and deletes a file an earlier bootstrap listed that the running version lacks. A flow you added to `flows/system/`, such as a hub, is not on the list and stays.

## Testing a script

```bash
flowgen test resources/scripts/transform.rhai --data samples/
```

Runs a Rhai script on sample events the way the [`script`](/docs/flowgen/core/script) task runs it, with its limits, helpers and module imports, and `ctx.cache` in memory. `--data` is a file or a folder: a `.json`, `.yaml` or `.yml` file is one event and an `.ndjson` or `.jsonl` file is one event per line, each as `event.data`. Imports resolve in the nearest `resources/` folder above the script. It prints the events each sample emits, or the error it raised, and exits with `1` when a sample fails and `2` when the script or the samples cannot be read.

## Validating a workspace

```bash
flowgen validate path/to/workspace
```

Checks every flow under `flows/` and resource under `resources/` of the workspace folder (the current folder by default) without running anything or reading the app config: each flow must parse with known fields, its task names and `depends_on` must form a valid graph and its task settings must be valid, including MCP tool and prompt names; Rhai scripts must compile and JSON files must parse. Like the app, it follows symbolic links, skips hidden files and folders, and reads only `.yaml`, `.yml` and `.json` files under `flows/`. It prints one line per issue, `<file>: <location>: <message>`, or the issues as JSON with `--json`, and exits with `1` when it finds any, `2` when the folder cannot be read, and `0` otherwise, so a CI job or a coding agent can run it before a change is merged.
