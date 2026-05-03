# Changelog

## 0.102.0

### Highlights

- **Hot-reload for cache-loaded flows.** A new watcher subscribes to flow
  key changes in the system cache (NATS KV) and a reconciler debounces
  rapid bursts (2 s window, 10 s ceiling) before reconciling the running
  flow registry: starting new flows, stopping removed ones, and swapping
  changed flows without interrupting unaffected flows. Replacement flows
  are fully built and ready before the old flow is cancelled.
- **Generic HTTP server** in `flowgen_core::http_server`. One reusable
  `HttpServer<D>` parameterised by a `Dispatcher` implementation, used by
  three peer roles: webhooks, MCP, and AI gateway. The axum router is
  built once at startup and never rebuilt; hot-reload is `DashMap::insert
  / retain` on a per-server dispatch table, with bulk
  `deregister_flow(flow_name)` driven by a `HasFlowName` supertrait on
  every registration type.
- **AI gateway server** for OpenAI-compatible chat completions. Runs on
  its own port (default 3002, path `/v1`) and exposes `POST
  /v1/chat/completions` and `GET /v1/models`. Per-flow routing is driven
  by the request body's `model` field (`<task-name>/<downstream-model>`),
  matching the convention used by vLLM, Ollama, LiteLLM, and OpenRouter.
- **Three independent listeners** (webhook on 3000, MCP on 3001, AI
  gateway on 3002), each with its own port, path, credentials path, and
  auth provider. The Helm chart now exposes a third Service for the AI
  gateway.

### Tasks

- New `llm_proxy` task type — registers a flow as a backend on the AI
  gateway server. Replaces the previous `ai_gateway` task type. Carries a
  `protocol: openai` field (default; one variant today, leaves room for
  Anthropic-shaped or other protocols).
- `mcp_tool`: `ToolRegistration` now carries `flow_name` so the MCP
  server can bulk-deregister every tool owned by a stopped flow.
- `http_webhook`: `WebhookRegistration` carries `flow_name` for the same
  bulk-deregister path; the webhook dispatcher now lives in
  `flowgen_http::webhook::dispatch` as free functions taking
  `&WebhookRegistration` and is mounted under one static catch-all route.

### Configuration

- New `worker.ai_gateway` block (`enabled`, `port`, `path`,
  `credentials_path`, `auth`). Defaults: port 3002, path `/v1`.
- New `worker.mcp_server.auth` field — MCP no longer borrows the webhook
  server's auth provider.
- `worker.http_server.path` and `worker.http_server.port` normalised from
  `Option<...>` to required-with-serde-default, matching the shape of
  `worker.mcp_server` and `worker.ai_gateway`. Defaults: port 3000, path
  `/api/flowgen/workers`.
- `llm_proxy` task config: dropped per-flow `path` field. The URL is
  determined by the chosen `protocol` rooted under `ai_gateway.path`.

### Internal

- `flowgen_core::http_server::HttpServer<D>` is now generic over a
  `Dispatcher` whose associated types are a `Registration` (must impl
  `HasFlowName`) and `Extras` (role-specific shared state, `()` for
  webhook). Webhook, MCP, and AI gateway each own a dispatcher impl in
  their crate.
- `flowgen_core::http_server::try_register` returns the rejected
  registration on key collision so callers can surface a hard error
  instead of silently overwriting (used by MCP for tool name
  uniqueness).
- `flowgen_core::cache::Cache::watch(prefix)` extension method returns
  a stream of `WatchEvent` (`Put`/`Delete`). NATS KV implementation
  provided; backends that do not support watching return
  `WatchNotSupported`.
- `flowgen_core::task::context::TaskContext` no longer carries
  `http_server`, `mcp_server`, `registered_http_routes`, or
  `registered_mcp_tools`. Server access is now per-processor via the
  relevant `ProcessorBuilder` (`http_server`, `mcp_server`,
  `ai_gateway_server`); deregistration is bulk-by-flow-name on each
  server, not per-route tracking lists.
- `flowgen_core::http_server` and the deleted
  `flowgen_core::mcp_server` marker trait modules. The old
  `register_route(path, Box<dyn Any + Send>)` indirection and runtime
  downcast are gone.
- New `flow::Error::LlmProxy` (renamed from `AiGateway`) and
  `flow::Error::AiGatewayServerNotEnabled`.
- New `app::Error::AiGatewayServerStart`. Removed `HttpServerDowncast`,
  `McpServerDowncast` (no longer reachable).
- `FlowHandle` shrunk to `{cancellation_token, join_handle,
  from_filesystem}`.
- `flowgen_app::reconciler` and `flowgen_app::watcher` are new modules
  driving the hot-reload loop.
- Removed the dead `flowgen_core::mcp::registry` re-export module.
  Consumers should import directly from `flowgen_core::registry`.

### Documentation

- `config.example.yaml` documents `worker.ai_gateway`, the optional
  `worker.mcp_server.auth` field, and the new normalised shape for
  `worker.http_server`.
- AI gateway docs page rewritten to describe `llm_proxy` task
  registration, `model`-field routing (`<task-name>/<downstream-model>`),
  and the `GET /v1/models` discovery endpoint.

### Helm

- Chart bumped to 0.15.0. New optional `ai_gateway` Service block in
  `values.yaml` and a third `containerPort` / `Service` template for the
  AI gateway port. Existing `http_server` and `mcp_server` Service blocks
  unchanged.

### Dependencies

- `flowgen_core` gains `axum` (for the generic `HttpServer<D>`).
- `flowgen_http` gains `bytes` and `tokio-util`.
- `flowgen_mcp` gains a path dependency on `flowgen_http`.

## 0.101.0

### Highlights

- New `git_sync` task: clones a Git repository and emits one event per
  file. Pure-Rust via `gix`; no `git` binary required at runtime.
- New `nats_kv_store` task: `get`, `put`, `list`, `delete` against
  NATS JetStream KV. Bucket auto-created on first use.
- System cache for flow loading: bootstrap a `system_sync_flows` flow
  on disk, let it pull user flows from a Git repo into NATS KV, and
  the worker loads them from the cache on the next start.
- New `validate.rs` in `flowgen_core` with shared name and path
  validators. Flow loaders reject names containing `/`, `\`, `..`,
  empty strings, or non-`[A-Za-z0-9_-]` characters before any flow
  is accepted.

### Security

- **Webhook body limit**: `http_webhook` now enforces
  `max_body_bytes` (default 10 MiB). Oversized requests are rejected
  with HTTP 413 before being read into memory.
- **Outbound HTTP timeouts**: `http_request` now applies `timeout`
  (default 30s) and `connect_timeout` (default 10s) on the reqwest
  client to prevent slow upstreams from pinning worker tasks.
- **Rhai resource limits**: `script` config exposes `limits` with
  `max_operations` (10M), `max_call_depth` (64), `max_string_size`
  (16 MiB), `max_array_size` and `max_map_size` (100k). Caps a
  single misbehaving script from stalling the worker.
- **Rhai env-var isolation**: split `render_template` into the
  env-injecting variant (operator-controlled YAML) and
  `render_template_no_env`. The Rhai-exposed `render(template,
  data)` now uses the no-env variant so flow-author scripts cannot
  read pod environment variables.
- **`clone_path` traversal guard**: operator-supplied `clone_path`
  values containing `..` segments are rejected at task init.

### Tasks

- `http_request`: error chain walking surfaces the underlying cause
  for opaque reqwest builder errors (e.g. URL parse, header value).
- `http_request`: `query_params` payload mode wired up for sending
  pure query parameters.
- `iterate`: `depends_on` semantics revamped; multi-leaf
  acknowledgement via shared `CompletionState`.
- `script`: `render(template, data)` Rhai helper for Handlebars
  rendering against script data (env access excluded — see Security).
- `git_sync`: derived `clone_path` defaults to
  `<temp>/<flow_name>/<task_name>` so multiple `git_sync` tasks in
  one worker do not collide. Override via `clone_path`.

### Configuration

- New typed validation errors in `flowgen_core::validate` (`Error`,
  `NameField`, `PathField`).
- `script::config::RhaiLimits` struct with serde defaults; safe to
  omit, fully overridable per task.
- `http::config::Processor` gains `timeout`, `connect_timeout`, and
  `max_body_bytes` fields, all with defaults.

### Documentation

- New docs pages introducing flowgen, the system cache loading
  story, and `nats_kv_store`.
- Landing page (`/`) redirects to `/docs/getting-started/why-flowgen`.

### Internal

- `flowgen_core::validate` module shared across crates.
- gix wired with `sha1` and `max-performance-safe` features.
- `reqwest` workspace deps include `query`, `gzip`, `brotli`,
  `charset`, `json`, `form`, `rustls`.

### Dependencies

- `salesforce_core` 0.13.4 → 0.13.6. Brings docs.rs build fixes
  and per-API cargo features upstream; we keep all four API modules
  (`restapi`, `bulkapi`, `toolingapi`, `pubsubapi`) enabled.
