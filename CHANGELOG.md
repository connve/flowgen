# Changelog

## 0.119.0

### Features

- **Config hot-reload.** Flowgen now watches the config directory for
  changes and automatically reconciles flows without restarting. Added
  flows start, removed flows stop, and modified flows are restarted.
  File system events are debounced to avoid churn during multi-file
  saves. HTTP server registrations (webhooks, MCP tools, AI gateway
  proxies) are deregistered per-flow before restart so stale entries
  never linger. The reconciler also calls
  `TaskManager::unregister(flow_name)` on the old flow before swapping
  in the replacement, which aborts that flow's lease renewal background
  task without touching the lease key in NATS. Without this step the
  old renewer survived the `Flow` drop in tokio and kept writing
  renewals to `lease.<flow_name>` under the same pod-level
  `holder_identity`, racing the replacement flow's renewer and
  producing endless `Lost lease ownership during renewal` warnings
  whose `current_holder` was the pod itself.

- **System cache separated from runtime cache.** Leader-election
  leases now live in the system bucket (typically `flowgen_system`)
  instead of the runtime bucket (`flowgen_cache`). The runtime bucket
  is what user scripts reach through `ctx.cache`; keeping leases out
  of it removes the risk of a script overwriting a coordination key,
  and lets the two buckets carry different retention policies (short
  history plus per-message TTL for leases, longer retention for
  per-flow state like replay IDs and counters). `FlowBuilder` now
  requires both `cache` and `system_cache`; in single-binary or
  in-memory deployments the same `Arc` is passed for both, so the
  separation is opt-in by configuration. Existing NATS deployments
  should drop the stale `lease.*` keys from the runtime bucket after
  upgrade; they are no longer renewed and will linger as dead
  entries until manually removed.

- **`Event::Display` drops `None` fields from the pretty output.**
  The pretty-printer used by the `log` task (and any `format!("{event}")`
  call) now omits `id`, `meta`, and `error` when they are `None`
  instead of emitting `"id": null`, `"meta": null`, `"error": null`.
  Industry-standard structured-log convention (`omitempty`, serde
  `skip_serializing_if = "Option::is_none"`, Pydantic
  `exclude_none=True`) — null fields are noise that bloats log
  storage and forces query parsers to handle `field == null` vs
  `field missing` as distinct cases. Always-present fields
  (`subject`, `data`, `timestamp`, `task_id`, `task_type`) keep
  appearing on every line.

- **`log` task drops `event.meta` from output by default.** Added
  `include_meta: bool` (default `false`) on the `log` task config.
  Meta is in-flow context for downstream tasks — stashed batches,
  correlation ids, per-event state — and tends to dwarf the data
  payload in any log line that prints it. With `include_meta: false`
  the log target sees only `subject`, `data`, `id`, `timestamp`,
  `task_id`, `task_type`, and `error`; meta still rides the
  pass-through event downstream unchanged. Operators that actually
  want meta in the log line opt in explicitly with `include_meta:
  true`. This unblocks observability for the system-sync bootstrap
  flows, which stash the full git batch (including file contents)
  in meta so `compute_diff` can read it after `nats_kv_store list`
  overwrites `event.data`.

- **Bootstrap sync flows self-heal on cache drift.** The
  `system_sync_flows` and `system_sync_resources` bootstrap flows
  in `local/flowgen.yaml` replaced their `skip_if_unchanged`
  short-circuit with `skip_if_in_sync`, which also checks that the
  cache key count matches the file count from the repo. Previously
  the bootstrap stamped `last_synced_commit` once it had written
  any keys and then short-circuited every subsequent tick on commit
  match alone — so any out-of-band deletion of a `flowgen.flows.*`
  or `flowgen.resources.*` key left the cache permanently missing
  that entry until an operator manually cleared `last_synced_commit`.
  The new check forces a full diff whenever the key count diverges,
  and the per-tick `log_actions` task makes every action
  (`put`/`delete`) visible at `info!` so a stuck pipeline is no
  longer silent.

- **`git_sync` emits include file path and short commit as log
  context.** The per-file `send_with_logging` call in
  `flowgen/git/src/sync/processor.rs` attaches `path=<file>` and
  `commit=<7-char hash>` via `EventLogger::context`, so every
  `event.subject=pull_repo` line names the file and revision
  instead of just the event id. Makes git-sync activity readable
  at a glance and lines up with the bootstrap's per-action log
  output for end-to-end traceability.

- **`EventExt::send_silent` for streaming hot paths.** Per-event logging
  via `send_with_logging` remains the default and stays at `info!` so
  operators see event flow without `RUST_LOG=debug`. Streaming processors
  whose intermediates would flood the log (the `ai_completion` per-token
  chunk path) now route through a new `send_silent` method, with the
  final chunk still going through `send_with_logging` so completion is
  observable. The MCP tool dispatch in `mcp/server.rs` also moves to
  `send_silent` because it already emits a richer `MCP tool invoked`
  info line of its own; previously it used a raw `tx.send` to dodge
  the double log. `grep -rn send_silent` lists every place we
  suppress event-flow visibility.

- **`#[allow(...)]` attributes banned workspace-wide.** Workspace
  `Cargo.toml` declares `[workspace.lints.clippy] allow_attributes =
  "deny"` and every crate opts in via `[lints] workspace = true`.
  Suppressing a lint now requires `#[expect(lint, reason = "...")]`,
  which turns into an error if the lint stops firing — dead
  suppressions surface automatically instead of rotting into the
  codebase. The one legitimate suppression
  (`flowgen/app/src/config.rs`'s `TaskType` enum, whose lowercase
  variants are YAML tags) has been migrated to `#[expect]` with a
  reason.

- **Generic HTTP server infrastructure.** Webhooks, MCP, and AI gateway
  now share a single `HttpServer<D>` type parameterised by a `Dispatcher`
  trait. Each role owns its URL layout and request handling; the server
  owns the listener, dispatch table, and hot-reload lifecycle. This
  eliminates duplicated server boilerplate across roles.

- **AI gateway (LLM proxy).** New `llm_proxy` task type exposes flows as
  OpenAI-compatible chat completion endpoints. Supports both OpenAI and
  Anthropic upstream protocols with automatic request/response
  translation. Multiple gateways can run on a single port, routed by the
  `model` field prefix.

- **OCI artifact sync.** New `oci_sync` task pulls flow definitions from
  an OCI registry (GHCR, ECR, GAR, Artifactory, Harbor) and emits one
  event per layer. Output shape mirrors `git_sync` so bootstrap
  pipelines can swap one for the other. Credentials auto-detect both
  the flowgen-native `{username, password}` shape and the standard
  Docker `config.json` (`kubernetes.io/dockerconfigjson`) payload — the
  same Secret used as the pod's `imagePullSecrets` can authenticate
  artifact pulls. Pure-Rust HTTPS stack via `oci-client`, no shell-out
  to `oras` or `docker`.

- **Versioned API paths.** Default MCP path changed from `/mcp` to
  `/mcp/v1` and default HTTP webhook path changed from
  `/api/flowgen/workers` to `/api/flowgen/workers/v1`, following the
  Google MCP convention. Paths remain configurable.

- **Force shutdown on second signal.** A second SIGTERM or SIGINT now
  forces an immediate exit instead of hanging. First signal initiates
  graceful shutdown, second signal calls `process::exit(1)`.

- **Git sync refactor.** Removed SSH auth support (`GitAuth`,
  `GitAuthType` enums) in favour of a simpler `credentials_path` field
  pointing to a JSON file with an HTTPS token. SSH URLs are rejected at
  startup with a clear error. The token is presented through a gix
  credential helper that responds to the server's `WWW-Authenticate`
  challenge — it never appears in the repository URL, in `.git/config`,
  or in logs. The credentials JSON now also accepts an optional
  `username` field (defaults to `x-access-token`) for hosts that
  require a specific literal username (GitLab OAuth tokens, Bitbucket
  Cloud token-auth).

### Improvements

- **Template rendering supports full object substitution.** A simple
  path template like `"{{event.data}}"` in a payload field now renders
  into the complete JSON object, preserving structure and types. This
  allows templates to replace `from_event: true` on HTTP and Salesforce
  payload configs. `from_event` still works but is now considered
  deprecated in favour of the template approach.

- **Per-event templating in `ai_completion`.** Credentials, endpoint, and
  model fields are now resolved at handle time against the incoming event
  instead of once at init. A single `ai_completion` task can route to
  multiple providers based on event data, with clients keyed and reused
  via the worker-wide `ClientRegistry`. Enables a consolidated LLM proxy
  flow (one task fans out to z.ai, Moonshot, OpenAI, etc.) instead of
  one flow per provider.

- **Leaf-task errors surface to source.** Added `signal_completion_with_error`
  to `CompletionState` so failed leaves deliver the underlying error to the
  source (HTTP webhook, MCP tool call, AI gateway request) instead of
  letting the source hang until `ack_timeout`. Wired across all 19
  leaf-capable processors. MCP tool callers and HTTP/AI-gateway clients
  now see the actual upstream error (Salesforce `MALFORMED_SEARCH`, LLM
  provider 4xx body, etc.) instead of an opaque timeout.

- **Fail-fast on upstream 4xx in Salesforce processors.** Adopts
  `salesforce_core 0.16.0`'s new `is_retryable()` helper on every
  `restapi`, `bulkapi`, `toolingapi`, `soapapi`, and `pubsubapi` error
  type. Permanent failures (bad input, auth, malformed query) now exit
  the retry loop immediately instead of being wrapped as transient. Same
  classification added to `ai_completion` for upstream LLM 4xx responses
  (text-matched against rig's error string until rig exposes a typed
  status). HTTP 429 is treated as permanent for LLMs since rate-limit
  recovery exceeds retry-loop timescales.

- **SOSL phrase content auto-escape.** `salesforce_restapi_search` now
  escapes SOSL reserved characters (`- ? & | ! { } [ ] ( ) ^ ~ * : \ " ' +`)
  inside `FIND {"..."}` phrase clauses before sending the query. Search
  terms with hyphens, ampersands, or other reserved characters work
  without manual escaping or `MALFORMED_SEARCH` errors. Already-escaped
  characters are passed through verbatim.

- **AI gateway error surfacing.** OpenAI-compatible streaming responses
  now emit `data: {"error": ...}` SSE frames when a leaf task fails;
  non-streaming responses return HTTP 502 with the underlying message
  instead of dropping the error and returning empty content.

- **`Source::Inline` now renders against event data.** Inline content
  passed through `Source::Inline::render()` previously assumed
  `config.render()` had already substituted templates at init time and
  returned the raw string. Processors that defer rendering to handle time
  (e.g. `ai_completion`) need the actual substitution. Inline templates
  without `{{...}}` syntax are unaffected (idempotent pass).

- **`ack_timeout` default documented.** The MCP `mcp_tool` task gained a
  fields table covering all configuration options. All five source-task
  docs (`generate`, `http/endpoint`, `nats/subscriber`, `salesforce/pubsub`,
  `ai/mcp`) now state the `ack_timeout` default as "wait indefinitely"
  with accurate per-source semantics.

- **SOSL prefix matching documented.** Added a "Prefix matching" section
  to `salesforce/rest` docs explaining how to append `*` inside the
  phrase to match prefixes. Example MCP/HTTP SOSL flows updated to use
  the wildcard form by default.

### Breaking changes

- **Lease cache keys preserve the flow name as written.** The task
  manager used to lowercase the flow name and convert underscores to
  hyphens when building the lease key (`flow.name`
  `salesforce_pubsubapi_account_subscriber` →
  `lease.salesforce-pubsubapi-account-subscriber`). The transform was
  a left-over from when the executor wrote to Kubernetes Lease
  objects, which require RFC 1123 DNS-safe names; the current NATS KV
  backend has no such constraint, and `validate_name` already keeps
  `flow.name` in a NATS-safe character set. The lease key is now
  built verbatim, so it matches what shows up in tracing fields,
  cache-prefix flow keys, and operator-facing logs. Existing
  deployments will see new lease keys appear after upgrade; the old
  `lease.<dashed>` entries linger as zombies until their TTL expires
  (60 s by default) and can be removed manually with
  `nats kv del flowgen_system lease.<old-name>` if desired.

- **`http_webhook` task renamed to `http_endpoint`.** The old name was
  misleading — the task handles any HTTP method (`GET`, `POST`, `PUT`,
  `DELETE`, `PATCH`), not only callback webhooks. The new name matches
  what the task actually does: it registers an endpoint on the worker's
  HTTP server. No alias is provided; existing flows must rename the task
  type. The docs route also moved from `/http/webhook` to `/http/endpoint`.

### Fixes

- **Dead dot-to-dash conversion in hot-reload key parsing removed.** The
  reconciler's cache-key-to-flow-name derivation rewrote every dot in
  the key suffix to a dash — a defensive transform left over from an
  earlier shape of the code. `validate_name` already rejects `.` in
  `flow.name` at config load, so the transform never had input to act
  on, but it would have silently broken round-trip if validation were
  ever loosened. The suffix is now returned verbatim.

- **Source ack hung in fan-out / fan-in DAGs.** When two branches re-merged
  at a buffer (or any other fan-in point), the source's completion channel
  was sized by summing children's leaf counts, which double-counted
  re-merged paths. The source then waited for more signals than there were
  real leaves, never received a successful ack, and never incremented its
  counter — a `generate` with `count: 1` fired every interval forever.
  Leaf counting now deduplicates by leaf index.

- **Buffer dropped acks when coalescing events from concurrent sources.**
  When the buffer accumulated events from distinct sources (e.g. concurrent
  webhook requests, parallel NATS deliveries), it kept only the last
  event's completion handle and silently dropped the rest, so N-1 sources
  per flush hung or saw their ack timeout fire. The buffer now collects
  every distinct upstream and fans the downstream ack back out to all of
  them. Events that share an upstream (single source fanned out and
  re-merged) still pass through directly with no proxy overhead.

- **`parse_json` in Rhai scripts now supports arrays.** The built-in
  Rhai `parse_json` only handles JSON objects at the root. The engine
  now registers a custom override that uses serde, supporting both
  arrays and objects. Fixes flows that load JSON array resources and
  pass them to `iterate`.

- **MCP Streamable HTTP spec compliance.** `notifications/initialized`
  now returns 202 Accepted instead of 200 OK. `ToolResult` serializes
  `isError` in camelCase per the MCP `CallToolResult` spec. SSE result
  events use the default format (`data:` only) instead of
  `event: message`, matching what standard MCP clients expect.

- **MCP and AI gateway servers start regardless of loaded tasks.** Servers
  now start when `enabled: true` in config, even if no matching tasks are
  loaded at startup. This supports hot-reload of flows from cache where
  tasks arrive after initial boot.

- **Improved config-not-found error message.** The error now includes the
  working directory and uses sentence case for clarity.

- **Unified NATS cache error messages.** All three NATS connection failure
  paths (system cache, flow cache, resource cache) now use a consistent
  message style and severity.

- **Suppressed noisy OpenTelemetry logs.** Default log filter now includes
  `opentelemetry=warn,opentelemetry_sdk=warn` to hide `MeterProvider`
  INFO messages.

- **Distinct server startup log messages.** Each server now logs its own
  identity ("Starting HTTP server", "Starting MCP server", "Starting AI
  gateway server") with port and path instead of all saying "Starting
  HTTP server".

- **Subscriber cancellation no longer hangs reconciler tear-down.** The
  salesforce pubsub, NATS jetstream, and `generate` (cron) subscribers
  spawned their reconnect loop into a detached `tokio::spawn` whose
  `JoinHandle` was dropped on the floor. When the hot-reload reconciler
  cancelled a flow's token, the running task was orphaned and kept
  hammering the upstream — the 30s deregistration timeout would always
  fire and stale subscribers would continue logging errors long after
  the flow's cache entry was deleted. `run()` now drives the loop
  directly and wraps every `await` (init, handle, backoff sleeps) in
  `tokio::select!` with the cancellation token so cancellation
  interrupts mid-backoff instead of waiting out the full delay.

- **Log message style consistency.** Stripped trailing periods from
  ~40 log messages across the codebase and converted internal
  `". Skipping."`-style sentence breaks into single-sentence-with-comma
  form, matching the convention "sentence (no period), structured
  fields after". `...` ellipses on shutdown messages are preserved.

### Docs

- **Inline vs external scripts.** Added guidance on when to keep Rhai
  inline in YAML versus moving it to a `resources/scripts/*.rhai` file,
  with the recommended project layout, a link to the official Rhai
  Visual Studio Code extension, and a new `script_fan_in_join.yaml`
  example showing a fan-in DAG joined by an external script. The example
  runs against the dummy CSV data shipped in the repository.

## 0.118.0

### Features

- **Shared connection pooling via ClientRegistry.** Tasks with identical
  credentials now automatically share the same authenticated client.
  The runtime hashes credential fields into a `ClientKey` and deduplicates
  at the worker level — no configuration needed. The first task to
  initialise performs the actual authentication; all others wait on a
  per-key mutex and reuse the result. Supported across Salesforce (REST,
  SOAP, Bulk, Tooling, PubSub), BigQuery (query, job, storage_read,
  storage_write), MSSQL, NATS (subscriber, publisher, kv_store), and
  Object Store (read, write, list, move).

### Dependencies

- **salesforce_core 0.15.0.** Fixes a bug where `reconnect()` replaced
  the `token_state` Arc instead of updating in-place, causing stale
  tokens for cloned clients. Shared Salesforce clients now see refreshed
  tokens immediately after reconnect.

## 0.117.0

### Features

- **Startup jitter for all background tasks.** Every background task now
  sleeps a random duration (up to `initial_backoff`) before its first
  initialization attempt. This staggers connections across flows and
  replicas to avoid thundering-herd login storms on external services.
  Blocking tasks (`http_webhook`, `mcp_tool`, `ai_gateway`) and
  `generate` tasks skip the jitter. The jitter is automatic and requires
  no configuration.

- **Exponential reconnect backoff for subscribers.** NATS JetStream and
  Salesforce PubSub subscribers now use exponential backoff (capped at
  5 minutes) when reconnecting after connection loss, replacing the
  previous flat `initial_backoff` sleep. Each successful reconnect resets
  the backoff.

- **AGENTS.md.** Committed agent instructions (code quality standards and
  architecture reference) to the repository root so they are shared
  across all AI coding tools.

## 0.116.0

### Features

- **Object store recursive listing.** New `recursive: true` option on
  `object_store` list operations traverses nested subdirectories instead of
  listing only the immediate prefix level. Enables `list → iterate → read`
  patterns on directory trees of unknown depth.

- **Pipeline tracing context.** `gcp_bigquery_query` now logs `num_rows`,
  `iterate` logs `array_length`, and `nats_jetstream_publisher` logs `subject`
  and `sequence` on each event — making it straightforward to trace row counts
  through a fan-out pipeline.

### Fixes

- **S3 virtual-hosted style requests by default.** S3 requests now use
  virtual-hosted style URLs (`{bucket}.s3.{region}.amazonaws.com`)
  matching the AWS SDK default. Previously path-style URLs were used,
  which can fail against bucket policies or VPC endpoint policies that
  expect virtual-hosted requests. Override with
  `aws_virtual_hosted_style_request: "false"` in `client_options`.

- **All processors now await spawned tasks on channel close.**
  Previously most processors used fire-and-forget `tokio::spawn` without
  collecting handles. All processors now collect `JoinHandle`s and await
  them when the input channel closes or cancellation is signalled,
  matching the pattern already used by `nats_jetstream_publisher`.

## 0.115.0

### Features

- **Salesforce SOAP API merge.** New `salesforce_soapapi_merge` task merges
  duplicate SObject records (Account, Contact, Lead, Individual) into a single
  master record via the Salesforce SOAP API. Related records are automatically
  reparented to the master. Supports field overrides and duplicate detection bypass.

### Fixes

- **BigQuery Storage Write: silent data loss on type mismatch.**
  `encode_scalar_value` now returns typed errors (`FieldTypeMismatch`,
  `FieldBase64Decode`) instead of silently skipping fields when JSON
  values don't match the expected BigQuery column type. Previously a
  string in an INT64 column was quietly dropped.

- **BigQuery Storage Write: nested RECORD/STRUCT support.**
  `build_proto_descriptor` and `json_to_proto_bytes` now recursively
  handle nested RECORD/STRUCT fields. Previously `nested_type: vec![]`
  was hardcoded, silently dropping all data in RECORD columns.

### Dependencies

- Upgraded `salesforce_core` from 0.13.6 to 0.14.0 (adds `soapapi::Client::merge()`).

## 0.114.0

### Features

- **`timestamp_format` Rhai function.** Format any Unix timestamp with a
  custom format string: `timestamp_format(timestamp_now(), "%Y-%m-%d %H:%M:%S")`.
  Uses [chrono strftime](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) syntax.

### Docs

- Added concrete output format tables to all 24 task documentation pages
  with links to underlying crate docs (arrow, serde_json, salesforce_core,
  async-nats, rhai, object_store, reqwest, tiberius).
- Added retry configuration links to all task configuration tables.
- Added `identify()` to PostHog integration so newsletter signups create
  persons instead of anonymous UUIDs.

### CI

- Fixed release notes shell interpolation bug — commit messages with
  backticks or special characters no longer break `gh release create`.
  Uses `--notes-file` instead of inline `--notes`.
- Added docs CI pipeline (`docs-ci.yml`) — runs `svelte-check` and build
  on PRs that touch `docs/`.

## 0.113.0

### Fixes

- **S3 credential chain now works out of the box.** The object store
  client for S3 now uses `AmazonS3Builder::from_env()`, which picks up
  IRSA (web identity token), EKS Pod Identity, ECS task roles, instance
  profiles, and environment variable credentials automatically. Previously
  `parse_url_opts` used `AmazonS3Builder::new()` which skipped the
  environment credential chain entirely, causing timeouts when IMDS was
  unreachable.

### Docs

- Fixed blank page on initial load caused by DOM manipulation breaking
  Svelte hydration. Tables use CSS-only overflow, copy buttons are
  appended without reparenting nodes.
- Fixed escaped underscores in installation table (`x86\_64`).

## 0.112.0

### Features

- **Application Default Credentials for GCP.** The `credentials_path`
  field is now optional on all BigQuery processors (`gcp_bigquery_query`,
  `gcp_bigquery_job`, `gcp_bigquery_storage_read`,
  `gcp_bigquery_storage_write`). When omitted, credentials are resolved
  via the Application Default Credentials chain:
  `GOOGLE_APPLICATION_CREDENTIALS_JSON` env, `GOOGLE_APPLICATION_CREDENTIALS`
  env, or `~/.config/gcloud/application_default_credentials.json`
  (written by `gcloud auth application-default login`).

## 0.111.0

### Features

- **Salesforce SOSL search task.** New `salesforce_restapi_search` task
  type for executing SOSL queries across multiple Salesforce objects.
  Supports Handlebars templating in the query string.

### Fixes

- **Default log level is now `info`.** When `RUST_LOG` is not set,
  flowgen defaults to `info` instead of `error`. New users no longer
  see a silent startup.
- Fixed "Loaded 1 flows" pluralization — now correctly says "Loaded 1 flow".

### Docs

- **Salesforce guides.** Added four end-to-end guides under Salesforce:
  CDC Replication, Data Export (Bulk API), Data Activation (Composite +
  Platform Events), and REST API (SObject, SOSL, MCP tools).
- Guides are nested under the Salesforce sidebar section with a
  subsection header.
- Added `protoc` as a build prerequisite in the installation docs.
- Mobile-friendly tables with horizontal scroll.
- Footer now matches connve.com (newsletter signup, GitHub, Helm icons).
- Mermaid diagram component for architecture visualizations in guides.

### Examples

- Reorganized Salesforce examples under `examples/salesforce/` with
  subdirectories per guide: `cdc-replication/`, `data-export/`,
  `data-activation/`, `rest-api/`.
- Removed old flat `examples/salesforce/` example files.

## 0.110.0

### Fixes

- macOS release binaries now cross-compile from a single `macos-latest`
  ARM runner instead of using the slow `macos-13` Intel runner.

## 0.109.0

### Fixes

- Fixed Docker `ENTRYPOINT` for distroless image — resolves binary
  extraction failure in release workflow and `docker run` without
  explicit command.

## 0.108.0

### Infrastructure

- **Native multi-arch Docker builds.** Split Docker build into parallel
  native runners (`ubuntu-latest` for amd64, `ubuntu-24.04-arm` for arm64)
  with digest-based merge into a single multi-arch manifest. Eliminates
  QEMU emulation overhead.
- **Pre-built binary extraction from Docker.** Linux release binaries are
  now extracted directly from the Docker image, guaranteeing the released
  binary is identical to what runs in containers.
- Fixed Helm chart install command in docs to use `helm.connve.com`.
- Docs: mobile hamburger menu with drawer navigation, matching connve.com
  navbar style.
- Added `ENTRYPOINT` to Dockerfile for distroless image compatibility.

## 0.107.0

### Features

- **Amazon S3 support for object store.** Read, write, list, and move
  operations now work with `s3://` paths. Credentials can be provided via
  a JSON file (`credentials_path`), inline `client_options`, or
  automatically from environment variables and IAM roles.

### Infrastructure

- **Pre-built binaries on GitHub Releases.** Each release now publishes
  binaries for Linux (amd64, arm64) and macOS (Intel, Apple Silicon).
  Linux binaries are extracted from the Docker image; macOS binaries
  are built natively.
- **Multi-arch Docker images.** Docker images now include both
  `linux/amd64` and `linux/arm64` platforms, built natively in parallel
  (no QEMU emulation) and merged into a single manifest.
- Replaced deprecated `actions/create-release@v1` with `gh release create`.

## Docs 0.2.0

- Connve branding: favicon, DaisyUI theme matching connve.com, full-width
  footer with company info and social links.
- Route structure flattened for `/docs/flowgen` base path, preparing for
  `connve.com/docs/flowgen/...` subpath hosting.
- Removed GitHub App token from docs CI workflow (uses default `GITHUB_TOKEN`).

## Chart 0.16.1

### Fixes

- WireGuard gateway: remove pod-level `sysctls` that cause
  `SysctlForbidden` on clusters with restricted pod security policies.
  The required sysctls (`net.ipv4.ip_forward`, `net.ipv4.conf.all.src_valid_mark`)
  are now set via the init container using `NET_ADMIN` capability instead.

## Chart 0.16.0

### Infrastructure

- **WireGuard gateway** replaces the WireGuard sidecar in the Helm
  chart. A standalone pod holds a single VPN identity and forwards
  ports via socat; flowgen pods connect through the gateway's
  ClusterIP Service. Fixes intermittent timeouts caused by multiple
  replicas sharing the same WireGuard private key and IP, which made
  the VPN server flap handshakes between pods. The `flowgen.wireguard`
  sidecar section is removed and replaced by the top-level
  `wireguardGateway` section with tunnel-based configuration.

## 0.106.0

### Infrastructure

- HTTP server `/healthz` endpoint for Kubernetes readiness and
  liveness probes. Pods that crash or haven't bound the listener
  are removed from the Service endpoints, preventing traffic from
  being routed to unhealthy replicas.

## 0.105.0

### Fixes

- `salesforce_bulkapi_query_job` no longer buffers all parsed Arrow
  RecordBatches in memory before emitting. Batches are now streamed
  one at a time via the centralized `FromReader` iterator, keeping
  peak memory to CSV bytes + one batch instead of CSV bytes + all
  batches. Fixes OOM kills on large Salesforce Bulk API exports
  (600k+ rows).

- `object_store` read now streams all multi-record formats (CSV,
  Parquet, Avro) one item at a time instead of collecting everything
  in memory before emitting. Uses the same `FromReader` iterator.

### Internal

- `FromReader` trait returns a lazy `ReaderIter` (boxed iterator)
  instead of `Vec`. Format-specific parsing (CSV, Parquet, Avro, JSON)
  stays centralized in `flowgen_core::buffer`; callers drive the
  iterator and can perform async sends between items. No call site
  collects all items in memory.

- `salesforce_bulkapi_query_job` completion handling extracted into
  `send_final_event` helper, removing five duplicated completion_tx
  wiring blocks across create, get, delete, abort, and get_results
  operations.

## 0.104.0

### Breaking

- **`event_buffer_size` default lowered from 10,000,000 to 10,000.**
  The previous 10M default dated from an earlier single-channel architecture
  and effectively disabled backpressure, allowing flows to consume
  unbounded memory under load. The new default caps each inter-task channel
  at 10,000 events; when full the upstream task blocks until the downstream
  task drains a slot. No events are dropped. Flows that previously relied
  on the large buffer as a shock absorber will now apply backpressure
  sooner, which bounds memory at the cost of brief producer stalls during
  bursts. Override per-worker via `worker.event_buffer_size` in the config
  file if your workload requires a larger buffer.

### Tasks

- `http_request` supports a new `response_type` field (`json` | `bytes` |
  `text`, default `json`). Set `response_type: bytes` to download binary
  payloads (archives, images, etc.) as `EventData::Bytes`; set
  `response_type: text` to capture plain-text responses as a JSON string.

- New `EventData::Bytes` variant for binary payloads. Tasks that need raw
  bytes pattern-match on the variant directly. The JSON projection (used
  by templating and logging) returns `{"$bytes": true, "len": N}` — the
  actual payload is never inlined into JSON.

- `object_store` write accepts `EventData::Bytes` input and writes raw
  bytes. Auto-detection maps bytes to `format: bytes` with a `.bin`
  extension; explicit `format: bytes` is also supported.

## 0.103.0

### Tasks

- `http_request` now transparently decompresses gzip-, brotli-, and
  deflate-encoded responses (`Content-Encoding: gzip`, `br`, `deflate`).
  The client sends `Accept-Encoding: gzip, br, deflate` on every outbound
  request; servers that do not compress ignore the header. Previously the
  client read raw compressed bytes as text and silently corrupted any
  response a server compressed. No config change.

## 0.102.0

### Breaking

- **Top-level config key `flow_resources` is `resources`** as of
  0.101.0. The field was first introduced as `flow_resources` (PR
  #146) and renamed to `resources` (PR #147) without a release
  note, which silently broke any tenant whose `config.yaml` still
  used the older name — resource-loading tasks (`bulkapi`, `mssql`,
  `gcp_bigquery_*`) fail with "Resource path is not configured".
  Tenants on `flow_resources:` must rename it to `resources:` in
  their inline config and any external config ConfigMaps. No
  backward-compatibility alias is provided.

### Tasks

- `iterate` now accepts `EventData::ArrowRecordBatch` and
  `EventData::Avro` inputs by converting them to a JSON array of
  row objects internally before walking the rows. Previously the
  task rejected non-JSON inputs with `ExpectedJsonGotArrowRecordBatch`
  / `ExpectedJsonGotAvro`, breaking ETL chains where an upstream
  task (BigQuery storage read, MSSQL query) emits Arrow and a
  downstream `iterate` walks the result set. The two old error
  variants are removed; conversion failures surface via the new
  `EventConversion` variant.

### Configuration

- `script::config::RhaiLimits` defaults raised: `max_map_size` and
  `max_array_size` go from `100_000` to `1_000_000`. The previous
  defaults flatten nested entries, so a script processing N rows ×
  ~20 fields hits the cap at N ≈ 5k — well within normal ETL batch
  sizes. Operators on tight memory budgets can override via the
  `limits` block on individual `script` tasks; the per-script
  override semantics are unchanged.

### Internal

- New unit test `test_rhai_string_suffix_strip_lowercase` covering
  the `split` + `ends_with` + `len` + `sub_string` + `to_lower`
  pattern used by Salesforce CDC routing scripts. Locks the
  contract so a Rhai dependency bump that changes string-op
  semantics fails fast in CI.

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
