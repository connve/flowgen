# Changelog

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
