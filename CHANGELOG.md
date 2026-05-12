# Changelog

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
