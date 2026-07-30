# Contributing to flowgen

This document covers the conventions we follow across the Rust workspace and
the architectural patterns that keep tasks (processors, publishers,
subscribers) consistent with each other. Read it before your first PR; skim
it again whenever you're adding a new task type or touching error handling.

## Code quality

### Error handling

- No `unwrap()`, `expect()`, or `panic!()` in production code. If a failure
  is reachable at runtime, it needs a typed error, not an assumption that it
  can't happen.
- Use an `Error` enum (via `thiserror`) with one variant per distinct
  failure case, each with a descriptive `#[error("...")]` message and
  `#[source]` where there's an underlying error to chain.
- Propagate with `?`, convert with `map_err()`, and handle `Result`/`Option`
  explicitly rather than reaching for a combinator that swallows the error.

### Comments and documentation

- Comments should earn their place by explaining the *why* — a hidden
  constraint, a workaround, a non-obvious tradeoff — not restate the *what*
  the code already says through good naming.
- Write comments as complete sentences, avoid acronyms, and don't leave them
  stale after a functionality change.
- Document module intros, public-API methods, struct fields, and constants.
  Don't feel obliged to comment every private function, especially when its
  name already makes the intent clear.
- Don't add comments inside unit tests — the test name and assertions
  should carry the explanation.
- Every task type needs a working example under `examples/`, demonstrating
  a realistic use case with a short comment per task, not teaching prose.
- Only link to URLs that already exist in the repo or were given to you
  directly — don't guess at a plausible-looking path.
  - Docs site: `https://connve.com/docs/flowgen/<route>`, mirroring
    `docs/src/routes/` (e.g. `core/script`, `concepts/resources`).
  - GitHub: `https://github.com/connve/flowgen/blob/main/<repo-relative-path>`
    for files on `main`.

### Code structure

- Default derive set where it fits:
  `#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]`.
- Pull duplicated logic into a helper rather than copy-pasting it a third
  time; follow the patterns already established elsewhere in the codebase
  before inventing a new one.
- Prefer `match` over `if/else` chains and over `Option`/`Result`
  combinator chains (`unwrap_or`, `unwrap_or_else`,
  `.map(...).unwrap_or(...)`). Slice patterns and destructuring
  (`match v.as_slice() { [] => ..., [x] => ..., _ => ... }`) read clearer
  than cascading `else if`. A single boolean check is fine as `if/else`.
- Clippy wins when it disagrees with the `match` preference above — some
  lints (`manual_unwrap_or_default`, for instance) actively want the
  combinator form. Follow clippy rather than adding `#[allow(...)]` to keep
  the `match` style; `cargo clippy -- -D warnings` is the hard constraint,
  the style preference is soft.

### Testing

Two tiers, picked by what the code under test touches:

- **Unit tests**: `#[cfg(test)] mod tests` inline in the file, using
  in-memory fakes (`MemoryCache`, etc.). Default choice — covers logic
  within or across a few modules of the same crate without needing a real
  backend.
- **Integration tests**: `<crate>/tests/<name>_integration.rs`, cargo's
  built-in convention. Reach for this when the behavior only shows up
  against something unit tests can't fake convincingly — a real network
  listener, an external CLI, or a real backend's actual protocol quirks
  (e.g. NATS per-message TTL support, which an in-memory cache can't
  reproduce).
  - If the test needs an external service, spin it up with
    `testcontainers` and mark the test `#[ignore]` so default `cargo test`
    stays fast; CI's `test-integration` job runs the ignored set
    separately (`cargo test --workspace --tests -- --ignored`).
  - If it doesn't (spawns an in-process server, shells out to a CLI
    already in `PATH`), leave it un-ignored — it runs in the normal job
    alongside unit tests.
  - Every integration test file opens with a `//!` module doc stating what
    it covers, what it depends on (Docker, git, nothing), and why it's
    `#[ignore]`d or not.

Reference implementations:
- Real backend via testcontainers (`#[ignore]`d): `flowgen/app/tests/cache_integration.rs`
- No external dependency (not `#[ignore]`d): `flowgen/app/tests/health_integration.rs`,
  `flowgen/git/tests/sync_integration.rs`

### Dependencies

Declare all package dependencies at the workspace root (`Cargo.toml`).
Crate-level `Cargo.toml` files reference them with `workspace = true` and
never pin their own version:

```toml
tokio = { workspace = true, features = ["full"] }
```

### Standard libraries

- Reach for an established crate over hand-rolling the functionality.
- Date/time: always `chrono` — parsing, conversions, timezones. Don't
  hand-compute a duration or timestamp.
- Errors: always `thiserror` — `#[derive(thiserror::Error)]`,
  `#[error("...")]`, `#[source]` for chaining.

### Configuration

- Document every config field with its purpose and an example.
- Use `#[serde(default)]` for optional fields with a sensible default.
- Duration fields take human-readable strings (`"30s"`, `"5m"`, `"1h"`) via
  `humantime_serde`: `#[serde(default, with = "humantime_serde")]` for
  `Option<Duration>`.
- Validate at parse time where you can, with an error message that points
  at the actual misconfiguration.
- Any change to `AppConfig` needs a matching update in
  `config.example.yaml`, with a comment explaining the option.

### API responses

Return the response types the client library already gives you
(`QueryResponse`, `GetQueryResultsResponse`, etc.) rather than wrapping them
in a custom type. Only convert when the event pipeline needs a different
shape — e.g. BigQuery's `QueryResponse` becomes an Arrow `RecordBatch` for
`EventData`, but the query call itself uses the library's native response.

## Architecture reference

### Retry strategy

There are two retry patterns depending on what the task is:

**Subscribers** (Salesforce PubSub, NATS JetStream, Kafka, ...) retry
forever — they're long-lived infrastructure and giving up isn't an option.
Use a circuit breaker only to detect permanent errors (bad credentials,
broken config) during initialization; once initialized, reconnect
automatically on any event-loop failure.

**Everything else** (publishers, processors, webhooks, HTTP requests) uses
the circuit-breaker pattern with `max_attempts` (default 10) and fails fast
once exhausted. A publisher that fails init just leaves the message unacked
for a later retry; a webhook init failure usually means a permanent config
error.

Rules that apply to both:
- Never hand-roll exponential backoff — use the global `RetryConfig`,
  applied automatically at the handler level in `run()`.
- Polling for async job completion isn't a retry scenario; use a plain
  fixed interval instead.
- Durations are human-readable strings (`"1s"`, `"500ms"`), never raw
  milliseconds.

Defaults: `max_attempts: 10`, `initial_backoff: "1s"`, backoff sequence
with ±50% jitter (~1s, 2s, 4s, 8s, ... ~512s), circuit breaker trips after
roughly 15 minutes total.

```yaml
retry:
  max_attempts: 10       # default: 10 attempts (~15 min), circuit breaker for most tasks
  initial_backoff: "2s"  # default: "1s", first retry waits ~2s, grows exponentially
```

Failures log immediately with full context (flow, task, task_id,
task_type); expect one log line per failed attempt during a transient
outage — that's normal, not a sign of a bug.

Reference implementations:
- Subscriber (infinite retry): `flowgen/salesforce/src/pubsubapi/subscriber.rs`,
  `flowgen/nats/src/jetstream/subscriber.rs`
- Publisher (circuit breaker): `flowgen/salesforce/src/pubsubapi/publisher.rs`
- HTTP endpoint (circuit breaker): `flowgen/http/src/endpoint.rs`

### EventData types

Pick the format that matches the data source's native shape:

- **`EventData::ArrowRecordBatch`** — columnar storage (BigQuery, Parquet,
  Arrow files). Zero-copy, efficient, native to the source.
- **`EventData::Avro`** — gRPC and Pub/Sub (Salesforce Pub/Sub, Kafka with
  an Avro schema). Schema evolution, compact binary, streaming-friendly.
- **`EventData::Json`** — the default for everything else: REST APIs,
  webhooks, simple structures, legacy systems.
- **`EventData::Bytes`** — raw binary payloads that don't fit the other
  three: archive contents, non-UTF-8 blobs, anything you're passing
  through rather than interpreting.

When wiring up a new data source, match its native format rather than
converting early: columnar → Arrow, binary streaming → Avro, text APIs →
JSON, opaque binary → Bytes.

### Event chain preservation

Always send an event, even when there's no data — downstream tasks rely on
seeing *something* to know the operation completed. Returning early without
emitting an event breaks the chain and looks like a silent failure
downstream.

For an empty result: send an empty payload of the same type (empty Arrow
batch with empty schema, JSON metadata describing zero results, empty Avro
record). Route it through the same emission code path as a non-empty
result so there's exactly one place that sends:

```rust
let mut events = EventData::from_reader(cursor, content_type)?;

// If no data rows, from_reader returns empty vec but we still need to send an event
// to maintain the event chain. Create an empty Arrow batch so downstream knows job completed.
if events.is_empty() {
    let empty_batch = arrow::record_batch::RecordBatch::new_empty(
        std::sync::Arc::new(arrow::datatypes::Schema::empty())
    );
    events.push(EventData::ArrowRecordBatch(empty_batch));
}

// Emit all events through the same code path (empty or with data)
for event_data in events {
    let e = EventBuilder::new()
        .data(event_data)
        .subject(self.config.name.to_owned())
        .task_id(self.current_task_id)
        .task_type(self.task_type)
        .build()?;

    e.send_with_logging(self.tx.as_ref()).await?;
}
```

A missing event should mean failure, not "nothing to report" — that's what
lets downstream tasks and observability both trust the chain.

Reference: `flowgen/salesforce/src/bulkapi/job_retrieve.rs`

### Structured logging context

`send_with_logging()` returns an `EventLogger` you can chain `.context(key,
value)` calls onto before awaiting, to attach task-specific fields (row
counts, external IDs, token counts, latency, ...) to that event's success
log line:

```rust
event.send_with_logging(Some(&tx))
    .context("row_count", 1000)
    .context("external_id", "job-123")
    .await?;
```

`tracing` requires field names to be static per callsite, so it can't emit
one field per `.context()` call when the set of keys varies by task.
Instead, all `.context()` fields for one event are serialized together into
a single `context` JSON field on the log line. The admin UI unpacks that
JSON back into individual attribute rows client-side
(`web/src/lib/logRecord.ts`), so from a user's perspective each context key
still shows up as its own row — the joining is a `tracing` constraint, not
something either side of the UI needs to think about.

Per-task processing time already shows up via the OpenTelemetry
`task.handle` span, so don't add a duration field through `.context()` —
reach for the span instead.

### Task pattern (Processor/Runner)

Every task follows the same Processor/Runner shape:

```
task_name/
├── config.rs    # Configuration struct with serde
├── processor.rs # or query.rs, subscriber.rs, publisher.rs, etc.
└── mod.rs       # Module exports
```

**EventHandler** handles individual events. It holds shared state
(`client`, `config`, `task_id`, `tx`, `task_type`, `task_context`) and does
its work inside `with_event_context` for meta preservation:

```rust
pub struct EventHandler {
    client: Arc<Client>,
    config: Arc<Config>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    task_type: &'static str,
    task_context: Arc<TaskContext>,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        // Always check cancellation first.
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        // Process single event with event context for meta preservation.
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Event processing logic here

            // Send response and handle completion_tx
            self.send_response(response, completion_tx_arc).await
        }).await
    }

    async fn send_response(
        &self,
        response: Response,
        completion_tx_arc: Option<SharedCompletionTx>,
    ) -> Result<(), Error> {
        let mut event = EventBuilder::new()
            .data(EventData::Json(response_value))
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()?;

        // Signal completion or pass through to next task.
        match self.tx {
            None => {
                // Leaf task: signal completion with the event payload.
                // The shared CompletionState waits for every leaf in the
                // flow to signal before notifying the source task.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(event.data_as_json().ok());
                }
            }
            Some(_) => {
                // Pass through completion_tx to next task.
                event.completion_tx = completion_tx_arc.clone();
            }
        }

        event.send_with_logging(self.tx.as_ref()).await?;
        Ok(())
    }
}
```

**Processor** implements `Runner`, builds the `EventHandler` in `init()`,
and drives the receive loop in `run()`:

```rust
pub struct Processor {
    config: Arc<Config>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<TaskContext>,
    task_type: &'static str,
}

#[async_trait]
impl Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        // Initialize connections, clients, etc.
        // Access resource_loader from task_context if needed:
        // self.task_context.resource_loader.as_ref()

        let event_handler = EventHandler {
            client: Arc::new(client),
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    async fn run(mut self) -> Result<(), Error> {
        // Event loop with retry logic
        let retry_config = RetryConfig::merge(
            &self.task_context.retry,
            &self.config.retry,
        );

        let event_handler = Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                // ... init logic
            },
        ).await?;

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    tokio::spawn(async move {
                        // Handle event with retry
                    });
                }
                None => return Ok(()),
            }
        }
    }
}
```

**ProcessorBuilder** is the builder for `Processor`:

```rust
pub struct ProcessorBuilder {
    config: Option<Arc<Config>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self { ... }
    pub fn config(mut self, config: Arc<Config>) -> Self { ... }
    pub fn receiver(mut self, rx: Receiver<Event>) -> Self { ... }
    pub fn sender(mut self, tx: Sender<Event>) -> Self { ... }
    pub fn task_id(mut self, task_id: usize) -> Self { ... }
    pub fn task_context(mut self, task_context: Arc<TaskContext>) -> Self { ... }
    pub fn task_type(mut self, task_type: &'static str) -> Self { ... }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self.config.ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self.rx.ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self.task_context.ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self.task_type.ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}
```

Error variants every task needs at minimum:

```rust
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),

    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    // ... other error variants
}
```

Naming: `Processor` for general tasks, `Publisher`/`Subscriber` for pub/sub
roles, `Reader`/`Writer` for read/write operations, and always
`...Builder` for the builder struct.

A few patterns come up often enough to call out explicitly:

1. **Don't duplicate `task_context` fields onto `EventHandler`.** If it's
   already reachable via `self.task_context.resource_loader.as_ref()`,
   don't also store `resource_loader` directly — go through
   `task_context`.
2. **Run clippy before calling anything done**: `cargo clippy --workspace
   --all-targets --all-features -- -D warnings`. A cache-warm local run can
   pass while CI still fails, so re-run clean if you're unsure.
3. **Check cancellation first in `handle()`**, before doing any work:
   ```rust
   if self.task_context.cancellation_token.is_cancelled() {
       return Ok(());
   }
   ```
4. **Handle `completion_tx` correctly.** Extract it with
   `Arc::clone(&event).completion_tx.clone()`. If this is a leaf task
   (`tx` is `None`), call `arc.signal_completion(payload)` — the shared
   `CompletionState` tracks how many leaves are still outstanding and only
   notifies the source once the last one signals. Otherwise, pass it
   through: `event.completion_tx = completion_tx_arc.clone()`. Source
   tasks build the channel with
   `flowgen_core::event::new_completion_channel(self.task_context.leaf_count)`
   so the upstream message acks only after every leaf in the flow's DAG has
   signalled.
5. **Resource loading**: use `Source` for prompts, queries, and scripts
   (it supports both inline content and external resource files). Render
   per-event with
   `source.render(self.task_context.resource_loader.as_ref(), &event_data).await?`,
   or resolve static content with
   `source.resolve(self.task_context.resource_loader.as_ref()).await?`.

Reference implementations:
- `flowgen/ai-agent/src/completion/processor.rs` — resource loading, RAG,
  completion_tx, sandboxing
- `flowgen/gcp/src/bigquery/query.rs` — SQL query with resource loading
- `flowgen/nats/src/jetstream/publisher.rs` — event publishing
- `flowgen/object-store/src/reader.rs` — file reading

### Sandboxing (nsjail)

Sandboxing isolates LLM-generated tool execution from the host, via nsjail.

Today only the Rhai engine backs the `script` task, and Rhai is sandboxed
by design — it doesn't need nsjail. The `sandbox` field on the script
config is wired through for future engines, but in production the only
task that actually runs sandboxed code is `ai_completion` when its tools
are invoked.

Use sandboxing for AI agents with tools — they're the case vulnerable to
prompt injection via LLM-generated tool calls. Rhai scripts don't need it
(built-in safe sandbox). HTTP/SQL/webhook/connector tasks don't need it
either — they execute against external systems, not local code.

Sandboxing is optional with sensible defaults; omit `sandbox` to run
without it:

```yaml
ai_completion:
  name: "secure_agent"
  provider: google
  model: "gemini-2.5-flash-lite"
  prompt: "{{event.data}}"

  # Optional: enable sandbox for tool execution (omit for no sandbox)
  sandbox:
    memory_limit_mb: 512      # Default: 512 MB
    time_limit_seconds: 30    # Default: 30 seconds
    max_pids: 10              # Default: 10 processes
    allow_network: false      # Default: false (no network access)
    nsjail_path: "nsjail"     # Default: "nsjail" (searches PATH)
    user_id: 99999            # Default: 99999 (nobody)
    group_id: 99999           # Default: 99999 (nogroup)
```

Defaults: 512 MB memory, 30s time limit, 10 processes, network disabled,
uid/gid 99999 (nobody/nogroup).

If you ever introduce a new script engine that runs arbitrary host code
(Python, Bash, ...), sandboxing becomes required rather than optional for
untrusted code — the `sandbox` field already accepts a full
`SandboxConfig` for that case.

Reference implementations:
- `flowgen/core/src/nsjail/sandbox.rs` — nsjail executor with proper error handling
- `flowgen/core/src/task/script/config.rs` — script task with optional sandbox
- `flowgen/ai-agent/src/completion/config.rs` — AI completion with optional sandbox
