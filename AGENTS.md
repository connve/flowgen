### Code Quality Standards

#### Error Handling
- **NEVER use `unwrap()`, `expect()`, or `panic!()`** in production code.
- **Always use Error enum with clear error types, messages, and source attribution.**
- Create specific error variants for different failure cases (e.g., `ReplayIdCacheClear`, `MissingManagedSubscriptionConfig`).
- Each error variant must have a descriptive message using `#[error("...")]` attribute.
- Use `#[source]` attribute to preserve error chain for debugging.
- Use `?` operator for error propagation.
- Use `map_err()` to convert errors to the appropriate error type.
- Handle all `Result` and `Option` types explicitly with proper error messages.

#### Documentation & Comments
- Please add relevant comments to code which will serve as documentation.
- In case of functionality changes, please update comments.
- Always use Rust best practices and state of the art.
- **All comments must be complete sentences with proper punctuation (periods, commas, etc.).**
- Do not use acronyms.
- Document module intros, methods, struct fields, constants etc.
- Do not change any code while documenting.
- Do not comment every function, especially if function meaning is very clear.
- Do comment every function which is part of a public API of a module.
- Explain the "why" behind complex logic, not just the "what".
- Do not add comments into unit tests.
- **All tasks must be documented with working examples in the `examples/` directory.**
- Example YAML files should demonstrate realistic use cases with clear comments.

#### Code Structure
- If possible, please use following traits to structs: `#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]`.
- Eliminate code duplication by extracting common logic into helper methods.
- Use proper abstractions and separation of concerns.
- Follow existing patterns in the codebase for consistency.
- Keep functions focused on a single responsibility.

#### Dependency Management
- **All package dependencies must be declared at the root workspace level in the workspace `Cargo.toml`.**
- Individual crate `Cargo.toml` files should reference workspace dependencies using `workspace = true`.
- Never declare dependency versions directly in individual crates.
- Example: `tokio = { workspace = true, features = ["full"] }` in crate-level `Cargo.toml`.

#### Standard Libraries
- **Always use standard libraries, never implement functionality manually.**
- **Time/Date Management**: Always use `chrono` for date/time operations, parsing, conversions, and timezone handling.
  - Never manually calculate dates, timestamps, or timezones.
  - Use chrono's built-in methods like `parse_from_str()`, `num_days()`, `num_microseconds()`, etc.
- **Error Management**: Always use `thiserror` for custom error types.
  - Use `#[derive(thiserror::Error)]` for error enums.
  - Use `#[error("...")]` for error messages.
  - Use `#[source]` to preserve error chains.

#### Configuration & Validation
- All configuration fields should have clear documentation with examples.
- Use `#[serde(default)]` for optional fields with sensible defaults.
- **Always use `humantime_serde` for any duration configuration fields.**
- Duration fields should accept human-readable strings (e.g., "30s", "5m", "1h").
- Use `#[serde(default, with = "humantime_serde")]` for `Option<Duration>` fields.
- Validate configuration at parse time when possible.
- Provide clear error messages for configuration issues.
- **Any changes to `AppConfig` must be documented in `config.example.yaml`.**
- Include comments explaining the purpose and usage of new configuration options.

#### API Response Handling
- **Always return pure API responses from external services.**
- **Never create custom response types unless the API returns unstructured data.**
- Use the types provided by the client library (e.g., `QueryResponse`, `GetQueryResultsResponse`).
- Only transform to internal formats (like Arrow RecordBatch) when required for event data.
- If an API returns structured responses, use them directly without custom wrappers.
- Example: BigQuery returns `QueryResponse` - use it directly, only convert to Arrow for EventData.

### Flowgen Architecture Reference

#### Retry Strategy

**Two Retry Patterns:**

1. **Subscribers** (Salesforce PubSub, NATS JetStream, Kafka, etc.)
   - Use infinite retry loop - must maintain connectivity indefinitely
   - Initialize with circuit breaker to detect permanent errors (bad credentials, config issues)
   - After initialization fails, retry initialization after backoff (infinite)
   - If event loop fails, reinitialize and reconnect automatically
   - Never give up - these are critical infrastructure components

2. **Everything Else** (publishers, processors, webhooks, HTTP requests, etc.)
   - Use circuit breaker pattern with `max_attempts` (default: 10)
   - Fail fast after exhausting retries
   - For publishers: if init fails, message won't be acked and will retry later
   - For webhooks: init failure indicates permanent config error

**Implementation Rules:**
- **Never implement custom retry logic with manual exponential backoff.**
- **Always rely on the global retry system via `RetryConfig`.**
- The retry system is automatically applied at the handler level in the `run()` method.
- Polling for async job completion is NOT a retry scenario - use simple fixed intervals.
- Use `initial_backoff` with human-readable duration strings (e.g., `"1s"`, `"500ms"`) - never use raw milliseconds.

**Default Retry Configuration (Circuit Breaker):**
- `max_attempts: 10` (default) - Acts as a circuit breaker, fails after ~15 minutes total
- `initial_backoff: "1s"` (default) - First retry waits ~1 second
- Backoff sequence with jitter: ~1s, ~2s, ~4s, ~8s, ~16s, ~32s, ~64s, ~128s, ~256s, ~512s
- Total time before circuit breaker trips: approximately 15 minutes

**User Configuration:**
```yaml
retry:
  max_attempts: 10       # default: 10 attempts (~15 min), circuit breaker for most tasks
  initial_backoff: "2s"  # default: "1s", first retry waits ~2s, grows exponentially
```

**Retry Logging:**
- Failures are logged immediately with full context (flow, task, task_id, task_type).
- Retry system automatically retries with exponential backoff and jitter.
- Jitter randomizes each delay by ±50% to prevent thundering herd.
- Each retry attempt that fails will log another error message.
- After all retry attempts are exhausted, the task fails and returns an error.
- Multiple error logs for the same operation are normal during transient failures.

**Reference Implementations:**
- Subscriber (infinite retry): `flowgen/salesforce/src/pubsubapi/subscriber.rs`
- Subscriber (infinite retry): `flowgen/nats/src/jetstream/subscriber.rs`
- Publisher (circuit breaker): `flowgen/salesforce/src/pubsubapi/publisher.rs`
- Webhook (circuit breaker): `flowgen/http/src/webhook.rs`

#### EventData Types

Flowgen supports multiple event data formats. **Always choose the most appropriate format based on the data source:**

- **`EventData::ArrowRecordBatch`** - **PREFERRED for columnar storage systems**
  - Use for: BigQuery, Parquet files, Arrow files, columnar databases
  - Benefits: Zero-copy operations, efficient memory usage, native columnar format
  - Example: BigQuery query results should return Arrow RecordBatch directly

- **`EventData::Avro`** - **PREFERRED for gRPC and Pub/Sub communications**
  - Use for: Salesforce Pub/Sub, gRPC streams, Kafka (with Avro schema)
  - Benefits: Schema evolution, compact binary format, streaming support
  - Example: Salesforce Pub/Sub events are natively Avro

- **`EventData::Json`** - **DEFAULT for everything else**
  - Use for: REST APIs, webhooks, simple data structures, legacy systems
  - Benefits: Human-readable, universal compatibility, easy debugging
  - Example: HTTP webhook payloads, generic API responses

**Important**: When implementing a new data source, always consider the native format:
- Columnar storage (BigQuery, Parquet) → Arrow RecordBatch
- Binary streaming (Pub/Sub, gRPC) → Avro
- Text-based APIs (REST, webhooks) → JSON

#### Event Chain Preservation

**CRITICAL**: Always maintain the event chain by sending events even when there's no data to process.

**Rule**: Never return early without sending an event. Downstream tasks must receive an event to know the operation completed.

**Empty Data Handling:**
- When a query/fetch returns no data, send an empty event of the same type
- For Arrow RecordBatch: Send an empty RecordBatch with empty schema
- For JSON: Send metadata indicating empty result
- For Avro: Send empty record or metadata

**Example (Arrow RecordBatch):**
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

**Why This Matters:**
- Downstream tasks need to know an operation completed (even if empty)
- Observability: Every operation should produce a log via `send_with_logging()`
- Error detection: Missing events indicate failure, not empty results
- Flow control: Downstream tasks can handle empty data appropriately

**Reference Implementation:** `flowgen/salesforce/src/bulkapi/job_retrieve.rs`

#### Task Pattern (Processor/Runner)

All flowgen tasks MUST follow the standard Processor/Runner pattern:

**File Structure:**
```
task_name/
├── config.rs    # Configuration struct with serde
├── processor.rs # or query.rs, subscriber.rs, publisher.rs, etc.
└── mod.rs       # Module exports
```

**Required Components:**

1. **EventHandler** - Handles individual events
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

2. **Processor** - Implements the Runner trait
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

3. **ProcessorBuilder** - Builder pattern for Processor
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

**Required Error Variants:**
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

**Naming Conventions:**
- Use `Processor` for general task processors
- Use `Publisher` for publishing tasks
- Use `Subscriber` for subscription tasks
- Use `Reader` for read operations
- Use `Writer` for write operations
- Always suffix with `Builder` for builder structs

**CRITICAL PATTERNS - MUST FOLLOW:**

1. **DO NOT duplicate fields from task_context**
   - WRONG: `resource_loader: Option<ResourceLoader>` in EventHandler
   - CORRECT: Access via `self.task_context.resource_loader.as_ref()`
   - EventHandler should contain `task_context: Arc<TaskContext>` and access all shared resources through it

2. **Always check clippy** — run `cargo clippy` before considering any change complete.

3. **Always check cancellation first in handle()**
   ```rust
   if self.task_context.cancellation_token.is_cancelled() {
       return Ok(());
   }
   ```

4. **Always handle completion_tx properly**
   - Extract it: `let completion_tx_arc = Arc::clone(&event).completion_tx.clone();`
   - If leaf task (tx is None): Call `arc.signal_completion(payload)` on the
     `SharedCompletionTx`. The shared `CompletionState` tracks how many leaves
     are still expected; the source is notified only after the last one signals.
   - If not a leaf task: Pass through via `event.completion_tx = completion_tx_arc.clone();`
   - Source tasks (subscribers, webhooks) build the channel with
     `flowgen_core::event::new_completion_channel(self.task_context.leaf_count)`
     so the upstream message is acked only after every leaf in the flow's
     directed acyclic graph has signalled.

5. **Resource loading pattern**
   - Use `Source` type for prompts, queries, scripts (supports inline + resource files)
   - Load with: `source.render(self.task_context.resource_loader.as_ref(), &event_data).await?`
   - For static content: `source.resolve(self.task_context.resource_loader.as_ref()).await?`

**Reference Implementations:**
- `flowgen/ai-agent/src/completion/processor.rs` (resource loading, RAG, completion_tx, sandboxing)
- `flowgen/gcp/src/bigquery/query.rs` (SQL query with resource loading)
- `flowgen/nats/src/jetstream/publisher.rs` (event publishing)
- `flowgen/object-store/src/reader.rs` (file reading)

#### Sandboxing (nsjail)

**Sandboxing isolates LLM-generated tool execution from the host using nsjail.**

The `script` task only ships with the Rhai engine today; Rhai is sandboxed by design and does not need nsjail. The `sandbox` field is wired through the script config struct for future engines, but the only task that actually runs sandboxed code in production is `ai_completion` when its tools are invoked.

**When to use sandboxing:**
- **AI agents with tools**: Recommended when agents execute LLM-generated tool calls (vulnerable to prompt injection).
- **Rhai scripts**: Not needed (Rhai has built-in safe sandbox).
- **HTTP/SQL/Webhooks/Connectors**: Not applicable (execute against external systems, no local code).

**Sandbox Configuration:**

Sandboxing is optional with sensible defaults. Omit `sandbox` field to run without sandboxing, or provide configuration to enable it.

**AI Agent Example:**
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

**Default Sandbox Limits:**
- Memory: 512 MB
- Time: 30 seconds
- Processes: 10
- Network: DISABLED
- User/Group: 99999 (nobody/nogroup)

**Security Model:**
- **Sandboxed (optional):**
  - AI agents with tools (recommended for untrusted LLM-generated tool calls).
- **Not sandboxed:**
  - Rhai scripts (safe embedded language with built-in sandbox).
  - HTTP requests (just network I/O, attack is data poisoning not code execution).
  - SQL queries (executes against external systems).
  - Webhooks (just HTTP client calls).

**Adding a new script engine:** if you ever introduce Python, Bash, or another engine that runs arbitrary host code, sandboxing becomes required for untrusted code, and the patterns above apply directly. The `sandbox` field on `script` config already accepts a full `SandboxConfig` for that future case.

**Reference Implementation:**
- `flowgen/core/src/nsjail/sandbox.rs` (nsjail executor with proper error handling)
- `flowgen/core/src/task/script/config.rs` (script task with optional sandbox)
- `flowgen/ai-agent/src/completion/config.rs` (AI completion with optional sandbox)
