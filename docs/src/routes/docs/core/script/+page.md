# Script (Rhai)

Runs [Rhai](https://rhai.rs) scripts to transform, filter, enrich, and route events. The script receives the event as the `event` variable, and a `ctx` object exposing the distributed cache, mutable metadata, and resource loading. The script's return value becomes the next event's data.

Rhai is a sandboxed, embedded scripting language: it cannot perform IO, spawn processes, or access the host filesystem except through what flowgen explicitly exposes (`ctx.cache`, `ctx.resource`, the built-in helpers).

## Configuration

```yaml
- script:
    name: transform
    code: |
      let data = event.data;
      #{
        id: data.id,
        name: data.name,
        processed_at: timestamp_to_iso(timestamp_now())
      }
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `engine` | string | `rhai` | Script engine. Only `rhai` is supported today. |
| `code` | string / resource | required | Script code — inline string or `{ resource: "path" }` to load from the resources directory. |
| `depends_on` | list | | Upstream task names. See [Flows](/docs/concepts/flows). |
| `retry` | object | | Retry overrides. See [Retry](/docs/concepts/retry). |

## What the script sees

Two top-level variables:

| Variable | Description |
|---|---|
| `event` | Read-only view of the incoming event. |
| `ctx` | Mutable context object — cache, meta, resource loader. |

### `event` (read-only)

| Field | Description |
|---|---|
| `event.data` | Event payload. JSON object/array/scalar depending on the upstream task. |
| `event.meta` | Metadata snapshot from the upstream event. **Read-only here** — to mutate metadata, use `ctx.meta` (see below). |
| `event.id` | Optional identifier set by the source task. |
| `event.subject` | Subject string set by the producing task. |
| `event.error` | Error string when an upstream task failed after retries. Useful for routing to dead-letter destinations. |

### `ctx.meta` (mutable)

`ctx.meta` is a copy of `event.meta` that the script can mutate. Any changes are preserved on the emitted event:

```rhai
ctx.meta.batch_id = uuid();
ctx.meta.processed = true;
event.data
```

The next task in the flow sees both the original meta keys and any keys the script set or overrode.

### `ctx.cache` — distributed cache

Keys are automatically namespaced by the flow name, so two flows can use the same logical key without colliding.

| Method | Returns | Description |
|---|---|---|
| `ctx.cache.get(key)` | `()` or string | Read a value. Returns `()` (unit) if the key does not exist. |
| `ctx.cache.put(key, value)` | bool | Store a value (string or integer) with no expiration. Returns `true` on success. |
| `ctx.cache.put(key, value, ttl_secs)` | bool | Store with a time-to-live in seconds. |
| `ctx.cache.delete(key)` | bool | Delete a key. Returns `true` on success. |
| `ctx.cache.list_keys(prefix)` | array of strings | List keys under the given prefix (with the flow-name namespace stripped from results). Returns an empty array on cache errors. |

### `ctx.resource` — load files at script runtime

Loads content from the resource backend (filesystem or cache, see [Resources](/docs/concepts/resources)). Useful when the resource key depends on the event:

```rhai
let template_key = "templates/" + event.data.tenant + ".html";
let template = ctx.resource.get(template_key);
if template == () {
    throw "no template for tenant: " + event.data.tenant;
}
template
```

| Method | Returns | Description |
|---|---|---|
| `ctx.resource.get(key)` | `()` or string | Load a resource by key. Returns `()` if loading fails. |

For static resources known at config time, prefer the task's `code: { resource: ... }` field — it's resolved once and supports templating.

## Built-in helpers

### Identifiers and hashes

| Function | Description |
|---|---|
| `uuid()` | Generate a UUIDv7 string (timestamp-ordered, sorts naturally by creation time). |
| `sha256(input)` | SHA-256 hash, lowercase hex. Useful for deterministic IDs from composite keys. |
| `sha512(input)` | SHA-512 hash, lowercase hex. |

### Timestamps

All timestamps are Unix epoch in **seconds** unless noted.

| Function | Returns | Description |
|---|---|---|
| `timestamp_now()` | i64 | Current Unix timestamp in seconds. |
| `parse_timestamp(iso)` | i64 | Parse ISO 8601 / RFC 3339 to Unix milliseconds. Throws on parse error. |
| `parse_rfc2822_timestamp(s)` | i64 | Parse RFC 2822 (e.g., HTTP `Date` header) to Unix milliseconds. |
| `timestamp_to_iso(ts_secs)` | string | Format Unix seconds as ISO 8601 (`2026-02-02T12:00:00Z`). |
| `timestamp_to_year(ts_secs)` | i64 | Extract year. |
| `timestamp_to_month(ts_secs)` | i64 | Extract month (1–12). |
| `timestamp_to_day(ts_secs)` | i64 | Extract day of month (1–31). |
| `timestamp_to_hour(ts_secs)` | i64 | Extract hour (0–23). |
| `timestamp_round_to_hour(ts_secs)` | i64 | Floor a timestamp to the start of its hour. |
| `timestamp_to_hive_path(ts_secs)` | string | Format as `year=YYYY/month=MM/day=DD/hour=HH` for partitioned storage layouts. |

### Templating

| Function | Description |
|---|---|
| `render(template, data)` | Render a Handlebars template string against a data map. Same syntax as YAML config templates — `{{path.to.value}}`, `{{env.VAR}}`. Returns the rendered string or throws on missing variables. |

```rhai
let body = render("Hello {{name}}, your order #{{order_id}} is ready.", #{
    name: event.data.customer,
    order_id: event.data.id,
});
event.data.notification = body;
event.data
```

## Return values

The script's return value determines what the next task sees:

| Return | Effect |
|---|---|
| Object / array / scalar | Becomes the next event's `data`. |
| The full `event` value | Forwards as-is (preserves `event.id`, etc.). Use this when you only modify metadata. |
| `()` (unit) | **Filters the event.** No downstream event is emitted. The source's completion is signalled so it does not hang. |

```rhai
// Skip events that fail validation.
if event.data.amount < 0 {
    return ();
}
event.data
```

## Examples

**Set metadata:**

```yaml
- script:
    name: enrich
    code: |
      ctx.meta.batch_id = uuid();
      ctx.meta.received_at = timestamp_now();
      event.data
```

**Deduplication with cache:**

```yaml
- script:
    name: dedup
    code: |
      let key = "seen." + event.data.id;
      if ctx.cache.get(key) != () {
          return ();   // Skip duplicate.
      }
      ctx.cache.put(key, "1", 86400);   // TTL 24 hours.
      event.data
```

**Hive-partitioned partition key:**

```yaml
- script:
    name: route
    code: |
      let path = timestamp_to_hive_path(timestamp_now());
      ctx.meta.partition = path;
      event.data
```

**Deterministic ID from composite fields:**

```yaml
- script:
    name: idempotency_key
    code: |
      let key = sha256(event.data.campaign_id + ":" + event.data.reference_id);
      ctx.meta.idempotency_key = key;
      event.data
```

**Routing on error events:**

```yaml
- script:
    name: dlq_router
    code: |
      if event.error != () {
          ctx.meta.lane = "dlq";
      }
      event.data
```

**Load code from a resource file:**

```yaml
- script:
    name: transform
    code:
      resource: scripts/transform.rhai
```

The file content is rendered as a Handlebars template against the event before execution — see [Resources](/docs/concepts/resources).

## Logging

Rhai's built-in `print(value)` and `debug(value)` route through flowgen's tracing infrastructure. Output inherits the current span context (flow name, task name, task type), so messages are searchable alongside the rest of the worker logs.

```rhai
print("Processing order: " + event.data.id);
debug(event.data);
event.data
```
