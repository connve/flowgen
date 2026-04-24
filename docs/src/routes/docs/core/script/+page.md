# Script (Rhai)

Runs [Rhai](https://rhai.rs) scripts to transform, filter, and enrich events. The script receives the event as the `event` variable and returns the modified event.

## Configuration

```yaml
- script:
    name: transform
    code: |
      let data = event.data;
      #{
        id: data.Id,
        name: data.Name,
        processed_at: timestamp_to_iso(timestamp_now())
      }
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `engine` | string | `rhai` | Script engine. |
| `code` | string/resource | required | Script code — inline or loaded from a resource file. |
| `sandbox` | object | | Sandbox config for Python/Bash execution (not needed for Rhai). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

## Event access

Scripts receive:

- `event.data` — event payload (JSON object).
- `event.meta` — event metadata (preserved across tasks).
- `event.id` — event identifier (if set by source).
- `event.subject` — task name that produced the event.

## Built-in functions

| Function | Description |
|---|---|
| `timestamp_now()` | Current time in microseconds since epoch. |
| `timestamp_to_iso(ts)` | Convert microsecond timestamp to ISO 8601 string. |
| `uuid()` | Generate a UUIDv7 string. |
| `ctx.cache.get(key)` | Read from distributed cache. |
| `ctx.cache.put(key, value)` | Write to distributed cache. |
| `ctx.cache.put(key, value, ttl)` | Write with TTL in seconds. |
| `ctx.cache.delete(key)` | Delete from distributed cache. |

## Examples

**Set metadata:**

```yaml
- script:
    name: enrich
    code: |
      event.meta.source = "salesforce";
      event.meta.region = event.data.BillingCountry;
      event
```

**Deduplication with cache:**

```yaml
- script:
    name: dedup
    code: |
      let key = "seen." + event.data.id;
      if ctx.cache.get(key) != () {
        return ();  // Skip duplicate.
      }
      ctx.cache.put(key, "1", 86400);  // TTL 24h.
      event
```

**Load code from resource file:**

```yaml
- script:
    name: transform
    code:
      resource: scripts/transform.rhai
```
