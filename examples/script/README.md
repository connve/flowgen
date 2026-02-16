# Script Task Examples

This directory contains examples demonstrating the Script task functionality in Flowgen.

## Overview

The Script task allows you to transform event data using Rhai scripts. Scripts can be inline or loaded from external files.

## Context Object (`ctx`)

Scripts have access to a `ctx` object that provides runtime capabilities:

### `ctx.cache` - Distributed Cache Operations

Access to the distributed cache (NATS KV, Redis, or in-memory) for stateful operations.

**Methods:**
- `ctx.cache.get(key)` → `Option<String>` - Retrieve a value from cache
- `ctx.cache.put(key, value, ttl_seconds)` → `bool` - Store a value with TTL in seconds
- `ctx.cache.delete(key)` → `bool` - Remove a value from cache

**Automatic Namespacing:**
Cache keys are automatically prefixed with the flow name to prevent collisions between flows.
- Script uses: `ctx.cache.get("processed")`
- Actual key: `my_flow_name.processed`

**Example:**
```rhai
// Check if already processed
// Actual key will be: flow_name.processed.{event.data.id}
let cache_key = "processed." + event.data.id;
if ctx.cache.get(cache_key) != () {
    return null;  // Skip duplicate
}

// Mark as processed (24 hour TTL)
ctx.cache.put(cache_key, "true", 86400);
```

### `ctx.meta` - Event Metadata

Read and modify event metadata that persists through the event chain.

**Example:**
```rhai
// Read metadata from previous task
let previous_step = ctx.meta.flow_step;

// Add new metadata
ctx.meta.processed_at = timestamp_now();
ctx.meta.flow_step = "enrichment";
ctx.meta.version = "2.0";

// Metadata is preserved in downstream tasks
```

## Examples

### Basic Examples

#### `script_inline_basic.yaml`
Simple inline script demonstrating basic data transformation.

#### `script_inline_multiline.yaml`
Multi-line inline script with data enrichment and metadata.

#### `script_resource_file.yaml`
Loading scripts from external files for reusability and version control.

### Cache Examples

#### `script_cache_deduplication.yaml`
**Use Case:** Prevent reprocessing duplicate events
- Uses `ctx.cache` to track processed event IDs
- Returns `null` to skip duplicates
- Configurable TTL for cache expiration

#### `script_incremental_processing.yaml`
**Use Case:** Incremental file processing without moving/deleting files
- Tracks processed files by name and hash
- Enables repeated flow runs without reprocessing
- Perfect for object store polling patterns
- 7-day TTL ensures automatic cleanup

#### `script_cache_and_metadata.yaml`
**Use Case:** Comprehensive example with rate limiting and caching
- Rate limiting using cache counters
- Caching expensive computations (user profiles)
- Conditional metadata based on data values
- Cache cleanup operations

### Metadata Examples

#### `script_metadata_tracking.yaml`
**Use Case:** Track data lineage through processing pipeline
- Add metadata at each processing stage
- Track timing and transformations
- Preserve metadata across multiple script tasks
- Useful for observability and debugging

### Advanced Examples

#### `script_advanced_external.yaml`
**Use Case:** Production-ready idempotent processing workflow
- External script from `resources/scripts/cache_and_metadata_advanced.rhai`
- Idempotency checks to prevent duplicate processing
- Stateful processing (running totals, aggregations)
- User behavior tracking (frequency analysis)
- Comprehensive metadata for observability

## Common Patterns

### 1. Idempotency Pattern
Prevent duplicate processing using cache:

```rhai
let idempotency_key = "processed:" + event.data.id;

if ctx.cache.get(idempotency_key) != () {
    return null;  // Already processed
}

// Process event...

// Mark as processed (7 day TTL = 604800 seconds)
ctx.cache.put(idempotency_key, timestamp_now(), 604800);
```

### 2. Rate Limiting Pattern
Limit request frequency per user/resource:

```rhai
let rate_key = "rate_limit:user:" + event.data.user_id;
let count = ctx.cache.get(rate_key);

if count == () {
    ctx.cache.put(rate_key, "1", 60);  // 60 second window
} else if parse_int(count) >= 10 {
    return null;  // Rate limit exceeded
} else {
    ctx.cache.put(rate_key, to_string(parse_int(count) + 1), 60);
}
```

### 3. Incremental Processing Pattern
Track processed files by hash to enable incremental updates:

```rhai
let file_key = "file:" + event.data.name + ":" + event.data.md5Hash;

if ctx.cache.get(file_key) != () {
    return null;  // File already processed
}

// Process file...

ctx.cache.put(file_key, timestamp_now(), 604800);  // 7 day TTL
```

### 4. Metadata Chain Pattern
Track processing stages across multiple tasks:

```rhai
// Task 1
ctx.meta.stage_1_completed = timestamp_now();
ctx.meta.validation_status = "passed";

// Task 2 can access Task 1's metadata
let validation = ctx.meta.validation_status;
ctx.meta.stage_2_completed = timestamp_now();
```

### 5. Stateful Aggregation Pattern
Maintain running totals or aggregates:

```rhai
let total_key = "running_total";
let current = ctx.cache.get(total_key);
let total = if current == () { 0.0 } else { parse_float(current) };

let new_total = total + event.data.amount;
ctx.cache.put(total_key, to_string(new_total), 86400);

event.data.running_total = new_total;
```

## Cache TTL Guidelines

Choose appropriate TTL values based on your use case:

- **Deduplication:** 24 hours - 7 days (86400 - 604800 seconds)
- **Rate Limiting:** 1 minute - 1 hour (60 - 3600 seconds)
- **Session Data:** 30 minutes - 4 hours (1800 - 14400 seconds)
- **Computed Results:** 1 hour - 24 hours (3600 - 86400 seconds)
- **User Profiles:** 1 hour - 7 days (3600 - 604800 seconds)

## Best Practices

1. **Always set TTL on cache entries** to prevent memory leaks
2. **Use descriptive cache keys** with namespaces (e.g., `"processed:file:123"`)
3. **Include version in cache keys** when data format changes (e.g., `"v2:user:123"`)
4. **Add metadata for observability** (timestamps, processing stages, etc.)
5. **Return `null` to skip events** instead of throwing errors
6. **Use external files for complex scripts** (easier testing and version control)
7. **Combine file hash with name** for incremental processing to detect changes

## Testing Scripts

Test scripts locally before deploying:

```bash
# Run flow with script examples
flowgen run examples/script/script_cache_deduplication.yaml
```

Monitor cache operations:
```bash
# Check NATS KV (if using NATS cache)
nats kv get flowgen-cache "processed:file:example.csv"
```

## Additional Resources

- [Rhai Language Book](https://rhai.rs/book/)
- [Flowgen Script Task Documentation](../../docs/tasks/script.md)
- [Cache Configuration](../../docs/configuration.md#cache)
