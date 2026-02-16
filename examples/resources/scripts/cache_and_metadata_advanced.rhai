// Advanced Rhai script demonstrating ctx.cache and ctx.meta patterns
// This script implements a complete incremental data processing workflow

// ============================================
// IDEMPOTENCY CHECK
// ============================================

// Create idempotency key from event ID or data hash
let event_id = event.data.id;
let idempotency_key = "processed:" + event_id;

// Check if already processed
if ctx.cache.get(idempotency_key) != () {
    print("Event " + event_id + " already processed, skipping");
    // Set metadata for observability
    ctx.meta.status = "duplicate";
    ctx.meta.reason = "already_processed";
    return null;
}

// ============================================
// STATEFUL PROCESSING
// ============================================

// Track running totals using cache
let total_key = "running_total";
let current_total = ctx.cache.get(total_key);
let total = if current_total == () {
    0.0
} else {
    parse_float(current_total)
};

// Update running total
let new_total = total + event.data.amount;
ctx.cache.put(total_key, to_string(new_total), 86400);  // 24 hour TTL

// Add to event data
event.data.running_total = new_total;
event.data.previous_total = total;

// ============================================
// CONDITIONAL PROCESSING BASED ON CACHE
// ============================================

// Check if this is a high-frequency user
let user_id = event.data.user_id;
let user_events_key = "user_events:" + user_id;
let user_event_count = ctx.cache.get(user_events_key);

if user_event_count == () {
    ctx.cache.put(user_events_key, "1", 3600);
    event.data.user_frequency = "new";
} else {
    let count = parse_int(user_event_count);
    ctx.cache.put(user_events_key, to_string(count + 1), 3600);

    if count > 100 {
        event.data.user_frequency = "high";
    } else if count > 10 {
        event.data.user_frequency = "medium";
    } else {
        event.data.user_frequency = "low";
    }
}

// ============================================
// METADATA TRACKING
// ============================================

// Add comprehensive processing metadata
ctx.meta.processing_timestamp = timestamp_now();
ctx.meta.idempotency_key = idempotency_key;
ctx.meta.running_total = new_total;
ctx.meta.user_frequency = event.data.user_frequency;
ctx.meta.processing_stage = "enrichment";

// Track data lineage
ctx.meta.source_event_id = event_id;
ctx.meta.transformations = [
    "idempotency_check",
    "running_total_update",
    "user_frequency_calculation"
];

// Add conditional metadata
if event.data.amount > 1000 {
    ctx.meta.high_value = true;
    ctx.meta.requires_review = true;
}

// ============================================
// MARK AS PROCESSED
// ============================================

// Store in cache to prevent reprocessing (7 day TTL)
ctx.cache.put(idempotency_key, timestamp_now(), 604800);

// Set final metadata
ctx.meta.status = "processed";

// Return enriched event
event
