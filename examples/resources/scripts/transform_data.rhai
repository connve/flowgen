// External Rhai script for transforming event data
// This script demonstrates loading complex logic from an external file

// Extract input values
let input_value = event.data.value;
let multiplier = event.data.multiplier;

// Perform transformation
let result = input_value * multiplier;

// Add computed fields to event
event.data.result = result;
event.data.computation_type = "multiplication";
event.data.processed_timestamp = timestamp_now();

// Add conditional logic
if result > 100 {
    event.data.category = "high";
} else if result > 50 {
    event.data.category = "medium";
} else {
    event.data.category = "low";
}

// Return modified event
event
