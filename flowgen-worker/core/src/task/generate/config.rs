//! Configuration structures for generate task types.
//!
//! Defines configuration options for event generation tasks that produce
//! synthetic or scheduled data streams in workflows.

use serde::{Deserialize, Serialize};

/// Configuration for generate subscriber tasks that produce scheduled events.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Subscriber {
    /// Optional label for event subject generation.
    pub label: Option<String>,
    /// Optional message content for generated events.
    pub message: Option<String>,
    /// Interval between generated events in milliseconds.
    pub interval: u64,
    /// Optional maximum number of events to generate before stopping.
    pub count: Option<u64>,
}
